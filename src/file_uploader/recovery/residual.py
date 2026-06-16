"""残留文件恢复模块。

启动时扫描，将残留的 slot json 归拢为一个或多个满了的 slot 目录，
加入打包队列；孤儿归档 (.tar.zst) 发现后直接上传。
"""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import FileWriterConfig
    from ..interfaces.object_store import ObjectStore
    from ..interfaces.state_store import StateStore
    from ..interfaces.strategies import Packer, RemoteKeyGenerator
from ..layout import (
    active_dir_path,
    active_root,
    decode_ready_dir,
    encode_ready_dir,
    ensure_layout_roots,
    format_dir_id,
    sealed_dir_path,
    sealed_root,
)

logger = logging.getLogger(__name__)

# 匹配 slot_N_M 格式目录
_SLOT_DIR_PATTERN = re.compile(r"^slot_(\d+)_(\d+)$")


class ResidualRecovery:
    """启动时扫描残留文件并执行归拢/触发打包。

    设计：
    - 所有 slot_X_Y 目录下的 .json 文件全部归拢到 slot_0_0，
      然后按 pack_threshold 切分到 slot_0_1、slot_0_2...
    - 孤儿 .tar.zst 保留现有发现-上传逻辑。
    """

    def __init__(
        self,
        storage_root: str,
        state_store: StateStore,
        pack_threshold: int,
        config: "FileWriterConfig | None" = None,
    ) -> None:
        """
        Args:
            storage_root: 文件存储根目录（即 FileWriterConfig.storage_root）。
            state_store: 状态存储接口。
            pack_threshold: 文件数量封口阈值（来自 FileWriterConfig.pack_threshold）。
        """
        self._root = Path(storage_root)
        self._store = state_store
        self._pack_threshold = pack_threshold
        self._config = config

    # -----------------------------------------------------------------
    # 公共入口
    # -----------------------------------------------------------------

    async def scan_and_recover(self) -> int:
        """扫描并恢复残留文件。

        新逻辑：
        1. 扫描 **所有** slot_X_Y 目录（不信任老计数，全部重新归拢）。
        2. 将所有 .json 文件 rename 到 slot_0_0。
        3. 删除移空的残留目录。
        4. 如果 slot_0_0 中 json 数 >= pack_threshold，切分创建 slot_0_N。
        5. 更新 Redis 中 slot_0 的状态。
        6. 孤儿 .tar.zst 保留原逻辑。

        Returns:
            恢复处理的 json 文件总数。
        """
        if self._config is not None and self._config.storage_layout == "legacy_meta":
            return await self._scan_and_recover_legacy_meta()

        # 0. 清理过期归档：目录 + .tar.zst 共存时删除归档（json 归拢重打，避免重复上传）
        self._cleanup_stale_archives()

        # 1. 扫描所有 slot 目录
        slot_dirs = self._list_slot_dirs()
        if not slot_dirs:
            logger.info("No residual slot directories found.")
            return 0

        logger.info("Found %d residual slot directories", len(slot_dirs))

        # 2. 确保 slot_0_0 存在
        target_dir = self._root / "slot_0_0"
        target_dir.mkdir(parents=True, exist_ok=True)

        # 3. 将所有 .json 文件移入 slot_0_0
        total = 0
        for slot_path in slot_dirs:
            if slot_path == target_dir:
                continue  # 跳过自身
            moved = self._move_json_files(slot_path, target_dir)
            total += moved
            if moved > 0:
                logger.debug("Moved %d json from %s to slot_0_0", moved, slot_path.name)
            # 删除移空后的残留目录
            self._remove_empty_dir(slot_path)

        # 4. 统计 slot_0_0 中的 json 文件数
        #    已移入的 + slot_0_0 本身已有的
        remaining = len(list(target_dir.glob("*.json")))
        logger.info("slot_0_0 contains %d json files after merge", remaining)

        # 5. 按阈值切分
        created_dirs: list[str] = []
        if remaining >= self._pack_threshold:
            seq = 1
            pending = remaining
            while pending >= self._pack_threshold:
                new_dir = self._root / f"slot_0_{seq}"
                new_dir.mkdir(parents=True, exist_ok=True)
                self._take_json_files(target_dir, new_dir, self._pack_threshold)
                created_dirs.append(f"slot_0_{seq}")
                seq += 1
                pending -= self._pack_threshold
            remaining = pending  # 剩余留在 slot_0_0 中的数量
            logger.info(
                "Split: created %d dirs, %d remaining in slot_0_0",
                len(created_dirs),
                remaining,
            )

        # 6. 更新 Redis 中 slot_0 的状态
        dir_seq = len(created_dirs)  # 下一个序号
        await self._store.reset_slot_state(slot=0, active_count=remaining, dir_seq=dir_seq)

        # 7. 将切分出的目录加入 ready 队列
        for dir_name in created_dirs:
            await self._store.enqueue_ready_dir(f"0:{dir_name}")

        # 8. 孤儿 .tar.zst 入 ready queue，由 MetaPacker 上传
        orphans = self._discover_orphan_archives()
        if orphans:
            logger.info("Found %d orphan archive(s)", len(orphans))
            for arc_path in orphans:
                stem = arc_path.stem.replace(".tar", "")  # slot_X_Y
                m = _SLOT_DIR_PATTERN.match(stem)
                if m:
                    slot, seq = m.group(1), m.group(2)
                    ready_key = f"{slot}:{stem}"
                    await self._store.enqueue_ready_dir(ready_key)
                    logger.info("Orphan archive %s enqueued as ready key %s", arc_path.name, ready_key)

        return total

    async def finalize_and_upload(
        self,
        *,
        object_store: "ObjectStore",
        packer: "Packer",
        remote_key_generator: "RemoteKeyGenerator",
        packer_max_retries: int = 0,
        resume_orphan_archives: bool = True,
    ) -> int:
        """Finalize residual files into archives and upload them during shutdown."""
        recovered = await self.scan_and_recover()
        await self._enqueue_remainder_for_finalize()
        ready_dirs = await self._drain_ready_dirs()
        if not ready_dirs:
            logger.info("No residual ready dirs to finalize during shutdown")
            return recovered

        uploaded = 0
        for ready_dir in ready_dirs:
            try:
                uploaded += await self._process_ready_dir_with_retry(
                    ready_dir,
                    object_store=object_store,
                    packer=packer,
                    remote_key_generator=remote_key_generator,
                    packer_max_retries=packer_max_retries,
                    resume_orphan_archives=resume_orphan_archives,
                )
            except Exception:
                await self._store.enqueue_ready_dir(ready_dir)
                raise
        logger.info(
            "Residual finalize complete: recovered_json=%s uploaded_archives=%s",
            recovered,
            uploaded,
        )
        return recovered

    async def _scan_and_recover_legacy_meta(self) -> int:
        """Recover active/sealed/slot-N/dir-000001 layout used by main_meta_writer.py."""
        assert self._config is not None
        ensure_layout_roots(self._config)
        pool_dir = sealed_root(self._config) / ".pool"
        if pool_dir.exists():
            shutil.rmtree(pool_dir, ignore_errors=True)
        pool_dir.mkdir(parents=True, exist_ok=True)

        total = 0
        for base in (active_root(self._config), sealed_root(self._config)):
            if not base.exists():
                continue
            for slot_root in sorted(path for path in base.iterdir() if path.is_dir()):
                if slot_root.name == ".pool":
                    continue
                total += self._move_tree_json_files(slot_root, pool_dir)

        self._clear_children(active_root(self._config))
        self._clear_children(sealed_root(self._config), exclude_names={".pool"})

        slot_count = max(1, self._config.slot_count)
        slot_dir_seq = {slot: 0 for slot in range(slot_count)}
        ready_dirs: list[str] = []
        all_json = sorted(pool_dir.rglob("*.json"))
        full_batch_size = max(1, self._pack_threshold)
        full_dirs = len(all_json) // full_batch_size
        remainder = len(all_json) % full_batch_size

        cursor = 0
        for batch_index in range(full_dirs):
            slot = batch_index % slot_count
            slot_dir_seq[slot] += 1
            dir_id = format_dir_id(self._config, slot, slot_dir_seq[slot])
            target = sealed_dir_path(self._config, slot, dir_id)
            target.mkdir(parents=True, exist_ok=True)
            for src in all_json[cursor : cursor + full_batch_size]:
                src.rename(target / src.name)
            ready_dirs.append(encode_ready_dir(self._config, slot, dir_id))
            cursor += full_batch_size

        from ..interfaces.state_store import SlotRuntimeState

        slot_states: list[SlotRuntimeState] = []
        if remainder > 0:
            slot_dir_seq[0] += 1
            active_dir_id = format_dir_id(self._config, 0, slot_dir_seq[0])
            target = active_dir_path(self._config, 0, active_dir_id)
            for src in all_json[cursor:]:
                src.rename(target / src.name)
            slot_states.append(SlotRuntimeState(0, active_dir_id, remainder, slot_dir_seq[0]))
        else:
            slot_dir_seq[0] = max(1, slot_dir_seq[0] or 1)
            active_dir_id = format_dir_id(self._config, 0, slot_dir_seq[0])
            active_dir_path(self._config, 0, active_dir_id)
            slot_states.append(SlotRuntimeState(0, active_dir_id, 0, slot_dir_seq[0]))

        for slot in range(1, slot_count):
            slot_dir_seq[slot] = max(1, slot_dir_seq[slot] or 1)
            active_dir_id = format_dir_id(self._config, slot, slot_dir_seq[slot])
            active_dir_path(self._config, slot, active_dir_id)
            slot_states.append(SlotRuntimeState(slot, active_dir_id, 0, slot_dir_seq[slot]))

        await self._store.clear_state()
        await self._store.initialize_runtime_state(slot_states, ready_dirs)
        shutil.rmtree(pool_dir, ignore_errors=True)
        logger.info(
            "Recovered legacy metadata state: slots=%s, ready_dirs=%s, json_files=%s",
            len(slot_states),
            len(ready_dirs),
            len(all_json),
        )
        return total

    # -----------------------------------------------------------------
    # 内部工具
    # -----------------------------------------------------------------

    def _list_slot_dirs(self) -> list[Path]:
        """列出 storage_root 下所有 slot_X_Y 格式的目录。"""
        result: list[Path] = []
        if not self._root.exists():
            return result
        for entry in self._root.iterdir():
            if not entry.is_dir():
                continue
            m = _SLOT_DIR_PATTERN.match(entry.name)
            if m:
                result.append(entry)
        return result

    @staticmethod
    def _move_json_files(src: Path, dst: Path) -> int:
        """将 src 目录下所有 .json 文件 rename 到 dst。

        Returns:
            移动的文件数。
        """
        count = 0
        for f in src.glob("*.json"):
            target = dst / f.name
            # 如果目标已存在，加后缀避免覆盖
            if target.exists():
                stem = f.stem
                suffix = f.suffix
                idx = 1
                while True:
                    target = dst / f"{stem}_{idx}{suffix}"
                    if not target.exists():
                        break
                    idx += 1
            f.rename(target)
            count += 1
        return count

    @classmethod
    def _move_tree_json_files(cls, src: Path, dst: Path) -> int:
        count = 0
        for f in sorted(src.rglob("*.json")):
            target = dst / f.name
            if target.exists():
                stem = f.stem
                suffix = f.suffix
                idx = 1
                while True:
                    target = dst / f"{stem}_{idx:04d}{suffix}"
                    if not target.exists():
                        break
                    idx += 1
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                f.rename(target)
            except FileNotFoundError:
                continue
            count += 1
        return count

    @staticmethod
    def _clear_children(base: Path, *, exclude_names: set[str] | None = None) -> None:
        excludes = exclude_names or set()
        if not base.exists():
            return
        for child in base.iterdir():
            if child.name in excludes:
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)

    @staticmethod
    def _remove_empty_dir(dir_path: Path) -> None:
        """尝试删除空目录。非空（含 .tar.zst 等）则忽略。"""
        try:
            dir_path.rmdir()
        except OSError:
            pass  # 目录非空（可能含孤儿归档等），不做强制清理

    @staticmethod
    def _take_json_files(src: Path, dst: Path, count: int) -> None:
        """从 src 中取前 count 个 .json 文件 rename 到 dst。"""
        files = sorted(src.glob("*.json"))[:count]
        for f in files:
            f.rename(dst / f.name)

    # -----------------------------------------------------------------
    # 孤儿归档（保留原设计）
    # -----------------------------------------------------------------

    def _cleanup_stale_archives(self) -> int:
        """删除与目录共存的过期 .tar.zst（Ctrl+C 中断场景残留）。

        仅当 .tar.zst 和同名目录同时存在时才删除归档；
        仅 .tar.zst（无目录）的孤儿归档保留，后续入队上传。

        Returns:
            删除的归档数。
        """
        count = 0
        if not self._root.exists():
            return count
        for arc in self._root.glob("*.tar.zst"):
            stem = arc.stem.replace(".tar", "")
            dir_path = self._root / stem
            if dir_path.is_dir():
                arc.unlink(missing_ok=True)
                count += 1
                logger.info("Removed stale archive %s (source dir %s still exists)", arc.name, stem)
        return count

    def _discover_orphan_archives(self) -> list[Path]:
        """发现存储根目录下无对应 slot 目录的 .tar.zst 文件。"""
        orphans: list[Path] = []
        if not self._root.exists():
            return orphans
        for arc in self._root.glob("*.tar.zst"):
            stem = arc.stem.replace(".tar", "")  # slot_X_Y
            m = _SLOT_DIR_PATTERN.match(stem)
            if not m:
                continue
            dir_name = f"slot_{m.group(1)}_{m.group(2)}"
            dir_path = self._root / dir_name
            if not dir_path.exists():
                orphans.append(arc)
        return orphans

    async def _drain_ready_dirs(self) -> list[str]:
        ready_dirs: list[str] = []
        while True:
            ready_dir = await self._store.dequeue_ready_dir()
            if ready_dir is None:
                return ready_dirs
            ready_dirs.append(ready_dir)

    async def _enqueue_remainder_for_finalize(self) -> None:
        if self._config is None:
            return
        slot_count = max(1, self._config.slot_count)
        for slot in range(slot_count):
            state = await self._store.get_slot_state(slot)
            if state is None or state.active_count <= 0:
                continue
            ready_dir = encode_ready_dir(self._config, slot, state.active_dir_id)
            if self._config.storage_layout == "legacy_meta":
                source_dir = active_dir_path(self._config, slot, state.active_dir_id)
                target_dir = sealed_dir_path(self._config, slot, state.active_dir_id)
                target_dir.parent.mkdir(parents=True, exist_ok=True)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                if source_dir.exists():
                    source_dir.rename(target_dir)
            await self._store.enqueue_ready_dir(ready_dir)

    async def _process_ready_dir_immediately(
        self,
        ready_dir: str,
        *,
        object_store: "ObjectStore",
        packer: "Packer",
        remote_key_generator: "RemoteKeyGenerator",
        packer_max_retries: int,
        resume_orphan_archives: bool,
    ) -> int:
        slot, dir_id = decode_ready_dir(ready_dir)
        dir_path = sealed_dir_path(self._config, slot, dir_id) if self._config else self._root / dir_id
        if self._config is not None and self._config.storage_layout != "legacy_meta":
            dir_path = self._config.storage_root / dir_id

        if not dir_path.exists():
            if not resume_orphan_archives:
                logger.warning("Ready dir not found on disk during finalize: %s", dir_path)
                return 0
            archive_path = self._find_orphan_archive(dir_path.parent)
            if not archive_path.exists():
                logger.warning("Ready dir and orphan archive both missing during finalize: %s", dir_path)
                return 0
            return await self._upload_archive(archive_path, object_store, remote_key_generator, dir_path)

        if not any(dir_path.iterdir()):
            shutil.rmtree(dir_path, ignore_errors=True)
            return 0

        archive_path = await self._pack_with_retry(packer, dir_path, packer_max_retries)
        if archive_path is None:
            logger.error("Packing failed for residual dir %s, keeping local files", dir_path)
            return 0

        return await self._upload_archive(archive_path, object_store, remote_key_generator, dir_path)

    async def _process_ready_dir_with_retry(
        self,
        ready_dir: str,
        *,
        object_store: "ObjectStore",
        packer: "Packer",
        remote_key_generator: "RemoteKeyGenerator",
        packer_max_retries: int,
        resume_orphan_archives: bool,
    ) -> int:
        max_attempts = max(1, packer_max_retries + 1)
        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            try:
                return await self._process_ready_dir_immediately(
                    ready_dir,
                    object_store=object_store,
                    packer=packer,
                    remote_key_generator=remote_key_generator,
                    packer_max_retries=packer_max_retries,
                    resume_orphan_archives=resume_orphan_archives,
                )
            except Exception as exc:
                last_exc = exc
                if attempt >= max_attempts - 1:
                    break
                wait_seconds = 2 ** attempt
                logger.warning(
                    "Retrying residual finalize for %s in %ss (attempt %s/%s)",
                    ready_dir,
                    wait_seconds,
                    attempt + 2,
                    max_attempts,
                )
                await asyncio.sleep(wait_seconds)
        assert last_exc is not None
        raise RuntimeError(f"Residual finalize failed for {ready_dir}") from last_exc

    async def _pack_with_retry(self, packer: "Packer", dir_path: Path, max_retries: int) -> Path | None:
        import inspect

        for attempt in range(max_retries + 1):
            try:
                result = packer(dir_path)
                if inspect.isawaitable(result):
                    result = await result
                if result is not None:
                    return result
            except Exception:
                logger.exception(
                    "Residual finalize pack attempt %s/%s failed for %s",
                    attempt + 1,
                    max_retries + 1,
                    dir_path,
                )
            if attempt < max_retries:
                await asyncio.sleep(2 ** attempt)
        return None

    async def _upload_archive(
        self,
        archive_path: Path,
        object_store: "ObjectStore",
        remote_key_generator: "RemoteKeyGenerator",
        dir_path: Path,
    ) -> int:
        archive_path = self._ensure_unique_archive_name(archive_path)
        remote_key = remote_key_generator(archive_path.name)
        upload_ok = await object_store.upload(archive_path, remote_key)
        if not upload_ok:
            raise RuntimeError(f"Residual finalize upload failed for {archive_path}")
        archive_path.unlink(missing_ok=True)
        shutil.rmtree(dir_path, ignore_errors=True)
        logger.info("Finalized residual archive %s -> %s", archive_path, remote_key)
        return 1

    @staticmethod
    def _ensure_unique_archive_name(archive_path: Path) -> Path:
        from datetime import datetime

        expected = archive_path.with_name(
            f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:8]}.tar.zst"
        )
        if archive_path == expected:
            return archive_path
        while expected.exists():
            expected = archive_path.with_name(
                f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:8]}.tar.zst"
            )
        archive_path.rename(expected)
        return expected

    @staticmethod
    def _find_orphan_archive(directory: Path) -> Path:
        archives = sorted(directory.glob("*.tar.zst")) if directory.exists() else []
        if archives:
            return archives[0]
        return directory / "__missing__.tar.zst"
