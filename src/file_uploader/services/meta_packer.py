"""打包上传服务 —— MetaPacker。

负责从待打包队列取目录、打包、上传到 OSS、清理本地文件。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..config import FileWriterConfig
from ..interfaces.object_store import ObjectStore
from ..interfaces.state_store import StateStore
from ..interfaces.strategies import Packer, RemoteKeyGenerator
from ..layout import decode_ready_dir, sealed_dir_path

logger = logging.getLogger(__name__)


class MetaPacker:
    """打包与上传服务。

    轮询待打包队列 → 打包成归档 → 上传至 OSS → 清理本地。

    用法::

        packer = MetaPacker(
            config=config,
            state_store=store,
            object_store=oss_uploader,
            packer=tar_zstd_packer,
            remote_key_generator=key_gen,
        )
        await packer.run()
    """

    def __init__(
        self,
        *,
        config: FileWriterConfig,
        state_store: StateStore,
        object_store: ObjectStore,
        packer: Packer,
        remote_key_generator: RemoteKeyGenerator,
    ) -> None:
        self._cfg = config
        self._store = state_store
        self._oss = object_store
        self._packer = packer
        self._key_gen = remote_key_generator
        self._token = str(uuid.uuid4())[:12]
        self._running = False
        self._workers: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # 启动 & 停止
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动打包服务（启动多个 worker）。"""
        if self._running:
            return
        self._running = True
        for i in range(self._cfg.packer_concurrency):
            task = asyncio.create_task(self._worker(i))
            self._workers.append(task)
        logger.info(
            "MetaPacker started (concurrency=%s, interval=%.1fs, max_retries=%s)",
            self._cfg.packer_concurrency,
            self._cfg.packer_interval,
            self._cfg.packer_max_retries,
        )

    async def stop(self) -> None:
        """停止打包服务。"""
        if not self._running:
            return
        self._running = False
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("MetaPacker stopped")

    # ------------------------------------------------------------------
    # Worker 循环
    # ------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        """单个 packer worker。"""
        logger.debug("Packer worker %s started", worker_id)
        while self._running:
            ready_dir = await self._store.dequeue_ready_dir()
            if ready_dir is None:
                await asyncio.sleep(self._cfg.packer_interval)
                continue

            # 尝试锁定目录（防止其他 packer 重复打包）
            locked = await self._store.lock_ready_dir(ready_dir, self._token)
            if not locked:
                logger.debug("Worker %s failed to lock %s, skipping", worker_id, ready_dir)
                continue

            try:
                await self._process_ready_dir(ready_dir)
            except Exception:
                logger.exception("Worker %s failed to process %s", worker_id, ready_dir)
            finally:
                await self._store.unlock_ready_dir(ready_dir)

        logger.debug("Packer worker %s stopped", worker_id)

    # ------------------------------------------------------------------
    # 处理封口目录
    # ------------------------------------------------------------------

    async def _process_ready_dir(self, ready_dir: str) -> None:
        """处理单个封口目录：打包 → 上传 → 清理。"""
        try:
            slot, dir_id = decode_ready_dir(ready_dir)
        except (IndexError, ValueError):
            logger.error("Invalid ready_dir format: %s", ready_dir)
            return

        dir_path = sealed_dir_path(self._cfg, slot, dir_id)
        if self._cfg.storage_layout != "legacy_meta":
            dir_path = self._cfg.storage_root / dir_id

        if not dir_path.exists():
            # 目录已被其他进程清理（或上次运行残留），检查是否有孤儿归档
            orphan_archive = self._find_orphan_archive(dir_path.parent)
            if orphan_archive.exists():
                logger.info(
                    "Ready dir %s not found, but orphan archive exists: %s, will upload directly",
                    dir_path, orphan_archive,
                )
                await self._upload_and_cleanup(orphan_archive, dir_path)
            else:
                logger.warning("Ready dir not found on disk: %s, skipping", dir_path)
            return

        if not any(dir_path.iterdir()):
            logger.debug("Ready dir is empty: %s, removing", dir_path)
            shutil.rmtree(dir_path, ignore_errors=True)
            return

        # 打包
        archive_path = await self._pack_with_retry(dir_path)

        if archive_path is None:
            logger.error("Packing failed for %s after %s retries, removing dir", dir_path, self._cfg.packer_max_retries)
            shutil.rmtree(dir_path, ignore_errors=True)
            return

        # 上传
        archive_path = self._ensure_unique_archive_name(archive_path)
        archive_name = archive_path.name
        remote_key = self._key_gen(archive_name)

        upload_ok = await self._oss.upload(archive_path, remote_key)
        if not upload_ok:
            logger.error("Upload failed for %s, keeping local archive", archive_path)
            return

        # 清理：删除本地归档 + 源目录
        try:
            archive_path.unlink(missing_ok=True)
            shutil.rmtree(dir_path, ignore_errors=True)
            logger.info("Processed & cleaned %s -> %s", ready_dir, remote_key)
        except Exception:
            logger.exception("Cleanup error for %s", ready_dir)

    # ------------------------------------------------------------------
    # 孤儿归档上传
    # ------------------------------------------------------------------

    async def _upload_and_cleanup(self, archive_path: Path, dir_path: Path) -> None:
        """直接上传已有归档文件并清理（断点续传）。"""
        remote_key = self._key_gen(archive_path.name)
        upload_ok = await self._oss.upload(archive_path, remote_key)
        if not upload_ok:
            logger.error("Upload failed for orphan archive %s, keeping", archive_path)
            return

        try:
            archive_path.unlink(missing_ok=True)
            shutil.rmtree(dir_path, ignore_errors=True)
            logger.info("Processed orphan archive %s -> %s", archive_path, remote_key)
        except Exception:
            logger.exception("Cleanup error for orphan archive %s", archive_path)

    def _ensure_unique_archive_name(self, archive_path: Path) -> Path:
        expected = archive_path.with_name(self._new_archive_name())
        if archive_path == expected:
            return archive_path
        while expected.exists():
            expected = archive_path.with_name(self._new_archive_name())
        archive_path.rename(expected)
        return expected

    @staticmethod
    def _new_archive_name() -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        return f"{timestamp}_{uuid.uuid4().hex[:8]}.tar.zst"

    @staticmethod
    def _find_orphan_archive(directory: Path) -> Path:
        archives = sorted(directory.glob("*.tar.zst")) if directory.exists() else []
        if archives:
            return archives[0]
        return directory / "__missing__.tar.zst"

    # ------------------------------------------------------------------
    # 打包重试逻辑
    # ------------------------------------------------------------------

    async def _pack_with_retry(self, dir_path: Path) -> Optional[Path]:
        """打包目录，支持重试。

        Returns:
            归档文件路径，如果全部失败则返回 None。
        """
        max_retries = self._cfg.packer_max_retries

        for attempt in range(max_retries + 1):
            try:
                result = self._packer(dir_path)
                if inspect.isawaitable(result):
                    result = await result
                if result is not None:
                    return result
            except Exception:
                logger.exception("Packer attempt %s/%s failed for %s", attempt + 1, max_retries + 1, dir_path)

            if attempt < max_retries:
                wait = 2 ** attempt  # 指数退避: 1s, 2s, 4s...
                logger.info("Retrying pack for %s in %ss (attempt %s/%s)", dir_path, wait, attempt + 1, max_retries)
                await asyncio.sleep(wait)

        return None
