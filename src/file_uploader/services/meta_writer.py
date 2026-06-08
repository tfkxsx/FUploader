"""文件写入服务 —— MetaWriter。

负责从消息源消费、解析、写盘、触发封口。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from ..config import FileWriterConfig
from ..interfaces.message_source import MessageSource
from ..interfaces.state_store import SlotRuntimeState, StateStore
from ..interfaces.strategies import FileNameGenerator, MessageParser
from ..layout import active_dir_path, encode_ready_dir, format_dir_id, sealed_dir_path
from ..messages import DecodedFilePayload, serialize_file_payload

logger = logging.getLogger(__name__)


@dataclass
class _PendingWrite:
    raw_body: bytes
    completed: asyncio.Future[None]


class MetaWriter:
    """文件写入服务。

    每个 slot 对应磁盘上一个目录；当目录内文件数达到 pack_threshold 时封口（seal），
    并交给 MetaPacker 打包上传。

    用法::

        writer = MetaWriter(
            config=config,
            state_store=store,
            message_source=source,
            message_parser=parser,
            file_name_generator=name_gen,
        )
        await writer.write_loop()
    """

    def __init__(
        self,
        *,
        config: FileWriterConfig,
        state_store: StateStore,
        message_source: MessageSource,
        message_parser: MessageParser,
        file_name_generator: FileNameGenerator,
        on_rotation_requested: Callable[[], Awaitable[None] | None] | None = None,
    ) -> None:
        self._cfg = config
        self._store = state_store
        self._source = message_source
        self._parser = message_parser
        self._name_gen = file_name_generator
        self._on_rotation_requested = on_rotation_requested
        self._running = False
        self._save_ok = 0
        self._rotation_requested = False
        self._rotation_stop_task: asyncio.Task[None] | None = None
        self._slot_lock = asyncio.Lock()
        self._slot_condition = asyncio.Condition(self._slot_lock)
        self._slot_inflight: dict[int, int] = {}
        self._sealing_slots: dict[int, str] = {}
        self._slot_states: dict[int, SlotRuntimeState] = {}
        self._write_queue_maxsize = max(
            1,
            max(1, config.prefetch_count) * max(1, config.meta_writer_process_count),
        )
        self._write_queue: asyncio.Queue[_PendingWrite | None] = asyncio.Queue(
            maxsize=self._write_queue_maxsize,
        )
        self._write_workers: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------
    # 启动 & 停止
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动写入服务。"""
        if self._running:
            return
        self._running = True
        await self._seal_over_threshold_slots_on_start()
        await self._load_owned_slot_states()
        await self._source.set_prefetch_count(self._cfg.prefetch_count)
        self._write_workers = [
            asyncio.create_task(self._write_worker(worker_id))
            for worker_id in range(self._cfg.write_concurrency)
        ]
        await self._source.start_consume(self._handle_message)
        logger.info(
            "MetaWriter started (slots=%s, threshold=%s, write_concurrency=%s, prefetch=%s, "
            "process_count=%s, queue_maxsize=%s, slot_strategy=%s, worker_index=%s, max_tasks_per_child=%s)",
            self._cfg.slot_count,
            self._cfg.pack_threshold,
            self._cfg.write_concurrency,
            self._cfg.prefetch_count,
            self._cfg.meta_writer_process_count,
            self._write_queue_maxsize,
            self._cfg.slot_strategy,
            self._cfg.worker_index,
            self._cfg.meta_writer_max_tasks_per_child,
        )

    async def stop(self) -> None:
        """停止写入服务。"""
        if not self._running:
            return
        self._running = False
        await self._source.stop_consume()
        for _ in self._write_workers:
            await self._write_queue.put(None)
        await asyncio.gather(*self._write_workers, return_exceptions=True)
        self._write_workers.clear()
        logger.info("MetaWriter stopped")

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def _handle_message(self, raw_body: bytes) -> None:
        """将消息交给写 worker，并在完成后返回给消息源执行 ack。"""
        if not self._running:
            raise RuntimeError("MetaWriter is not running")
        completed = asyncio.get_running_loop().create_future()
        await self._write_queue.put(_PendingWrite(raw_body=raw_body, completed=completed))
        await completed

    async def _write_worker(self, worker_id: int) -> None:
        logger.debug("MetaWriter worker %s started", worker_id)
        while True:
            pending = await self._write_queue.get()
            if pending is None:
                self._write_queue.task_done()
                break
            try:
                await self._process_message(pending.raw_body)
            except Exception as exc:
                if not pending.completed.done():
                    pending.completed.set_exception(exc)
            else:
                if not pending.completed.done():
                    pending.completed.set_result(None)
            finally:
                self._write_queue.task_done()
        logger.debug("MetaWriter worker %s stopped", worker_id)

    async def _process_message(self, raw_body: bytes) -> None:
        """解析并写入单条消息。"""
        try:
            data = self._parser(raw_body)
            if inspect.isawaitable(data):
                data = await data
        except Exception:
            logger.exception("Failed to parse message")
            raise

        try:
            file_name = self._name_gen(data)
        except Exception:
            logger.exception("Failed to generate file name")
            raise

        slot = self._select_slot(file_name)

        async with self._slot_condition:
            await self._slot_condition.wait_for(lambda: slot not in self._sealing_slots)
            state = self._slot_states.get(slot)
            if state is None:
                state = await self._store.get_slot_state(slot)
                if state is None:
                    raise RuntimeError(f"Slot {slot} state not found in store")
                self._slot_states[slot] = state

            dir_path = active_dir_path(self._cfg, slot, state.active_dir_id)
            observed_dir_id = state.active_dir_id
            self._slot_inflight[slot] = self._slot_inflight.get(slot, 0) + 1

        file_path = dir_path / file_name
        should_seal = False
        is_sealer = False
        try:
            content = self._serialize(data)
            await self._write_file(file_path, content)
            should_seal, new_count = await self._store.increment_and_check_threshold(
                slot, delta=1, threshold=self._cfg.pack_threshold,
            )
            logger.debug("Slot=%s file=%s count=%s seal=%s", slot, file_name, new_count, should_seal)

            if should_seal:
                async with self._slot_condition:
                    current_state = self._slot_states.get(slot)
                    if (
                        current_state is not None
                        and current_state.active_dir_id == observed_dir_id
                        and slot not in self._sealing_slots
                    ):
                        self._sealing_slots[slot] = observed_dir_id
                        is_sealer = True
        except Exception:
            logger.exception("Failed to process file %s", file_path)
            raise
        finally:
            async with self._slot_condition:
                self._slot_inflight[slot] -= 1
                self._slot_condition.notify_all()

        if is_sealer:
            async with self._slot_condition:
                await self._slot_condition.wait_for(lambda: self._slot_inflight.get(slot, 0) == 0)
                current_state = self._slot_states.get(slot)
                try:
                    if current_state is not None and current_state.active_dir_id == observed_dir_id:
                        await self._seal_slot(current_state)
                finally:
                    self._sealing_slots.pop(slot, None)
                    self._slot_condition.notify_all()

        self._save_ok += 1
        await self._request_rotation_if_needed()

    # ------------------------------------------------------------------
    # 封口逻辑
    # ------------------------------------------------------------------

    async def _load_owned_slot_states(self) -> None:
        """Cache active directory metadata so the hot path does not serialize on Redis."""
        for slot in self._owned_slots():
            state = await self._store.get_slot_state(slot)
            if state is not None:
                self._slot_states[slot] = state

    async def _seal_over_threshold_slots_on_start(self) -> None:
        """Seal active dirs that already reached the threshold before this process started."""
        for slot in self._owned_slots():
            async with self._slot_lock:
                state = await self._store.get_slot_state(slot)
                if state is None:
                    continue
                disk_count = self._count_active_json_files(slot, state.active_dir_id)
                if disk_count != state.active_count:
                    logger.info(
                        "Calibrating active count from disk: slot=%s dir=%s redis_count=%s disk_count=%s",
                        slot,
                        state.active_dir_id,
                        state.active_count,
                        disk_count,
                    )
                    await self._store.set_slot_active_count(slot, disk_count)
                    state = SlotRuntimeState(
                        slot=state.slot,
                        active_dir_id=state.active_dir_id,
                        active_count=disk_count,
                        dir_seq=state.dir_seq,
                    )
                if disk_count < self._cfg.pack_threshold:
                    continue
                logger.info(
                    "Active dir already reached threshold on startup, sealing: slot=%s dir=%s count=%s threshold=%s",
                    slot,
                    state.active_dir_id,
                    disk_count,
                    self._cfg.pack_threshold,
                )
                await self._seal_slot(state)

    def _count_active_json_files(self, slot: int, dir_id: str) -> int:
        dir_path = active_dir_path(self._cfg, slot, dir_id)
        return sum(1 for path in dir_path.rglob("*.json") if path.is_file())

    def _owned_slots(self) -> list[int]:
        if self._cfg.slot_strategy == "worker_index":
            assert self._cfg.worker_index is not None
            return [self._cfg.worker_index]
        return list(range(self._cfg.slot_count))

    async def _seal_slot(self, state: SlotRuntimeState) -> None:
        """将当前活动目录封口，创建新目录。"""
        ready_dir = encode_ready_dir(self._cfg, state.slot, state.active_dir_id)
        logger.info("Sealing slot=%s dir=%s", state.slot, state.active_dir_id)

        next_seq = state.dir_seq + 1
        next_dir_id = format_dir_id(self._cfg, state.slot, next_seq)

        source_dir = active_dir_path(self._cfg, state.slot, state.active_dir_id)
        if self._cfg.storage_layout == "legacy_meta":
            target_dir = sealed_dir_path(self._cfg, state.slot, state.active_dir_id)

            def _move_to_sealed() -> None:
                target_dir.parent.mkdir(parents=True, exist_ok=True)
                if target_dir.exists():
                    shutil.rmtree(target_dir, ignore_errors=True)
                if source_dir.exists():
                    source_dir.rename(target_dir)

            await asyncio.to_thread(_move_to_sealed)

        # 原子切换目录 + 归零计数 + 递增 seq
        await self._store.switch_slot_dir(state.slot, next_dir_id)
        self._slot_states[state.slot] = SlotRuntimeState(
            slot=state.slot,
            active_dir_id=next_dir_id,
            active_count=0,
            dir_seq=next_seq,
        )

        # 将封口目录加入待打包队列
        await self._store.enqueue_ready_dir(ready_dir)

        # 确保新目录已创建
        active_dir_path(self._cfg, state.slot, next_dir_id)

    # ------------------------------------------------------------------
    # IO 操作
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize(data: Any) -> bytes:
        """序列化结构化数据为字节。

        默认 JSON 序列化，可覆盖。
        """
        import json

        if isinstance(data, DecodedFilePayload):
            return serialize_file_payload(data.payload, content_type=data.content_type)
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    async def _write_file(self, path: Path, content: bytes) -> None:
        """异步写入文件（带超时）。"""
        loop = asyncio.get_running_loop()

        def _sync_write() -> None:
            path.write_bytes(content)

        await asyncio.wait_for(
            loop.run_in_executor(None, _sync_write),
            timeout=self._cfg.save_timeout,
        )

    def _select_slot(self, file_name: str) -> int:
        if self._cfg.slot_strategy == "worker_index":
            assert self._cfg.worker_index is not None
            return self._cfg.worker_index
        return abs(hash(file_name)) % self._cfg.slot_count

    async def _request_rotation_if_needed(self) -> None:
        """达到单子进程处理阈值后，停止消费并交给外层 supervisor 重启。"""
        max_tasks = self._cfg.meta_writer_max_tasks_per_child
        if self._rotation_requested or max_tasks <= 0 or self._save_ok < max_tasks:
            return

        self._rotation_requested = True
        logger.info(
            "MetaWriter reached max_tasks_per_child, requesting graceful rotation: save_ok=%s, max_tasks_per_child=%s",
            self._save_ok,
            max_tasks,
        )
        self._rotation_stop_task = asyncio.create_task(self.stop())
        if self._on_rotation_requested is not None:
            result = self._on_rotation_requested()
            if inspect.isawaitable(result):
                await result
