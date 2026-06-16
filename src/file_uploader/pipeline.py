"""Pipeline 入口 —— Builder 模式。

提供一键构建 & 启动 FileWriter Pipeline 的便捷入口。
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import multiprocessing
import os
import signal
import time
import uuid
from pathlib import Path
from typing import Callable, Optional

from .config import FileWriterConfig
from .interfaces.message_source import MessageSource
from .interfaces.object_store import ObjectStore
from .interfaces.state_store import StateStore
from .interfaces.strategies import (
    FileNameGenerator,
    MessageParser,
    Packer,
    RemoteKeyGenerator,
)
from .layout import active_dir_path, ensure_layout_roots, format_dir_id
from .recovery import ResidualRecovery
from .runtime import get_meta_writer_process_count, get_node_id
from .services.meta_packer import MetaPacker
from .services.meta_writer import MetaWriter

logger = logging.getLogger(__name__)


class FileWriterPipeline:
    """File Writer Pipeline —— 构建器 + 生命周期管理。

    用法::

        pipeline = (
            FileWriterPipeline
            .with_config(config)
            .with_state_store(redis_store)
            .with_message_source(rabbitmq_source)
            .with_object_store(uploader)
            .with_message_parser(my_parser)
            .with_file_name_generator(my_name_gen)
            .with_packer(tar_zstd_packer)
            .with_remote_key_generator(my_key_gen)
        )

        await pipeline.start()
        await pipeline.wait()  # 阻塞直到收到终止信号
        await pipeline.stop()
    """

    def __init__(self) -> None:
        self._config: Optional[FileWriterConfig] = None
        self._state_store: Optional[StateStore] = None
        self._message_source: Optional[MessageSource] = None
        self._object_store: Optional[ObjectStore] = None
        self._message_parser: Optional[MessageParser] = None
        self._file_name_generator: Optional[FileNameGenerator] = None
        self._packer: Optional[Packer] = None
        self._remote_key_generator: Optional[RemoteKeyGenerator] = None
        self._enable_recovery: bool = True

        self._writer: Optional[MetaWriter] = None
        self._packer_svc: Optional[MetaPacker] = None
        self._shutdown_event: Optional[asyncio.Event] = None
        self._main_task: Optional[asyncio.Task] = None
        self._rotation_requested = False

    # ------------------------------------------------------------------
    # Builder 方法（链式调用）
    # ------------------------------------------------------------------

    @classmethod
    def with_config(cls, config: FileWriterConfig) -> "FileWriterPipeline":
        self = cls()
        self._config = config
        return self

    def with_state_store(self, store: StateStore) -> "FileWriterPipeline":
        self._state_store = store
        return self

    def with_message_source(self, source: MessageSource) -> "FileWriterPipeline":
        self._message_source = source
        return self

    def with_object_store(self, store: ObjectStore) -> "FileWriterPipeline":
        self._object_store = store
        return self

    def with_message_parser(self, parser: MessageParser) -> "FileWriterPipeline":
        self._message_parser = parser
        return self

    def with_file_name_generator(self, gen: FileNameGenerator) -> "FileWriterPipeline":
        self._file_name_generator = gen
        return self

    def with_packer(self, packer: Packer) -> "FileWriterPipeline":
        self._packer = packer
        return self

    def with_remote_key_generator(self, gen: RemoteKeyGenerator) -> "FileWriterPipeline":
        self._remote_key_generator = gen
        return self

    def no_recovery(self) -> "FileWriterPipeline":
        """禁用启动时的残留文件恢复。"""
        self._enable_recovery = False
        return self

    # ------------------------------------------------------------------
    # 验证 & 初始化
    # ------------------------------------------------------------------

    def _validate(self) -> None:
        missing = []
        for name, val in [
            ("config", self._config),
            ("state_store", self._state_store),
            ("message_source", self._message_source),
            ("object_store", self._object_store),
            ("message_parser", self._message_parser),
            ("file_name_generator", self._file_name_generator),
            ("packer", self._packer),
            ("remote_key_generator", self._remote_key_generator),
        ]:
            if val is None:
                missing.append(name)
        if missing:
            raise ValueError(f"Missing required pipeline components: {', '.join(missing)}")

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动 Pipeline（初始化状态 + 残留恢复 + 启动 Writer & Packer）。"""
        self._validate()
        assert self._config is not None
        assert self._state_store is not None

        logger.info("Starting FileWriterPipeline...")

        if hasattr(self._state_store, "connect"):
            try:
                result = self._state_store.connect()  # type: ignore[attr-defined]
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Error connecting state_store")
                raise

        # 确保存储目录存在
        ensure_layout_roots(self._config)

        # 初始化运行时状态（幂等）+ 残留恢复。
        # 多进程/多节点启动时只能有一个进程操作磁盘残留，其它进程阻塞等待状态完成。
        await self._bootstrap_runtime_state()

        # 构建服务
        self._writer = MetaWriter(
            config=self._config,
            state_store=self._state_store,
            message_source=self._message_source,   # type: ignore[arg-type]
            message_parser=self._message_parser,   # type: ignore[arg-type]
            file_name_generator=self._file_name_generator,  # type: ignore[arg-type]
            on_rotation_requested=self._request_rotation,
        )

        self._packer_svc = MetaPacker(
            config=self._config,
            state_store=self._state_store,
            object_store=self._object_store,        # type: ignore[arg-type]
            packer=self._packer,                    # type: ignore[arg-type]
            remote_key_generator=self._remote_key_generator,  # type: ignore[arg-type]
        )

        # 启动
        self._shutdown_event = asyncio.Event()
        await self._writer.start()
        await self._packer_svc.start()
        logger.info("FileWriterPipeline started successfully")

    async def stop(self) -> None:
        """优雅关闭 Pipeline。"""
        logger.info("Stopping FileWriterPipeline...")
        if self._writer:
            await self._writer.stop()
        if self._packer_svc:
            await self._packer_svc.stop()

        finalize_completed = False
        if self._should_finalize_residual_files():
            finalize_completed = await self._finalize_residual_files()

        if finalize_completed and self._state_store:
            try:
                await self._state_store.clear_state()
            except Exception:
                logger.exception("Error clearing state after residual finalize")

        if self._state_store:
            if isinstance(self._state_store, object) and hasattr(self._state_store, "disconnect"):
                try:
                    await self._state_store.disconnect()
                except Exception:
                    logger.exception("Error disconnecting state_store")

        logger.info("FileWriterPipeline stopped")

    async def wait(self) -> None:
        """阻塞直到收到 SIGINT/SIGTERM。"""
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown_event.set)
            except NotImplementedError:
                # Windows 不支持 add_signal_handler
                pass
        logger.info("Pipeline running, waiting for shutdown signal...")
        await self._shutdown_event.wait()

    @property
    def rotation_requested(self) -> bool:
        """是否因 MetaWriter 达到 max_tasks_per_child 而退出等待。"""
        return self._rotation_requested

    def _request_rotation(self) -> None:
        self._rotation_requested = True
        if self._shutdown_event is not None:
            self._shutdown_event.set()

    def _should_finalize_residual_files(self) -> bool:
        if self._rotation_requested:
            return False
        return bool(self._config and self._config.residue_file_upload)

    async def _finalize_residual_files(self) -> bool:
        assert self._config is not None
        assert self._state_store is not None
        assert self._object_store is not None
        assert self._packer is not None
        assert self._remote_key_generator is not None

        token = str(uuid.uuid4())
        acquired = await self._state_store.try_acquire_finalize_lock(token)
        if not acquired:
            logger.info("Another instance is finalizing residual files, skipping")
            return False

        try:
            recovery = ResidualRecovery(
                storage_root=self._config.storage_root,
                state_store=self._state_store,
                pack_threshold=self._config.pack_threshold,
                config=self._config,
            )
            await recovery.finalize_and_upload(
                object_store=self._object_store,
                packer=self._packer,
                remote_key_generator=self._remote_key_generator,
                packer_max_retries=self._config.packer_max_retries,
                resume_orphan_archives=self._config.resume_orphan_archives,
            )
            return True
        finally:
            await self._state_store.release_finalize_lock()

    # ------------------------------------------------------------------
    # 运行时状态初始化
    # ------------------------------------------------------------------

    async def _bootstrap_runtime_state(self) -> None:
        """Initialize runtime state and recover residual files behind one init lock."""
        assert self._state_store is not None
        assert self._config is not None

        if not self._enable_recovery and await self._state_store.has_runtime_state():
            logger.info("Runtime state already initialized, skipping")
            return

        import uuid

        token = str(uuid.uuid4())
        acquired = await self._state_store.try_acquire_init_lock(token)
        if not acquired:
            logger.info("Another instance is bootstrapping runtime state, waiting...")
            while True:
                await asyncio.sleep(1)
                if await self._state_store.has_runtime_state() and not await self._state_store.is_init_locked():
                    logger.info("Runtime state bootstrapped by another instance")
                    return

        try:
            if not self._enable_recovery and await self._state_store.has_runtime_state():
                logger.info("Runtime state initialized by another instance (double-check)")
                return

            if self._enable_recovery:
                recovery = ResidualRecovery(
                    storage_root=self._config.storage_root,
                    state_store=self._state_store,
                    pack_threshold=self._config.pack_threshold,
                    config=self._config,
                )
                await recovery.scan_and_recover()
                return

            await self._initialize_empty_runtime_state()
        finally:
            await self._state_store.release_init_lock()

    async def _initialize_empty_runtime_state(self) -> None:
        """Create empty runtime state without scanning residual files."""
        assert self._state_store is not None
        assert self._config is not None

        from .interfaces.state_store import SlotRuntimeState

        slots = []
        for i in range(self._config.slot_count):
            dir_seq = 1 if self._config.storage_layout == "legacy_meta" else 0
            dir_id = format_dir_id(self._config, i, dir_seq)
            slots.append(
                SlotRuntimeState(
                    slot=i,
                    active_dir_id=dir_id,
                    active_count=0,
                    dir_seq=dir_seq,
                )
            )
            active_dir_path(self._config, i, dir_id)

        await self._state_store.initialize_runtime_state(slots, ready_dirs=[])
        logger.info("Runtime state initialized: %s slots", len(slots))


PipelineFactory = Callable[[], FileWriterPipeline]


async def run_pipeline_once(pipeline_factory: PipelineFactory) -> None:
    """构建并运行一次 Pipeline，直到信号或 writer 轮转请求触发退出。"""
    pipeline = pipeline_factory()
    await pipeline.start()
    try:
        await pipeline.wait()
    finally:
        await pipeline.stop()


def _run_pipeline_process(pipeline_factory: PipelineFactory, base_node_id: str, process_index: int) -> None:
    os.environ["NODE_ID"] = f"{base_node_id}-meta-p{process_index}"
    asyncio.run(run_pipeline_once(pipeline_factory))


def _spawn_pipeline_process(
    pipeline_factory: PipelineFactory,
    base_node_id: str,
    process_index: int,
) -> multiprocessing.Process:
    process = multiprocessing.Process(
        target=_run_pipeline_process,
        args=(pipeline_factory, base_node_id, process_index),
        name=f"file_uploader-meta_writer-{process_index}",
    )
    process.start()
    logger.info("FileWriterPipeline child started, process=%s, pid=%s", process.name, process.pid)
    return process


def run_pipeline_supervised(
    pipeline_factory: PipelineFactory,
    *,
    process_count: int | None = None,
    base_node_id: str | None = None,
    restart_delay: float = 1.0,
) -> None:
    """在父进程中守护 Pipeline 子进程，子进程退出后自动拉起 replacement。

    配合 FileWriterConfig.meta_writer_max_tasks_per_child 使用，用于通过进程轮转释放 OS 碎片内存。
    pipeline_factory 必须是 multiprocessing 可序列化的可调用对象；推荐使用模块顶层函数。
    """
    count = get_meta_writer_process_count() if process_count is None else max(1, process_count)
    base_node_id = base_node_id or get_node_id()
    processes: dict[int, multiprocessing.Process] = {}
    stopping = False

    def stop_children(*_: object) -> None:
        nonlocal stopping
        stopping = True
        for process in processes.values():
            if process.is_alive():
                process.terminate()
        for process in processes.values():
            process.join(timeout=10)
            if process.is_alive() and process.pid is not None:
                os.kill(process.pid, signal.SIGKILL)

    previous_sigint = signal.getsignal(signal.SIGINT)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, stop_children)
    signal.signal(signal.SIGTERM, stop_children)
    try:
        for process_index in range(count):
            processes[process_index] = _spawn_pipeline_process(pipeline_factory, base_node_id, process_index)

        while not stopping:
            for process_index, process in list(processes.items()):
                if process.exitcode is None:
                    continue

                if process.exitcode == 0:
                    logger.info(
                        "FileWriterPipeline child exited normally, restarting replacement, process=%s, exit_code=%s",
                        process.name,
                        process.exitcode,
                    )
                else:
                    logger.warning(
                        "FileWriterPipeline child exited unexpectedly, restarting replacement, process=%s, exit_code=%s",
                        process.name,
                        process.exitcode,
                    )
                process.join(timeout=0)
                time.sleep(max(0.0, restart_delay))
                processes[process_index] = _spawn_pipeline_process(pipeline_factory, base_node_id, process_index)

            for process in processes.values():
                process.join(timeout=1)
    finally:
        stop_children()
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
