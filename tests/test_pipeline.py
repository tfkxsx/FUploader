"""FileWriterPipeline 组装 & 生命周期测试。"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import re
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional

import pytest

from file_uploader.config import FileWriterConfig
from file_uploader.interfaces.message_source import MessageSource
from file_uploader.interfaces.object_store import ObjectStore
from file_uploader.interfaces.state_store import (
    SlotRuntimeState,
    StateStore,
)
from file_uploader.pipeline import FileWriterPipeline


# ---------------------------------------------------------------------------
# 内存态 StateStore（测试用）
# ---------------------------------------------------------------------------

@dataclass
class _MemSlotState:
    slot: int
    active_dir_id: str = ""
    active_count: int = 0
    dir_seq: int = 0


class InMemoryStateStore(StateStore):
    """线程安全的内存态 StateStore，专为测试设计。"""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._has_runtime = False
        self._init_lock_held: Optional[str] = None
        self._finalize_lock_held: Optional[str] = None
        self._slots: dict[int, _MemSlotState] = {}
        self._ready_dirs: list[str] = []
        self._pack_locks: dict[str, str] = {}
        self._disconnected = False

    # ---- connect / disconnect ----

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        self._disconnected = True

    # ---- 运行时状态 ----

    async def has_runtime_state(self) -> bool:
        return self._has_runtime

    async def initialize_runtime_state(
        self,
        slots: list[SlotRuntimeState],
        ready_dirs: list[str],
    ) -> None:
        async with self._lock:
            self._slots = {
                s.slot: _MemSlotState(
                    slot=s.slot,
                    active_dir_id=s.active_dir_id,
                    active_count=s.active_count,
                    dir_seq=s.dir_seq,
                )
                for s in slots
            }
            self._ready_dirs = list(ready_dirs)
            self._has_runtime = True

    # ---- init lock ----

    async def try_acquire_init_lock(self, token: str) -> bool:
        async with self._lock:
            if self._init_lock_held is not None:
                return False
            self._init_lock_held = token
            return True

    async def release_init_lock(self) -> None:
        async with self._lock:
            self._init_lock_held = None

    async def is_init_locked(self) -> bool:
        async with self._lock:
            return self._init_lock_held is not None

    async def try_acquire_finalize_lock(self, token: str) -> bool:
        async with self._lock:
            if self._finalize_lock_held is not None:
                return False
            self._finalize_lock_held = token
            return True

    async def release_finalize_lock(self) -> None:
        async with self._lock:
            self._finalize_lock_held = None

    # ---- slot ----

    async def get_slot_state(self, slot: int) -> Optional[SlotRuntimeState]:
        async with self._lock:
            ss = self._slots.get(slot)
            if ss is None:
                return None
            return SlotRuntimeState(
                slot=ss.slot,
                active_dir_id=ss.active_dir_id,
                active_count=ss.active_count,
                dir_seq=ss.dir_seq,
            )

    async def reset_slot_state(self, slot: int, *, active_count: int, dir_seq: int) -> None:
        async with self._lock:
            self._slots[slot] = _MemSlotState(
                slot=slot,
                active_dir_id=f"slot_{slot}_{max(dir_seq - 1, 0)}",
                active_count=active_count,
                dir_seq=dir_seq,
            )

    async def switch_slot_dir(self, slot: int, next_dir_id: str) -> None:
        async with self._lock:
            ss = self._slots.setdefault(slot, _MemSlotState(slot=slot))
            ss.active_dir_id = next_dir_id
            ss.active_count = 0
            try:
                ss.dir_seq = int(next_dir_id.rsplit("-", 1)[-1])
            except ValueError:
                ss.dir_seq += 1

    async def increment_slot_count(self, slot: int, delta: int) -> int:
        async with self._lock:
            ss = self._slots.setdefault(
                slot,
                _MemSlotState(slot=slot, active_dir_id=f"slot_{slot}_0"),
            )
            ss.active_count += delta
            return ss.active_count

    async def set_slot_active_count(self, slot: int, active_count: int) -> None:
        async with self._lock:
            ss = self._slots.setdefault(slot, _MemSlotState(slot=slot))
            ss.active_count = max(0, active_count)

    async def increment_and_check_threshold(self, slot: int, delta: int, threshold: int) -> tuple[bool, int]:
        new_count = await self.increment_slot_count(slot, delta)
        return new_count >= threshold, new_count

    async def increment_slot_active_count(self, slot_index: int, delta: int) -> int:
        return await self.increment_slot_count(slot_index, delta)

    async def get_slot_active_count(self, slot_index: int) -> int:
        async with self._lock:
            ss = self._slots.get(slot_index)
            return ss.active_count if ss else 0

    async def switch_slot_active_dir(self, slot_index: int, new_dir_id: str, *, reset_count: bool) -> None:
        async with self._lock:
            ss = self._slots.setdefault(slot_index, _MemSlotState(slot=slot_index))
            ss.active_dir_id = new_dir_id
            ss.dir_seq += 1
            if reset_count:
                ss.active_count = 0

    async def next_slot_dir(self, slot_index: int) -> tuple[str, int]:
        async with self._lock:
            ss = self._slots.setdefault(slot_index, _MemSlotState(slot=slot_index))
            ss.dir_seq += 1
            next_id = f"slot_{slot_index}_{ss.dir_seq}"
            return next_id, ss.dir_seq

    # ---- ready dir ----

    async def enqueue_ready_dir(self, ready_dir: str) -> None:
        async with self._lock:
            self._ready_dirs.append(ready_dir)

    async def pop_ready_dir(self) -> Optional[str]:
        return await self.dequeue_ready_dir()

    async def dequeue_ready_dir(self) -> Optional[str]:
        async with self._lock:
            if self._ready_dirs:
                return self._ready_dirs.pop(0)
            return None

    # ---- pack lock ----

    async def try_acquire_pack_lock(self, ready_dir: str, token: str) -> bool:
        return await self.lock_ready_dir(ready_dir, token)

    async def lock_ready_dir(self, ready_dir: str, token: str) -> bool:
        async with self._lock:
            if ready_dir in self._pack_locks:
                return False
            self._pack_locks[ready_dir] = token
            return True

    async def release_pack_lock(self, ready_dir: str, token: str) -> None:
        await self.unlock_ready_dir(ready_dir)

    async def unlock_ready_dir(self, ready_dir: str) -> None:
        async with self._lock:
            self._pack_locks.pop(ready_dir, None)

    # ---- runtime state clear (for recovery) ----

    async def clear_runtime_state(self) -> None:
        await self.clear_state()

    async def clear_state(self) -> None:
        async with self._lock:
            self._has_runtime = False
            self._slots.clear()
            self._ready_dirs.clear()


# ---------------------------------------------------------------------------
# 测试用 stub 策略函数
# ---------------------------------------------------------------------------

def _demo_parser(raw_body: bytes) -> dict:
    return json.loads(raw_body.decode())


def _demo_name_gen(data: dict) -> str:
    return f"{data.get('id', 'unknown')}.json"


def _demo_key_gen(archive_name: str) -> str:
    return f"test/prefix/{archive_name}"


async def _noop_packer(file_dir: Path) -> Path:
    """测试用打包器：创建一个空归档文件。"""
    archive_path = file_dir.with_suffix(".tar.zst")
    archive_path.write_bytes(b"")
    return archive_path


# ---------------------------------------------------------------------------
# 模拟 MessageSource（异步推消息）
# ---------------------------------------------------------------------------

@dataclass
class _FakeMessage:
    body: bytes
    _acked: bool = False
    _nacked: bool = False


class InMemoryMessageSource(MessageSource):
    """用 asyncio.Queue 模拟消息源。"""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._callback = None
        self._feed_task: Optional[asyncio.Task] = None

    async def start_consume(self, on_message) -> None:
        self._callback = on_message
        self._feed_task = asyncio.create_task(self._feed())

    async def _feed(self) -> None:
        while True:
            msg = await self._queue.get()
            if msg is None:  # 关闭信号
                break
            if self._callback:
                await self._callback(msg.body)

    async def ack(self, tag: Any) -> None:
        pass

    async def nack(self, tag: Any, requeue: bool = True) -> None:
        pass

    async def stop_consume(self) -> None:
        await self._queue.put(None)
        if self._feed_task is not None and self._feed_task is not asyncio.current_task():
            await self._feed_task
        self._feed_task = None

    async def set_prefetch_count(self, count: int) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def send(self, body: bytes) -> _FakeMessage:
        msg = _FakeMessage(body=body)
        await self._queue.put(msg)
        return msg


class ConcurrentMessageSource(InMemoryMessageSource):
    """并发调用 callback，模拟 RabbitMQ basic.consume 的在途消息。"""

    def __init__(self) -> None:
        super().__init__()
        self.prefetch_count: int | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    async def _feed(self) -> None:
        while True:
            msg = await self._queue.get()
            if msg is None:
                break
            if self._callback:
                task = asyncio.create_task(self._callback(msg.body))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks))

    async def set_prefetch_count(self, count: int) -> None:
        self.prefetch_count = count


# ---------------------------------------------------------------------------
# ObjectStore stub
# ---------------------------------------------------------------------------

class InMemoryObjectStore(ObjectStore):
    """把上传的文件存到内存 dict 中。"""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def upload(self, local_path: Path, remote_key: str) -> bool:
        self._store[remote_key] = local_path.read_bytes()
        return True

    async def exists(self, remote_key: str) -> bool:
        return remote_key in self._store


class FlakyObjectStore(InMemoryObjectStore):
    def __init__(self, failures_before_success: int) -> None:
        super().__init__()
        self._remaining_failures = failures_before_success

    async def upload(self, local_path: Path, remote_key: str) -> bool:
        if self._remaining_failures > 0:
            self._remaining_failures -= 1
            return False
        return await super().upload(local_path, remote_key)


class SlowInitStateStore(InMemoryStateStore):
    """StateStore that makes init visible as a blocking operation."""

    def __init__(self, started: asyncio.Event, release: asyncio.Event) -> None:
        super().__init__()
        self.started = started
        self.release = release
        self.initialize_calls = 0

    async def initialize_runtime_state(
        self,
        slots: list[SlotRuntimeState],
        ready_dirs: list[str],
    ) -> None:
        self.initialize_calls += 1
        self.started.set()
        await self.release.wait()
        await super().initialize_runtime_state(slots, ready_dirs)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_storage(tmp_path: Path) -> Path:
    return tmp_path / "file_uploader_data"


@pytest.fixture
def config(temp_storage: Path) -> FileWriterConfig:
    return FileWriterConfig(
        slot_count=2,
        pack_threshold=3,         # 小阈值便于触发封口
        storage_root=temp_storage,
        save_timeout=5.0,
        packer_concurrency=1,
        packer_interval=0.5,
        packer_max_retries=2,
        write_concurrency=2,
        meta_writer_max_tasks_per_child=0,
    )


@pytest.fixture
def state_store() -> InMemoryStateStore:
    return InMemoryStateStore()


@pytest.fixture
def message_source() -> InMemoryMessageSource:
    return InMemoryMessageSource()


@pytest.fixture
def object_store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


# ---------------------------------------------------------------------------
# 测试用例
# ---------------------------------------------------------------------------

class TestPipelineAssembly:
    """验证 Builder 模式 & 必需组件检查。"""

    def test_missing_components_raises(self) -> None:
        p = FileWriterPipeline()
        with pytest.raises(ValueError, match="Missing required"):
            p._validate()

    def test_full_builder_does_not_raise(self, config, state_store, message_source, object_store):
        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(message_source)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
        )
        p._validate()  # 不应该抛异常

    def test_no_recovery_flag(self, config, state_store):
        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(InMemoryMessageSource())
            .with_object_store(InMemoryObjectStore())
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )
        assert p._enable_recovery is False

    def test_legacy_redis_key_scope_matches_main_meta_writer(self):
        from file_uploader.adapters.redis_state_store import RedisStateStore

        store = RedisStateStore(
            key_style="legacy_meta",
            task_name="douyin_aweme",
            node_id="node_id-10.0.0.8-meta-p2",
            slot_count=4,
        )

        assert store._legacy_ready_queue_key == "douyin_aweme:meta:node_id-10.0.0.8:sealed_ready"
        assert store._legacy_init_lock_key == "douyin_aweme:meta:node_id-10.0.0.8:init_lock"
        assert store._slot_active_dir_key(2) == "douyin_aweme:meta:node_id-10.0.0.8:slot:2:active_dir"


class TestPipelineStartStop:
    """验证 Pipeline 启动 → 停止生命周期。"""

    async def test_start_and_stop(self, config, state_store, message_source, object_store):
        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(message_source)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )
        await p.start()
        assert p._writer is not None
        assert p._packer_svc is not None

        await p.stop()
        # 二次 stop 不抛异常
        await p.stop()

    async def test_start_initializes_runtime_state(
        self, config, state_store, message_source, object_store
    ):
        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(message_source)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )
        await p.start()
        assert await state_store.has_runtime_state() is True

    async def test_stop_finalizes_residual_files_when_enabled(
        self, config, state_store, object_store, tmp_path
    ):
        config = replace(
            config,
            storage_root=tmp_path / "shutdown_finalize",
            slot_count=1,
            pack_threshold=3,
            residue_file_upload=True,
            packer_interval=60.0,
        )
        msg_src = InMemoryMessageSource()
        pipeline = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await pipeline.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        for _ in range(20):
            if len(list(config.storage_root.rglob("*.json"))) == 2:
                break
            await asyncio.sleep(0.1)

        await pipeline.stop()

        assert len(object_store._store) == 1
        assert not list(config.storage_root.rglob("*.json"))
        assert await state_store.has_runtime_state() is False

    async def test_stop_skips_finalize_during_rotation(
        self, state_store, object_store, tmp_path
    ):
        config = FileWriterConfig(
            storage_root=tmp_path / "rotation_finalize_skip",
            slot_count=1,
            pack_threshold=10,
            residue_file_upload=True,
            meta_writer_max_tasks_per_child=1,
            packer_interval=60.0,
        )
        msg_src = InMemoryMessageSource()
        pipeline = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await pipeline.start()
        await msg_src.send(json.dumps({"id": "rotate"}).encode())

        for _ in range(30):
            if pipeline.rotation_requested:
                break
            await asyncio.sleep(0.1)

        await pipeline.stop()

        assert pipeline.rotation_requested is True
        assert len(object_store._store) == 0
        assert list(config.storage_root.rglob("*.json"))

    async def test_stop_finalize_failure_raises_and_keeps_runtime_state(
        self, config, state_store, tmp_path
    ):
        config = replace(
            config,
            storage_root=tmp_path / "shutdown_finalize_failure",
            slot_count=1,
            pack_threshold=3,
            residue_file_upload=True,
            packer_interval=60.0,
            packer_max_retries=0,
        )
        object_store = FlakyObjectStore(failures_before_success=10)
        msg_src = InMemoryMessageSource()
        pipeline = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await pipeline.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        for _ in range(20):
            if len(list(config.storage_root.rglob("*.json"))) == 2:
                break
            await asyncio.sleep(0.1)

        with pytest.raises(RuntimeError, match="Residual finalize failed"):
            await pipeline.stop()

        assert await state_store.has_runtime_state() is True
        assert await state_store.dequeue_ready_dir() == "0:slot_0_0"
        assert list(config.storage_root.rglob("*.json"))

    async def test_stop_finalize_retries_failed_upload(
        self, config, state_store, tmp_path
    ):
        config = replace(
            config,
            storage_root=tmp_path / "shutdown_finalize_retry",
            slot_count=1,
            pack_threshold=3,
            residue_file_upload=True,
            packer_interval=60.0,
            packer_max_retries=2,
        )
        object_store = FlakyObjectStore(failures_before_success=1)
        msg_src = InMemoryMessageSource()
        pipeline = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await pipeline.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        for _ in range(20):
            if len(list(config.storage_root.rglob("*.json"))) == 2:
                break
            await asyncio.sleep(0.1)

        await pipeline.stop()

        assert len(object_store._store) == 1
        assert await state_store.has_runtime_state() is False

    async def test_concurrent_start_bootstraps_runtime_state_once(self, config, object_store):
        started = asyncio.Event()
        release = asyncio.Event()
        store = SlowInitStateStore(started, release)
        sources = [InMemoryMessageSource(), InMemoryMessageSource()]

        pipelines = [
            (
                FileWriterPipeline.with_config(config)
                .with_state_store(store)
                .with_message_source(source)
                .with_object_store(object_store)
                .with_message_parser(_demo_parser)
                .with_file_name_generator(_demo_name_gen)
                .with_packer(_noop_packer)
                .with_remote_key_generator(_demo_key_gen)
                .no_recovery()
            )
            for source in sources
        ]

        tasks = [asyncio.create_task(p.start()) for p in pipelines]
        await asyncio.wait_for(started.wait(), timeout=2)
        await asyncio.sleep(0.1)
        assert store.initialize_calls == 1
        assert not tasks[0].done() or not tasks[1].done()

        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=3)
        assert store.initialize_calls == 1

        await asyncio.gather(*(p.stop() for p in pipelines))

    async def test_legacy_recovery_repartitions_existing_runtime_state(self, state_store, object_store, tmp_path):
        config = FileWriterConfig(
            storage_root=tmp_path / "legacy_recovery",
            slot_count=2,
            pack_threshold=100,
            slot_strategy="worker_index",
            worker_index=0,
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            meta_writer_max_tasks_per_child=0,
            packer_interval=60.0,
        )
        for slot, count in ((0, 71), (1, 70)):
            active_dir = config.storage_root / "active" / f"slot-{slot}" / "dir-000002"
            active_dir.mkdir(parents=True)
            for index in range(count):
                (active_dir / f"{slot}_{index}.json").write_text("{}", encoding="utf-8")
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="dir-000002", active_count=71, dir_seq=2),
                SlotRuntimeState(slot=1, active_dir_id="dir-000002", active_count=70, dir_seq=2),
            ],
            ready_dirs=[],
        )

        pipeline = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(InMemoryMessageSource())
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
        )

        await pipeline.start()

        sealed_files = list((config.storage_root / "sealed" / "slot-0" / "dir-000001").glob("*.json"))
        active_files = list((config.storage_root / "active" / "slot-0" / "dir-000002").glob("*.json"))
        assert len(sealed_files) == 100
        assert len(active_files) == 41
        assert not (config.storage_root / "active" / "slot-1" / "dir-000002").exists()
        slot0_state = await state_store.get_slot_state(0)
        slot1_state = await state_store.get_slot_state(1)
        assert slot0_state is not None
        assert slot0_state.active_count == 41
        assert slot0_state.active_dir_id == "dir-000002"
        assert slot1_state is not None
        assert slot1_state.active_count == 0
        assert await state_store.pop_ready_dir() == "slot-0:dir-000001"

        await pipeline.stop()


class TestEndToEnd:
    """端到端：发消息 → 写文件 → 封口 → 打包 → 上传。"""

    async def test_write_and_pack_flow(self, config, state_store, object_store, tmp_path):
        """发送 pack_threshold 条消息，验证打包 & 上传。"""
        config = replace(config, storage_root=tmp_path / "e2e_storage", slot_count=1)
        msg_src = InMemoryMessageSource()

        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(object_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await p.start()

        # 发送恰好等于 pack_threshold 的消息
        for i in range(config.pack_threshold):
            body = json.dumps({"id": f"item_{i}"}).encode()
            await msg_src.send(body)

        # 等待 writer 完成写入 + 封口 + packer 消费处理
        for _ in range(40):  # 最多等 20s
            if object_store._store:
                break
            await asyncio.sleep(0.5)

        await p.stop()

        # 验证至少有 1 个归档上传成功
        assert len(object_store._store) >= 1, f"Expected at least 1 upload, got {len(object_store._store)}"
        for key, data in object_store._store.items():
            assert key.startswith("test/prefix/")
            assert isinstance(data, bytes)

    async def test_slot_distribution(self, config, state_store, tmp_path):
        """验证消息被分配到不同的 slot。"""
        config = replace(config, storage_root=tmp_path / "slot_test")
        msg_src = InMemoryMessageSource()
        obj_store = InMemoryObjectStore()

        p = (
            FileWriterPipeline.with_config(config)
            .with_state_store(state_store)
            .with_message_source(msg_src)
            .with_object_store(obj_store)
            .with_message_parser(_demo_parser)
            .with_file_name_generator(_demo_name_gen)
            .with_packer(_noop_packer)
            .with_remote_key_generator(_demo_key_gen)
            .no_recovery()
        )

        await p.start()

        # 发送大量消息
        for i in range(100):
            body = json.dumps({"id": f"item_{i}"}).encode()
            await msg_src.send(body)
            await asyncio.sleep(0)  # yield control

        # 等待写入完成
        await asyncio.sleep(2)

        # 检查 slot 目录中有文件
        slot_dirs = sorted(
            d for d in config.storage_root.iterdir() if d.is_dir() and d.name.startswith("slot_")
        )
        # 至少有 slot 目录被创建
        assert len(slot_dirs) >= 1

        total_files = sum(1 for d in slot_dirs for f in d.iterdir() if f.is_file())
        assert total_files > 0

        await p.stop()


# ---------------------------------------------------------------------------
# MetaWriter 直接测试
# ---------------------------------------------------------------------------

class TestMetaWriter:
    def test_config_defaults_enable_writer_rotation(self, temp_storage):
        cfg = FileWriterConfig(storage_root=temp_storage)

        assert cfg.meta_writer_max_tasks_per_child == 1000

    def test_legacy_meta_config_uses_worker_slot(self, temp_storage, monkeypatch):
        monkeypatch.setenv("NODE_ID", "node_id-10.0.0.8-meta-p1")
        monkeypatch.setenv("META_WRITER_PROCESS_COUNT", "4")
        monkeypatch.setenv("META_WRITER_RABBITMQ_PREFETCH_COUNT", "20")
        monkeypatch.setenv("META_WRITER_MAX_TASKS_PER_CHILD", "77")

        cfg = FileWriterConfig.legacy_meta(
            storage_root=temp_storage,
            task_name="douyin_aweme",
        )

        assert cfg.node_id == "node_id-10.0.0.8-meta-p1"
        assert cfg.worker_index == 1
        assert cfg.slot_count == 4
        assert cfg.meta_writer_process_count == 4
        assert cfg.prefetch_count == 20
        assert cfg.meta_writer_max_tasks_per_child == 77
        assert cfg.slot_strategy == "worker_index"
        assert cfg.storage_layout == "legacy_meta"
        assert cfg.ready_dir_format == "legacy_slot"

    def test_meta_writer_process_count_reads_env(self, monkeypatch):
        from file_uploader import (
            get_meta_writer_max_tasks_per_child,
            get_meta_writer_process_count,
            get_meta_writer_rabbitmq_prefetch_count,
            get_residue_file_upload,
        )

        monkeypatch.setenv("META_WRITER_PROCESS_COUNT", "6")
        monkeypatch.setenv("META_WRITER_RABBITMQ_PREFETCH_COUNT", "24")
        monkeypatch.setenv("META_WRITER_MAX_TASKS_PER_CHILD", "88")
        monkeypatch.setenv("RESIDUE_FILE_UPLOADE", "true")

        assert get_meta_writer_process_count() == 6
        assert get_meta_writer_rabbitmq_prefetch_count() == 24
        assert get_meta_writer_max_tasks_per_child() == 88
        assert get_residue_file_upload() is True

    async def test_write_one_message(self, config, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = replace(config, storage_root=tmp_path / "writer_test")
        msg_src = InMemoryMessageSource()

        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )

        # 初始化 runtime state
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=i, active_dir_id=f"slot_{i}_0", active_count=0, dir_seq=0)
                for i in range(config.slot_count)
            ],
            ready_dirs=[],
        )
        # 创建 slot 目录
        for i in range(config.slot_count):
            (config.storage_root / f"slot_{i}_0").mkdir(parents=True, exist_ok=True)

        await msg_src.send(json.dumps({"id": "test_1"}).encode())

        await writer.start()

        # 等待写入
        for _ in range(20):
            files = list(config.storage_root.rglob("*.json"))
            if files:
                break
            await asyncio.sleep(0.5)

        files = list(config.storage_root.rglob("*.json"))
        assert len(files) >= 1, f"Expected at least 1 file, got {files}"

        await writer.stop()

    async def test_write_concurrency_runs_multiple_writes_in_parallel(self, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = FileWriterConfig(
            storage_root=tmp_path / "concurrent_writer",
            slot_count=1,
            pack_threshold=2,
            write_concurrency=2,
            prefetch_count=1,
            meta_writer_process_count=3,
            meta_writer_max_tasks_per_child=0,
        )
        msg_src = ConcurrentMessageSource()
        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=i, active_dir_id=f"slot_{i}_0", active_count=0, dir_seq=0)
                for i in range(config.slot_count)
            ],
            ready_dirs=[],
        )

        active_writes = 0
        max_active_writes = 0

        async def slow_write(path: Path, content: bytes) -> None:
            nonlocal active_writes, max_active_writes
            active_writes += 1
            max_active_writes = max(max_active_writes, active_writes)
            await asyncio.sleep(0.1)
            path.write_bytes(content)
            active_writes -= 1

        writer._write_file = slow_write  # type: ignore[method-assign]
        await writer.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        for _ in range(30):
            if writer._save_ok == 2:
                break
            await asyncio.sleep(0.05)

        assert msg_src.prefetch_count == 2
        assert writer._effective_prefetch_count == config.write_concurrency
        assert writer._write_queue.maxsize == 3
        assert max_active_writes == 2
        assert writer._save_ok == 2
        assert await state_store.pop_ready_dir() == "0:slot_0_0"
        slot_state = await state_store.get_slot_state(0)
        assert slot_state is not None
        assert slot_state.active_count == 0
        await writer.stop()

    async def test_write_file_runs_on_multiple_executor_threads(self, config, state_store, tmp_path, monkeypatch):
        from file_uploader.services.meta_writer import MetaWriter

        writer = MetaWriter(
            config=replace(config, storage_root=tmp_path / "threaded_writer"),
            state_store=state_store,
            message_source=InMemoryMessageSource(),
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )
        original_write_bytes = Path.write_bytes
        barrier = threading.Barrier(2)
        thread_ids: set[int] = set()
        thread_ids_lock = threading.Lock()

        def blocking_write_bytes(path: Path, content: bytes) -> int:
            with thread_ids_lock:
                thread_ids.add(threading.get_ident())
            barrier.wait(timeout=2)
            return original_write_bytes(path, content)

        monkeypatch.setattr(Path, "write_bytes", blocking_write_bytes)
        target_dir = tmp_path / "threaded_writer"
        target_dir.mkdir(parents=True)

        await asyncio.gather(
            writer._write_file(target_dir / "a.json", b"{}"),
            writer._write_file(target_dir / "b.json", b"{}"),
        )

        assert len(thread_ids) == 2
        await writer.stop()

    async def test_processing_failure_propagates_for_message_source_nack(self, config, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = replace(config, storage_root=tmp_path / "failed_writer")
        msg_src = InMemoryMessageSource()
        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=i, active_dir_id=f"slot_{i}_0", active_count=0, dir_seq=0)
                for i in range(config.slot_count)
            ],
            ready_dirs=[],
        )
        await writer.start()

        with pytest.raises(json.JSONDecodeError):
            await writer._handle_message(b"not-json")

        await writer.stop()

    async def test_seal_on_threshold(self, config, state_store, tmp_path):
        """发送 pack_threshold 条消息后触发封口。"""
        from file_uploader.services.meta_writer import MetaWriter

        config = replace(
            config,
            storage_root=tmp_path / "seal_test",
            pack_threshold=2,
            slot_count=1,
        )

        msg_src = InMemoryMessageSource()

        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )

        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="slot_0_0", active_count=0, dir_seq=0)
            ],
            ready_dirs=[],
        )
        (config.storage_root / "slot_0_0").mkdir(parents=True, exist_ok=True)

        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        await writer.start()

        # 等待封口：ready_dir 队列应有内容
        for _ in range(20):
            rd = await state_store.pop_ready_dir()
            if rd is not None:
                # 放回去供 packer 消费（这里只验证 writer 行为）
                await state_store.enqueue_ready_dir(rd)
                break
            await asyncio.sleep(0.5)

        has_ready = await state_store.pop_ready_dir()
        if has_ready is not None:
            await state_store.enqueue_ready_dir(has_ready)
        assert has_ready is not None, "Expected a ready_dir after sealing"

        await writer.stop()

    async def test_rotation_requested_after_max_tasks(self, config, state_store, tmp_path):
        """成功写入达到 max_tasks_per_child 后停止消费并通知外层。"""
        from file_uploader.services.meta_writer import MetaWriter

        config = FileWriterConfig(
            storage_root=tmp_path / "rotation_test",
            slot_count=1,
            pack_threshold=100,
            save_timeout=5.0,
            packer_concurrency=1,
            packer_interval=0.5,
            packer_max_retries=2,
            write_concurrency=2,
            meta_writer_max_tasks_per_child=2,
        )
        msg_src = InMemoryMessageSource()
        rotation_event = asyncio.Event()

        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
            on_rotation_requested=rotation_event.set,
        )

        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="slot_0_0", active_count=0, dir_seq=0)
            ],
            ready_dirs=[],
        )
        (config.storage_root / "slot_0_0").mkdir(parents=True, exist_ok=True)

        await writer.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        await asyncio.wait_for(rotation_event.wait(), timeout=5)
        for _ in range(20):
            if writer._running is False:
                break
            await asyncio.sleep(0.1)

        assert writer._save_ok == 2
        assert writer._running is False

    async def test_legacy_worker_writes_only_own_slot_and_seals(self, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = FileWriterConfig(
            storage_root=tmp_path / "legacy_writer",
            slot_count=3,
            pack_threshold=2,
            worker_index=1,
            slot_strategy="worker_index",
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            meta_writer_max_tasks_per_child=0,
        )
        msg_src = InMemoryMessageSource()
        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="dir-000001", active_count=0, dir_seq=1),
                SlotRuntimeState(slot=1, active_dir_id="dir-000001", active_count=0, dir_seq=1),
                SlotRuntimeState(slot=2, active_dir_id="dir-000001", active_count=0, dir_seq=1),
            ],
            ready_dirs=[],
        )

        await writer.start()
        await msg_src.send(json.dumps({"id": "a"}).encode())
        await msg_src.send(json.dumps({"id": "b"}).encode())

        for _ in range(20):
            ready = await state_store.pop_ready_dir()
            if ready is not None:
                await state_store.enqueue_ready_dir(ready)
                break
            await asyncio.sleep(0.2)

        assert (config.storage_root / "sealed" / "slot-1" / "dir-000001").is_dir()
        assert not (config.storage_root / "sealed" / "slot-0" / "dir-000001").exists()
        assert (config.storage_root / "active" / "slot-1" / "dir-000002").is_dir()
        assert await state_store.pop_ready_dir() == "slot-1:dir-000001"

        await writer.stop()

    async def test_legacy_worker_seals_over_threshold_slot_on_start(self, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = FileWriterConfig(
            storage_root=tmp_path / "legacy_startup_seal",
            slot_count=2,
            pack_threshold=3,
            worker_index=1,
            slot_strategy="worker_index",
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            meta_writer_max_tasks_per_child=0,
        )
        active_dir = config.storage_root / "active" / "slot-1" / "dir-000001"
        active_dir.mkdir(parents=True)
        for index in range(3):
            (active_dir / f"{index}.json").write_text("{}", encoding="utf-8")

        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="dir-000001", active_count=0, dir_seq=1),
                SlotRuntimeState(slot=1, active_dir_id="dir-000001", active_count=3, dir_seq=1),
            ],
            ready_dirs=[],
        )

        msg_src = InMemoryMessageSource()
        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )

        await writer.start()

        assert (config.storage_root / "sealed" / "slot-1" / "dir-000001").is_dir()
        assert (config.storage_root / "active" / "slot-1" / "dir-000002").is_dir()
        assert await state_store.pop_ready_dir() == "slot-1:dir-000001"
        slot_state = await state_store.get_slot_state(1)
        assert slot_state is not None
        assert slot_state.active_count == 0
        assert slot_state.active_dir_id == "dir-000002"

        await writer.stop()

    async def test_legacy_worker_trusts_disk_count_on_start(self, state_store, tmp_path):
        from file_uploader.services.meta_writer import MetaWriter

        config = FileWriterConfig(
            storage_root=tmp_path / "legacy_startup_calibrate",
            slot_count=2,
            pack_threshold=3,
            worker_index=1,
            slot_strategy="worker_index",
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            meta_writer_max_tasks_per_child=0,
        )
        active_dir = config.storage_root / "active" / "slot-1" / "dir-000001"
        active_dir.mkdir(parents=True)
        (active_dir / "only_one.json").write_text("{}", encoding="utf-8")

        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="dir-000001", active_count=0, dir_seq=1),
                SlotRuntimeState(slot=1, active_dir_id="dir-000001", active_count=99, dir_seq=1),
            ],
            ready_dirs=[],
        )

        msg_src = InMemoryMessageSource()
        writer = MetaWriter(
            config=config,
            state_store=state_store,
            message_source=msg_src,
            message_parser=_demo_parser,
            file_name_generator=_demo_name_gen,
        )

        await writer.start()

        assert not (config.storage_root / "sealed" / "slot-1" / "dir-000001").exists()
        assert await state_store.pop_ready_dir() is None
        slot_state = await state_store.get_slot_state(1)
        assert slot_state is not None
        assert slot_state.active_count == 1
        assert slot_state.active_dir_id == "dir-000001"

        await writer.stop()


# ---------------------------------------------------------------------------
# MetaPacker 直接测试
# ---------------------------------------------------------------------------

class TestMetaPacker:
    async def test_pack_ready_dir(self, config, state_store, tmp_path):
        from file_uploader.services.meta_packer import MetaPacker

        config = replace(config, storage_root=tmp_path / "packer_test")
        obj_store = InMemoryObjectStore()

        # 准备一个 ready_dir
        slot_dir = config.storage_root / "slot_0_1"
        slot_dir.mkdir(parents=True)
        (slot_dir / "file_a.json").write_text('{"id":"a"}')
        (slot_dir / "file_b.json").write_text('{"id":"b"}')

        ready_dir = "0:slot_0_1"

        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="slot_0_0", active_count=0, dir_seq=1)
            ],
            ready_dirs=[ready_dir],
        )

        packer = MetaPacker(
            config=config,
            state_store=state_store,
            object_store=obj_store,
            packer=_noop_packer,
            remote_key_generator=_demo_key_gen,
        )

        await packer.start()

        for _ in range(20):
            if obj_store._store:
                break
            await asyncio.sleep(0.5)

        assert len(obj_store._store) >= 1
        await packer.stop()

    async def test_legacy_packer_reads_sealed_slot_dir(self, state_store, tmp_path):
        from file_uploader.services.meta_packer import MetaPacker

        config = FileWriterConfig(
            storage_root=tmp_path / "legacy_packer",
            slot_count=2,
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            packer_interval=0.1,
        )
        obj_store = InMemoryObjectStore()
        sealed_dir = config.storage_root / "sealed" / "slot-1" / "dir-000001"
        sealed_dir.mkdir(parents=True)
        (sealed_dir / "a.json").write_text('{"id":"a"}')
        await state_store.initialize_runtime_state(
            slots=[
                SlotRuntimeState(slot=0, active_dir_id="dir-000001", active_count=0, dir_seq=1),
                SlotRuntimeState(slot=1, active_dir_id="dir-000002", active_count=0, dir_seq=2),
            ],
            ready_dirs=["slot-1:dir-000001"],
        )
        packer = MetaPacker(
            config=config,
            state_store=state_store,
            object_store=obj_store,
            packer=_noop_packer,
            remote_key_generator=_demo_key_gen,
        )

        await packer.start()
        for _ in range(20):
            if obj_store._store:
                break
            await asyncio.sleep(0.2)

        assert len(obj_store._store) == 1
        assert re.search(r"\d{8}_\d{6}_\d{6}_[0-9a-f]{8}\.tar\.zst$", next(iter(obj_store._store)))
        assert not sealed_dir.exists()
        await packer.stop()


class TestFileMessages:
    async def test_publish_file_messages_encodes_raw_payload(self):
        from file_uploader import file_message_name, parse_file_message, publish_file_messages

        bodies: list[str] = []

        async def publish(body: str) -> None:
            bodies.append(body)

        await publish_file_messages(
            [
                {
                    "file_name": "1001",
                    "payload": {"aweme_id": "1001", "desc": "hello"},
                }
            ],
            publish,
        )

        assert len(bodies) == 1
        envelope = json.loads(bodies[0])
        assert envelope["file_name"] == "1001"
        assert envelope["compression"] == "gzip"
        assert envelope["content_type"] == "json"

        decoded = parse_file_message(bodies[0])
        assert decoded.file_name == "1001"
        assert decoded.file_id == "1001"
        assert decoded.payload == {"aweme_id": "1001", "desc": "hello"}
        assert file_message_name(decoded) == "1001.json"

    async def test_publish_file_messages_generates_uuid_file_name_when_missing(self):
        from file_uploader import file_message_name, parse_file_message, publish_file_messages

        bodies: list[str] = []

        async def publish(body: str) -> None:
            bodies.append(body)

        await publish_file_messages([{"payload": {"ok": True}}], publish)

        decoded = parse_file_message(bodies[0])
        assert re.fullmatch(r"[0-9a-f]{32}", decoded.file_name)
        assert file_message_name(decoded) == f"{decoded.file_name}.json"

    def test_file_message_name_does_not_duplicate_json_suffix(self):
        from file_uploader import DecodedFilePayload, file_message_name

        decoded = DecodedFilePayload(file_name="abc.json", payload={}, content_type="json")
        assert file_message_name(decoded) == "abc.json"

    def test_parse_file_message_supports_legacy_aweme_id_envelope(self):
        from file_uploader import parse_file_message

        raw_payload = json.dumps({"aweme_id": "2002"}, ensure_ascii=False).encode("utf-8")
        envelope = {
            "aweme_id": "2002",
            "compression": "gzip",
            "payload": base64.b64encode(gzip.compress(raw_payload)).decode("ascii"),
        }

        decoded = parse_file_message(json.dumps(envelope).encode("utf-8"))
        assert decoded.file_name == "2002"
        assert decoded.payload == {"aweme_id": "2002"}
