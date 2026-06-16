"""Redis 状态存储适配器。"""

from __future__ import annotations

import logging
from typing import Optional

import redis.asyncio as aioredis

from ..interfaces.state_store import SlotRuntimeState, StateStore
from ..layout import extract_dir_seq
from ..runtime import get_node_id, node_scope

logger = logging.getLogger(__name__)

# -- Lua 脚本 (保证原子性) --

INCREMENT_AND_CHECK = r"""
local key = KEYS[1]
local threshold = tonumber(ARGV[1])
local delta = tonumber(ARGV[2])
local new_val = redis.call('HINCRBY', key, 'active_count', delta)
if new_val >= threshold then
    return {1, new_val}
else
    return {0, new_val}
end
"""

LOCK_READY_DIR = r"""
local lock_key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local result = redis.call('SET', lock_key, token, 'NX', 'EX', ttl)
return result and true or false
"""

LEGACY_INCREMENT_AND_CHECK = r"""
local key = KEYS[1]
local threshold = tonumber(ARGV[1])
local delta = tonumber(ARGV[2])
local new_val = redis.call('INCRBY', key, delta)
if new_val >= threshold then
    return {1, new_val}
else
    return {0, new_val}
end
"""


class RedisStateStore(StateStore):
    """基于 Redis 的 StateStore 实现。

    用法::

        store = RedisStateStore(
            host="localhost", port=6379, db=0,
            password="",
            key_prefix="file_uploader:task1:",
        )
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str = "",
        key_prefix: str = "file_uploader:",
        lock_ttl: int = 300,
        key_style: str = "default",
        task_name: str = "",
        node_id: str = "",
        slot_count: int = 1,
    ) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._prefix = key_prefix
        self._lock_ttl = lock_ttl
        self._key_style = key_style
        self._task_name = task_name
        self._node_id = node_id or get_node_id()
        self._slot_count = max(1, slot_count)
        self._legacy_prefix = f"{task_name}:meta:{node_scope(self._node_id)}" if task_name else ""
        self._client: Optional[aioredis.Redis] = None

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("RedisStateStore not connected. Call connect() first.")
        return self._client

    async def connect(self) -> None:
        """建立 Redis 连接。"""
        self._client = aioredis.Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password or None,
            decode_responses=True,
        )
        await self._client.ping()
        logger.info("RedisStateStore connected to %s:%s db=%s", self._host, self._port, self._db)

    async def disconnect(self) -> None:
        """关闭 Redis 连接。"""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("RedisStateStore disconnected")

    # -- Key 工具 --

    def _k(self, *parts: str) -> str:
        return f"{self._prefix}{':'.join(parts)}"

    def _legacy_k(self, *parts: str) -> str:
        if not self._legacy_prefix:
            raise ValueError("task_name is required when key_style='legacy_meta'")
        return f"{self._legacy_prefix}:{':'.join(parts)}"

    def _slot_active_dir_key(self, slot: int) -> str:
        return self._legacy_k(f"slot:{max(0, slot)}:active_dir")

    def _slot_active_count_key(self, slot: int) -> str:
        return self._legacy_k(f"slot:{max(0, slot)}:active_count")

    def _slot_dir_seq_key(self, slot: int) -> str:
        return self._legacy_k(f"slot:{max(0, slot)}:dir_seq")

    @property
    def _legacy_ready_queue_key(self) -> str:
        return self._legacy_k("sealed_ready")

    @property
    def _legacy_init_lock_key(self) -> str:
        return self._legacy_k("init_lock")

    @property
    def _legacy_finalize_lock_key(self) -> str:
        return self._legacy_k("finalize_lock")

    # -- 初始化锁（SET NX 实现） --

    async def try_acquire_init_lock(self, token: str) -> bool:
        if self._key_style == "legacy_meta":
            return bool(await self.client.set(self._legacy_init_lock_key, token, nx=True, ex=self._lock_ttl))
        key = self._k("lock:init")
        return bool(await self.client.set(key, token, nx=True, ex=self._lock_ttl))

    async def release_init_lock(self) -> None:
        if self._key_style == "legacy_meta":
            await self.client.delete(self._legacy_init_lock_key)
            return
        key = self._k("lock:init")
        await self.client.delete(key)

    async def is_init_locked(self) -> bool:
        if self._key_style == "legacy_meta":
            return bool(await self.client.exists(self._legacy_init_lock_key))
        return bool(await self.client.exists(self._k("lock:init")))

    async def try_acquire_finalize_lock(self, token: str) -> bool:
        if self._key_style == "legacy_meta":
            return bool(await self.client.set(self._legacy_finalize_lock_key, token, nx=True, ex=self._lock_ttl))
        return bool(await self.client.set(self._k("lock:finalize"), token, nx=True, ex=self._lock_ttl))

    async def release_finalize_lock(self) -> None:
        if self._key_style == "legacy_meta":
            await self.client.delete(self._legacy_finalize_lock_key)
            return
        await self.client.delete(self._k("lock:finalize"))

    # -- 运行时状态 --

    async def has_runtime_state(self) -> bool:
        if self._key_style == "legacy_meta":
            for slot in range(self._slot_count):
                if await self.client.get(self._slot_active_dir_key(slot)) is not None:
                    return True
            return False
        key = self._k("runtime:initialized")
        return bool(await self.client.exists(key))

    async def initialize_runtime_state(self, slots: list, ready_dirs: list) -> None:
        if self._key_style == "legacy_meta":
            pipe = self.client.pipeline()
            for s in slots:
                pipe.set(self._slot_active_dir_key(s.slot), s.active_dir_id)
                pipe.set(self._slot_active_count_key(s.slot), max(0, s.active_count))
                pipe.set(self._slot_dir_seq_key(s.slot), max(1, s.dir_seq))
            if ready_dirs:
                pipe.rpush(self._legacy_ready_queue_key, *ready_dirs)
            await pipe.execute()
            return
        pipe = self.client.pipeline()
        for s in slots:
            base = self._k(f"slot:{s.slot}")
            pipe.hset(base, mapping={
                "active_dir_id": s.active_dir_id,
                "active_count": str(s.active_count),
                "dir_seq": str(s.dir_seq),
            })
        if ready_dirs:
            pipe.rpush(self._k("queue:ready"), *ready_dirs)
        pipe.set(self._k("runtime:initialized"), "1")
        await pipe.execute()

    # -- Slot 状态操作 --

    async def get_slot_state(self, slot: int) -> Optional[SlotRuntimeState]:
        if self._key_style == "legacy_meta":
            active_dir = str(await self.client.get(self._slot_active_dir_key(slot)) or "").strip()
            active_count = int(await self.client.get(self._slot_active_count_key(slot)) or 0)
            dir_seq = int(await self.client.get(self._slot_dir_seq_key(slot)) or 0)
            if not active_dir:
                return None
            return SlotRuntimeState(
                slot=slot,
                active_dir_id=active_dir,
                active_count=max(0, active_count),
                dir_seq=max(1, dir_seq),
            )
        key = self._k(f"slot:{slot}")
        data = await self.client.hgetall(key)
        if not data:
            return None
        return SlotRuntimeState(
            slot=slot,
            active_dir_id=data["active_dir_id"],
            active_count=int(data["active_count"]),
            dir_seq=int(data["dir_seq"]),
        )

    async def reset_slot_state(self, slot: int, *, active_count: int, dir_seq: int) -> None:
        """重置 slot 状态（启动残留恢复用）。"""
        if self._key_style == "legacy_meta":
            prev_seq = max(dir_seq, 1)
            active_dir_id = f"dir-{prev_seq:06d}"
            pipe = self.client.pipeline()
            pipe.set(self._slot_active_dir_key(slot), active_dir_id)
            pipe.set(self._slot_active_count_key(slot), max(0, active_count))
            pipe.set(self._slot_dir_seq_key(slot), max(1, dir_seq))
            await pipe.execute()
            logger.info("Reset legacy slot %s: dir=%s count=%s seq=%s", slot, active_dir_id, active_count, dir_seq)
            return
        prev_seq = max(dir_seq - 1, 0)
        active_dir_id = f"slot_{slot}_{prev_seq}"
        key = self._k(f"slot:{slot}")
        await self.client.hset(
            key,
            mapping={
                "active_dir_id": active_dir_id,
                "active_count": str(active_count),
                "dir_seq": str(dir_seq),
            },
        )
        logger.info("Reset slot %s: dir=%s count=%s seq=%s", slot, active_dir_id, active_count, dir_seq)

    async def switch_slot_dir(self, slot: int, next_dir_id: str) -> None:
        if self._key_style == "legacy_meta":
            next_seq = max(1, extract_dir_seq(next_dir_id))
            pipe = self.client.pipeline()
            pipe.set(self._slot_active_dir_key(slot), next_dir_id)
            pipe.set(self._slot_active_count_key(slot), 0)
            pipe.set(self._slot_dir_seq_key(slot), next_seq)
            await pipe.execute()
            return
        key = self._k(f"slot:{slot}")
        pipe = self.client.pipeline()
        pipe.hset(key, "active_dir_id", next_dir_id)
        pipe.hset(key, "active_count", "0")
        pipe.hincrby(key, "dir_seq", 1)
        await pipe.execute()

    async def increment_slot_count(self, slot: int, delta: int) -> int:
        if self._key_style == "legacy_meta":
            return int(await self.client.incrby(self._slot_active_count_key(slot), delta))
        key = self._k(f"slot:{slot}")
        return await self.client.hincrby(key, "active_count", delta)

    async def set_slot_active_count(self, slot: int, active_count: int) -> None:
        active_count = max(0, active_count)
        if self._key_style == "legacy_meta":
            await self.client.set(self._slot_active_count_key(slot), active_count)
            return
        key = self._k(f"slot:{slot}")
        await self.client.hset(key, "active_count", str(active_count))

    # -- 触发封口（阈值检查） --

    async def increment_and_check_threshold(self, slot: int, delta: int, threshold: int) -> tuple[bool, int]:
        """原子增加 slot 计数并检查是否达到封口阈值。

        Returns:
            (should_seal, new_count)
        """
        if self._key_style == "legacy_meta":
            try:
                result = await self.client.eval(
                    LEGACY_INCREMENT_AND_CHECK,
                    1,
                    self._slot_active_count_key(slot),
                    str(threshold),
                    str(delta),
                )
                return bool(result[0]), int(result[1])
            except aioredis.ResponseError:
                new_count = await self.increment_slot_count(slot, delta)
                return new_count >= threshold, new_count
        key = self._k(f"slot:{slot}")
        try:
            result = await self.client.eval(
                INCREMENT_AND_CHECK,
                1,
                key,
                str(threshold),
                str(delta),
            )
            return bool(result[0]), int(result[1])
        except aioredis.ResponseError:
            # fallback: 非原子但兼容
            new_count = await self.increment_slot_count(slot, delta)
            return new_count >= threshold, new_count

    # -- 待打包队列 --

    async def enqueue_ready_dir(self, ready_dir: str) -> None:
        if self._key_style == "legacy_meta":
            await self.client.rpush(self._legacy_ready_queue_key, ready_dir)
            return
        await self.client.rpush(self._k("queue:ready"), ready_dir)

    async def dequeue_ready_dir(self) -> Optional[str]:
        if self._key_style == "legacy_meta":
            return await self.client.lpop(self._legacy_ready_queue_key)
        return await self.client.lpop(self._k("queue:ready"))

    async def lock_ready_dir(self, ready_dir: str, token: str) -> bool:
        lock_key = (
            self._legacy_k(f"pack_lock:{ready_dir}")
            if self._key_style == "legacy_meta"
            else self._k(f"pack_lock:{ready_dir}")
        )
        try:
            result = await self.client.eval(
                LOCK_READY_DIR,
                1,
                lock_key,
                token,
                str(self._lock_ttl),
            )
            return bool(result)
        except aioredis.ResponseError:
            result = await self.client.set(lock_key, token, nx=True, ex=self._lock_ttl)
            return bool(result)

    async def unlock_ready_dir(self, ready_dir: str) -> None:
        lock_key = (
            self._legacy_k(f"pack_lock:{ready_dir}")
            if self._key_style == "legacy_meta"
            else self._k(f"pack_lock:{ready_dir}")
        )
        await self.client.delete(lock_key)

    async def clear_state(self) -> None:
        if self._key_style == "legacy_meta":
            pattern = self._legacy_k("*")
            cursor = 0
            while True:
                cursor, keys = await self.client.scan(cursor, match=pattern, count=100)
                keys = [
                    key for key in keys
                    if key not in {self._legacy_init_lock_key, self._legacy_finalize_lock_key}
                ]
                if keys:
                    await self.client.delete(*keys)
                if cursor == 0:
                    break
            logger.info("Redis legacy meta state cleared for prefix=%s", self._legacy_prefix)
            return
        pattern = self._k("*")
        init_lock_key = self._k("lock:init")
        finalize_lock_key = self._k("lock:finalize")
        cursor = 0
        while True:
            cursor, keys = await self.client.scan(cursor, match=pattern, count=100)
            keys = [key for key in keys if key not in {init_lock_key, finalize_lock_key}]
            if keys:
                await self.client.delete(*keys)
            if cursor == 0:
                break
        logger.info("Redis state cleared for prefix=%s", self._prefix)
