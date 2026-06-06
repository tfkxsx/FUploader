"""状态存储抽象接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class SlotRuntimeState:
    """单个 slot 的运行时状态。

    Attributes:
        slot: slot 编号。
        active_dir_id: 当前写入目录标识。
        active_count: 当前目录已累计文件数。
        dir_seq: 目录序号（自增）。
    """

    slot: int
    active_dir_id: str
    active_count: int
    dir_seq: int


class StateStore(ABC):
    """状态存储抽象接口。

    用于替代 Redis 的运行时状态管理。
    实现者需保证操作原子性（如 Redis 可通过 Lua 脚本实现）。
    """

    # -- 初始化锁（防止多进程竞态） --

    @abstractmethod
    async def try_acquire_init_lock(self, token: str) -> bool:
        """尝试获取初始化锁。

        Args:
            token: 唯一标识本次启动的 token。

        Returns:
            True 表示获取成功（获得初始化权）。
        """
        ...

    @abstractmethod
    async def release_init_lock(self) -> None:
        """释放初始化锁。"""
        ...

    @abstractmethod
    async def is_init_locked(self) -> bool:
        """检查初始化锁是否存在。"""
        ...

    # -- 运行时状态标记 --

    @abstractmethod
    async def has_runtime_state(self) -> bool:
        """检查是否存在运行时状态（用于判断是否残留）。"""
        ...

    @abstractmethod
    async def initialize_runtime_state(self, slots: list, ready_dirs: list) -> None:
        """写入初始运行时状态。

        Args:
            slots: SlotRuntimeState 列表。
            ready_dirs: 格式 "slot:dir_id"。
        """
        ...

    # -- 写入时状态管理 --

    @abstractmethod
    async def get_slot_state(self, slot: int) -> Optional[SlotRuntimeState]:
        """获取指定 slot 的状态。

        Args:
            slot: slot 编号。

        Returns:
            SlotRuntimeState 或 None。
        """
        ...

    @abstractmethod
    async def reset_slot_state(self, slot: int, *, active_count: int, dir_seq: int) -> None:
        """重置指定 slot 的运行时状态（启动残留恢复用）。

        将 slot 的 active_dir_id 设为 slot_{slot}_{max(dir_seq-1, 0)}，
        active_count 和 dir_seq 设为指定值。

        Args:
            slot: slot 编号。
            active_count: 重置后的活跃文件数。
            dir_seq: 重置后的目录序号。
        """
        ...

    @abstractmethod
    async def switch_slot_dir(self, slot: int, next_dir_id: str) -> None:
        """切换 slot 的活动目录（封口时调用）。

        Args:
            slot: slot 编号。
            next_dir_id: 新的活动目录标识。
        """
        ...

    @abstractmethod
    async def increment_slot_count(self, slot: int, delta: int) -> int:
        """原子增加 slot 的计数并返回新值。

        Args:
            slot: slot 编号。
            delta: 增量（通常为 1）。

        Returns:
            增加后的活跃文件总数。
        """
        ...

    @abstractmethod
    async def set_slot_active_count(self, slot: int, active_count: int) -> None:
        """设置指定 slot 的活动目录计数。

        Args:
            slot: slot 编号。
            active_count: 活动目录中的实际文件数。
        """
        ...

    # -- 待打包队列 --

    @abstractmethod
    async def enqueue_ready_dir(self, ready_dir: str) -> None:
        """将目录加入待打包队列。

        Args:
            ready_dir: 格式 "slot:dir_id"。
        """
        ...

    @abstractmethod
    async def dequeue_ready_dir(self) -> Optional[str]:
        """从待打包队列取出一个目录。

        Returns:
            格式 "slot:dir_id" 或 None（队列为空）。
        """
        ...

    @abstractmethod
    async def lock_ready_dir(self, ready_dir: str, token: str) -> bool:
        """尝试锁定待打包目录（防止多 packer 抢同一目录）。

        Args:
            ready_dir: 格式 "slot:dir_id"。
            token: 当前 packer 的唯一标识。

        Returns:
            True 表示锁定成功。
        """
        ...

    @abstractmethod
    async def unlock_ready_dir(self, ready_dir: str) -> None:
        """释放待打包目录锁。

        Args:
            ready_dir: 格式 "slot:dir_id"。
        """
        ...

    @abstractmethod
    async def clear_state(self) -> None:
        """清空所有运行时状态（正常退出时调用）。"""
        ...
