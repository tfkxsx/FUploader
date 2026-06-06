"""消息源抽象接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable


class MessageSource(ABC):
    """消息源抽象接口。

    用于替代 RabbitMQ 等具体消息队列的硬绑定。
    实现者需提供消费、确认与拒绝等方法。
    """

    @abstractmethod
    async def start_consume(self, on_message: Callable[[bytes], Awaitable[None]]) -> None:
        """启动消费，每收到一条消息调用 on_message(raw_body)。

        Args:
            on_message: 异步回调，参数为消息原始字节。
        """
        ...

    @abstractmethod
    async def ack(self, tag: Any) -> None:
        """确认消息已处理完毕。

        Args:
            tag: 来自 MessageSource 内部的消息标识。
        """
        ...

    @abstractmethod
    async def nack(self, tag: Any, requeue: bool = True) -> None:
        """拒绝消息。

        Args:
            tag: 来自 MessageSource 内部的消息标识。
            requeue: True 表示允许重回队列，False 表示丢弃/死信。
        """
        ...

    @abstractmethod
    async def stop_consume(self) -> None:
        """停止消费，结束 start_consume 中的循环。"""
        ...

    @abstractmethod
    async def set_prefetch_count(self, count: int) -> None:
        """动态设置预取数。

        Args:
            count: 期望的 prefetch 数量。
        """
        ...