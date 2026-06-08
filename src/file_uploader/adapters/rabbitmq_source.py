"""RabbitMQ 消息源适配器。"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

import aio_pika

from ..interfaces.message_source import MessageSource

logger = logging.getLogger(__name__)


class RabbitMQSource(MessageSource):
    """基于 aio-pika 的 RabbitMQ 消息源实现。

    用法::

        source = RabbitMQSource(
            host="localhost", port=5672, vhost="/",
            user="guest", password="guest",
            queue_name="meta_writer_queue",
            prefetch_count=50,
        )
        await source.start_consume(handler)
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5672,
        vhost: str = "/",
        user: str = "guest",
        password: str = "guest",
        queue_name: str,
        prefetch_count: int = 50,
        requeue_on_nack: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._vhost = vhost
        self._user = user
        self._password = password
        self._queue_name = queue_name
        self._prefetch_count = prefetch_count
        self._requeue_on_nack = requeue_on_nack

        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.RobustChannel] = None
        self._queue: Optional[aio_pika.RobustQueue] = None
        self._consumer_tag: Optional[str] = None
        self._consume_task: Optional[asyncio.Task] = None
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._inflight_messages = 0
        self._idle_event = asyncio.Event()
        self._idle_event.set()

    # ------------------------------------------------------------------
    # MessageSource 接口
    # ------------------------------------------------------------------

    async def start_consume(self, on_message: Callable[[bytes], Awaitable[None]]) -> None:
        logger.info("Connecting to RabbitMQ %s:%s/%s", self._host, self._port, self._vhost)
        self._connection = await aio_pika.connect_robust(
            host=self._host,
            port=self._port,
            virtualhost=self._vhost,
            login=self._user,
            password=self._password,
        )
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch_count)
        self._queue = await self._channel.declare_queue(self._queue_name, durable=True)

        async def _callback(message: aio_pika.IncomingMessage) -> None:
            self._inflight_messages += 1
            self._idle_event.clear()
            try:
                await on_message(message.body)
            except Exception:
                logger.exception("on_message callback raised, message will be nack'd")
                await message.nack(requeue=self._requeue_on_nack)
            else:
                await message.ack()
            finally:
                self._inflight_messages -= 1
                if self._inflight_messages <= 0:
                    self._idle_event.set()

        listener = self._queue.iterator()
        self._consume_task = asyncio.create_task(self._run_consume(listener, _callback))
        logger.info("RabbitMQ consumer started on queue=%s", self._queue_name)

    async def _run_consume(
        self,
        listener: aio_pika.QueueIterator,
        callback: Callable[[aio_pika.IncomingMessage], Awaitable[None]],
    ) -> None:
        """内部消费循环。"""
        try:
            async for message in listener:
                task = asyncio.create_task(callback(message))
                self._callback_tasks.add(task)
                task.add_done_callback(self._callback_tasks.discard)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Consumer loop terminated unexpectedly")

    async def ack(self, tag: Any) -> None:
        # ack/nack 已在 _callback 内部手动完成，
        # 该方法是接口保留，供批量 ack 时外部使用。
        pass

    async def nack(self, tag: Any, requeue: bool = True) -> None:
        # 同上，保留接口。
        pass

    async def stop_consume(self) -> None:
        logger.info("Stopping RabbitMQ consumer...")
        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None
        if self._callback_tasks:
            await asyncio.gather(*tuple(self._callback_tasks), return_exceptions=True)
        await self._idle_event.wait()
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        logger.info("RabbitMQ consumer stopped")

    async def set_prefetch_count(self, count: int) -> None:
        self._prefetch_count = count
        if self._channel and not self._channel.is_closed:
            await self._channel.set_qos(prefetch_count=count)
