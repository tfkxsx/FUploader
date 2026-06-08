"""SDK 配置模块。"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from .runtime import (
    get_meta_writer_max_tasks_per_child,
    get_meta_writer_process_count,
    get_meta_writer_rabbitmq_prefetch_count,
    get_node_id,
    worker_index_from_node_id,
)


@dataclass(frozen=True)
class FileWriterConfig:
    """文件写入管线 SDK 配置。

    Attributes:
        storage_root: 本地存储根目录。
        slot_count: 分片数（多进程写文件时避免文件锁竞争）。
        pack_threshold: 每个目录最多包含多少文件后触发封口(seal)。
        write_concurrency: 写文件 worker 协程数。
        packer_concurrency: 打包上传 worker 协程数。
        save_timeout: 单文件写入超时（秒）。
        packer_interval: 打包轮询间隔（秒）。
        packer_max_retries: 打包失败最大重试次数（0=不重试，直接丢弃）。
        batch_size: 批量 ack 阈值。
        flush_interval: 时间窗口 flush（秒）。
        prefetch_count: 单个 writer 进程的消息预取数。
        meta_writer_process_count: writer 进程数；与 prefetch_count 的乘积作为本地写队列容量。
        meta_writer_max_tasks_per_child: 单个 writer 子进程成功处理多少条消息后优雅轮转（0=禁用）。
        node_id: 当前节点 ID，默认 node_id-{本机IP}，子进程中由 supervisor 注入 -meta-pN。
        task_name: legacy Redis key 命名使用的任务名。
        worker_index: 当前 worker 下标；legacy worker_index slot 策略默认从 node_id 解析。
        slot_strategy: "hash" 按文件名分 slot；"worker_index" 让每个子进程独占 slot。
        storage_layout: "flat_slot" 使用 slot_0_0；"legacy_meta" 使用 active/slot-0/dir-000001。
        ready_dir_format: "colon" 使用 0:dir；"legacy_slot" 使用 slot-0:dir。
        resume_orphan_archives: 启动时是否恢复孤儿归档文件。
    """

    storage_root: Path
    slot_count: int = 1
    pack_threshold: int = 1000
    write_concurrency: int = 10
    packer_concurrency: int = 2
    save_timeout: float = 30.0
    packer_interval: float = 1.0
    packer_max_retries: int = 3
    batch_size: int = 100
    flush_interval: float = 5.0
    prefetch_count: int = field(default_factory=get_meta_writer_rabbitmq_prefetch_count)
    meta_writer_process_count: int = field(default_factory=get_meta_writer_process_count)
    meta_writer_max_tasks_per_child: int = 1000
    node_id: str = ""
    task_name: str = ""
    worker_index: int | None = None
    slot_strategy: Literal["hash", "worker_index"] = "hash"
    storage_layout: Literal["flat_slot", "legacy_meta"] = "flat_slot"
    ready_dir_format: Literal["colon", "legacy_slot"] = "colon"
    resume_orphan_archives: bool = True

    def __post_init__(self) -> None:
        if not self.node_id:
            object.__setattr__(self, "node_id", get_node_id())
        if self.worker_index is None:
            object.__setattr__(self, "worker_index", worker_index_from_node_id(self.node_id))
        if self.slot_count < 1:
            raise ValueError("slot_count must be >= 1")
        if self.pack_threshold < 1:
            raise ValueError("pack_threshold must be >= 1")
        if self.write_concurrency < 1:
            raise ValueError("write_concurrency must be >= 1")
        if self.packer_concurrency < 1:
            raise ValueError("packer_concurrency must be >= 1")
        if self.save_timeout <= 0:
            raise ValueError("save_timeout must be > 0")
        if self.packer_interval <= 0:
            raise ValueError("packer_interval must be > 0")
        if self.packer_max_retries < 0:
            raise ValueError("packer_max_retries must be >= 0")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.flush_interval <= 0:
            raise ValueError("flush_interval must be > 0")
        if self.prefetch_count < 1:
            raise ValueError("prefetch_count must be >= 1")
        if self.meta_writer_process_count < 1:
            raise ValueError("meta_writer_process_count must be >= 1")
        if self.meta_writer_max_tasks_per_child < 0:
            raise ValueError("meta_writer_max_tasks_per_child must be >= 0")
        if self.worker_index is not None and self.worker_index < 0:
            raise ValueError("worker_index must be >= 0")
        if self.slot_strategy not in ("hash", "worker_index"):
            raise ValueError("slot_strategy must be 'hash' or 'worker_index'")
        if self.storage_layout not in ("flat_slot", "legacy_meta"):
            raise ValueError("storage_layout must be 'flat_slot' or 'legacy_meta'")
        if self.ready_dir_format not in ("colon", "legacy_slot"):
            raise ValueError("ready_dir_format must be 'colon' or 'legacy_slot'")
        if self.slot_strategy == "worker_index" and self.worker_index is None:
            raise ValueError("worker_index is required when slot_strategy='worker_index'")
        if self.worker_index is not None and self.worker_index >= self.slot_count:
            raise ValueError("worker_index must be < slot_count")

    @classmethod
    def legacy_meta(
        cls,
        *,
        storage_root: Path,
        task_name: str,
        process_count: int | None = None,
        **kwargs: object,
    ) -> "FileWriterConfig":
        """Create config aligned with the verified main_meta_writer.py layout."""
        resolved_process_count = get_meta_writer_process_count() if process_count is None else max(1, process_count)
        kwargs.setdefault("prefetch_count", get_meta_writer_rabbitmq_prefetch_count())
        kwargs.setdefault("meta_writer_process_count", resolved_process_count)
        kwargs.setdefault("meta_writer_max_tasks_per_child", get_meta_writer_max_tasks_per_child())
        return cls(
            storage_root=storage_root,
            task_name=task_name,
            slot_count=resolved_process_count,
            slot_strategy="worker_index",
            storage_layout="legacy_meta",
            ready_dir_format="legacy_slot",
            **kwargs,
        )
