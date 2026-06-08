"""
file-uploader: 通用的「写文件 → 打包 → 上传 OSS」SDK。

快速开始:
    pipeline = (
        FileWriterPipeline
        .with_config(config)
        ...
        .with_remote_key_generator(my_key_gen)
    )
    await pipeline.start()
    await pipeline.wait()
    await pipeline.stop()
"""

from .adapters import OssProvider, OssUploader, RabbitMQSource, RedisStateStore
from .config import FileWriterConfig
from .interfaces.object_store import ObjectStore
from .interfaces.message_source import MessageSource
from .interfaces.state_store import SlotRuntimeState, StateStore
from .interfaces.strategies import (
    FileNameGenerator,
    MessageParser,
    Packer,
    RemoteKeyGenerator,
)
from .messages import (
    DecodedFilePayload,
    FileMessage,
    FileMessagePublisher,
    encode_file_message,
    file_message_name,
    parse_file_message,
    publish_file_messages,
    serialize_file_payload,
)
from .packers import tar_zstd_packer
from .pipeline import FileWriterPipeline, run_pipeline_once, run_pipeline_supervised
from .recovery import ResidualRecovery
from .runtime import (
    get_meta_writer_max_tasks_per_child,
    get_meta_writer_process_count,
    get_meta_writer_rabbitmq_prefetch_count,
    get_node_id,
)
from .services import MetaPacker, MetaWriter

__all__ = [
    # -- 配置 --
    "FileWriterConfig",
    # -- 接口 --
    "ObjectStore",
    "MessageSource",
    "StateStore",
    "SlotRuntimeState",
    # -- 策略类型 --
    "MessageParser",
    "FileNameGenerator",
    "Packer",
    "RemoteKeyGenerator",
    # -- 文件消息 --
    "FileMessage",
    "DecodedFilePayload",
    "FileMessagePublisher",
    "encode_file_message",
    "parse_file_message",
    "file_message_name",
    "serialize_file_payload",
    "publish_file_messages",
    # -- 适配器 --
    "OssProvider",
    "OssUploader",
    "RabbitMQSource",
    "RedisStateStore",
    # -- 打包器 --
    "tar_zstd_packer",
    # -- 服务 --
    "MetaWriter",
    "MetaPacker",
    # -- 恢复 --
    "ResidualRecovery",
    # -- Pipeline --
    "FileWriterPipeline",
    "run_pipeline_once",
    "run_pipeline_supervised",
    "get_meta_writer_max_tasks_per_child",
    "get_meta_writer_process_count",
    "get_meta_writer_rabbitmq_prefetch_count",
    "get_node_id",
]
