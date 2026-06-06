"""
抖音爬虫使用 file-uploader SDK 的完整示例。

演示如何将原 main_result_writer 逻辑迁移到通用 SDK。
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from file_uploader import (
    FileWriterConfig,
    FileWriterPipeline,
    OssProvider,
    OssUploader,
    RabbitMQSource,
    RedisStateStore,
    get_meta_writer_process_count,
    run_pipeline_supervised,
    tar_zstd_packer,
)

# ---------------------------------------------------------------------------
# 业务策略函数：可替换为具体业务逻辑
# ---------------------------------------------------------------------------


def douyin_message_parser(raw_body: bytes) -> dict:
    """解析 RabbitMQ 消息体为结构化数据。"""
    print(f'raw_body -->: {raw_body[:200]}')
    return json.loads(raw_body.decode("utf-8"))


def douyin_file_name_generator(data: dict) -> str:
    """根据数据内容生成文件名。

    例如：6630619144305773828.json
    """
    aweme_id = data.get("aweme_id", "unknown")
    print(f"aweme_id -->: {aweme_id}.json")
    return f"{aweme_id}.json"


def douyin_remote_key_generator(archive_name: str) -> str:
    """根据归档文件名生成远端 COS key。

    例如：douyin/aweme/20250126/slot_0_5.tar.zst
    """
    # 按日期分桶
    date_str = datetime.now().strftime("%Y%m%d")
    print(f"按日期分桶 -->: douyin/aweme/{date_str}/{archive_name}")
    return f"douyin/aweme/{date_str}/{archive_name}"


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------


TASK_NAME = "douyin_aweme"


def build_pipeline() -> FileWriterPipeline:
    process_count = get_meta_writer_process_count()

    # 1. 配置
    config = FileWriterConfig.legacy_meta(
        task_name=TASK_NAME,
        process_count=process_count,  # 读取 META_WRITER_PROCESS_COUNT
        pack_threshold=30,        # 每个目录 1000 个文件后封口
        storage_root=Path("/path/file_uploader/logs"),
        save_timeout=5.0,           # 写入超时 5s
        packer_concurrency=4,       # 4 个并发打包 worker
        packer_interval=2.0,        # 队列为空时轮询间隔 2s
        packer_max_retries=3,       # 打包失败最多重试 3 次
    )

    # 2. Redis 状态存储
    state_store = RedisStateStore(
        host="localhost",
        port=6379,
        db=1,
        password="",
        key_style="legacy_meta",
        task_name=TASK_NAME,
        node_id=config.node_id,
        slot_count=config.slot_count,
        lock_ttl=300,
    )

    # 3. RabbitMQ 消息源
    message_source = RabbitMQSource(
        host="localhost",
        port=5672,
        vhost="/",
        user="",
        password="",
        queue_name="douyin_aweme:metas:buffer",
        prefetch_count=50,
    )

    # 4. 对象存储上传器（腾讯云 COS）
    object_store = OssUploader(
        provider=OssProvider.COS,
        # 存储桶名称
        bucket="",
        # 地域
        region="",
        # 地域 Endpoint（不含 bucket 前缀）
        endpoint="",
        # 腾讯云 COS（兼容 TOS）的 Access Key / Secret Key
        access_key_id="",
        access_key_secret="",
    )

    # 5. 构建 Pipeline（链式 Builder）
    return (
        FileWriterPipeline
        .with_config(config)
        .with_state_store(state_store)
        .with_message_source(message_source)
        .with_object_store(object_store)
        .with_message_parser(douyin_message_parser)
        .with_file_name_generator(douyin_file_name_generator)
        .with_packer(tar_zstd_packer)
        .with_remote_key_generator(douyin_remote_key_generator)
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    run_pipeline_supervised(build_pipeline)


if __name__ == "__main__":
    main()
