# FUploader

通用的「消费消息 → 写本地文件 → 打包归档 → 上传 OSS」管线 SDK。

## 核心流程

```
 MessageSource          MetaWriter                  MetaPacker
 ────────────         ──────────────            ─────────────────
 RabbitMQ ──► 解析消息 ──► 写盘到 slot ──► 封口 ──► 打包 .tar.zst ──► 上传 OSS ──► 清理本地
                          │
                          ▼
                    Redis (StateStore)
                    · slot 计数 / 封口阈值
                    · 待打包队列
                    · 分布式锁
```

## 架构设计

### 三层抽象接口

SDK 通过抽象接口解耦外部依赖，方便替换不同中间件：

| 接口 | 职责 | 内置实现 |
|------|------|----------|
| `MessageSource` | 消息消费、ACK/NACK | `RabbitMQSource` (aio-pika) |
| `StateStore` | slot 状态、队列、分布式锁 | `RedisStateStore` (redis-py async) |
| `ObjectStore` | 文件上传、存在性检查 | `OssUploader` (COS / 阿里云 OSS / S3) |

### 四种策略注入

业务逻辑通过 Callable 注入，无需继承任何类：

| 策略 | 签名 | 作用 |
|------|------|------|
| `MessageParser` | `(bytes) -> dict` | 将原始消息字节解析为结构化数据 |
| `FileNameGenerator` | `(dict) -> str` | 根据数据生成写入文件名 |
| `Packer` | `(Path) -> Path \| None` | 将目录打包为归档文件 |
| `RemoteKeyGenerator` | `(str) -> str` | 根据归档文件名生成远端 OSS key |

### Slot 分片机制

- 按文件名 hash 取模分配到不同 slot 目录（`slot_0_0`、`slot_1_0` ...）
- 每个 slot 有独立的 `active_dir_id` 和 `active_count`
- 支持多进程并行写入同一存储目录，互不冲突

### Legacy 高并发模式

迁移原 `main_meta_writer.py` 场景时，推荐使用 legacy 模式：

- 主进程作为 supervisor，子进程作为 worker 执行业务。
- `META_WRITER_PROCESS_COUNT` 控制 worker 进程数。
- 子进程 `NODE_ID` 形如 `node_id-{本机IP}-meta-p0`、`node_id-{本机IP}-meta-p1`。
- 每个 worker 固定写自己的 slot，避免共享写锁竞争。
- Redis key 对齐 `{task_name}:meta:{node_id-{本机IP}}:*`。
- 本地目录对齐 `active/slot-0/dir-000001` 与 `sealed/slot-0/dir-000001`。

### 封口（Seal）机制

- 每个 slot 目录内文件数达到 `pack_threshold` 时自动触发封口
- 当前目录被标记为 ready，加入打包队列；同时创建新序号目录（如 `slot_0_0` → `slot_0_1`）
- 阈值检查通过 Redis Lua 脚本保证原子性

### 残留恢复

启动时自动扫描 `storage_root`，找出非当前活跃的 `slot_*` 目录，重新加入待打包队列，防止进程崩溃后数据丢失。

## 安装

```bash
# 基础安装
pip install FUploader

# 按需安装 OSS 后端
pip install "FUploader[cos]"      # 腾讯云 COS
pip install "FUploader[oss]"      # 阿里云 OSS
pip install "FUploader[s3]"       # AWS S3 / MinIO
pip install "FUploader[adapters]" # 全部 OSS 后端
```

以下示例展示抖音作品（aweme）元数据写入管线的完整用法 — 上游通过 `FileMessage` 编码投递作品数据到 RabbitMQ，下游消费并按 aweme_id 落盘、打包、上传 COS：

```python
import asyncio
from pathlib import Path

from file_uploader import (
    FileWriterConfig,
    FileWriterPipeline,
    OssProvider,
    OssUploader,
    RabbitMQSource,
    RedisStateStore,
    get_meta_writer_process_count,
    parse_file_message,
    tar_zstd_packer,
)


# 1. 业务策略函数
def douyin_message_parser(raw_body: bytes) -> dict:
    """解析上游通过 FileMessage 编码的消息（gzip + base64）。"""
    decoded = parse_file_message(raw_body)
    return {"aweme_id": decoded.file_name, **decoded.payload}


def douyin_file_name_generator(data: dict) -> str:
    """用作品 aweme_id 作为文件名。"""
    aweme_id = data.get("aweme_id", "unknown")
    return f"{aweme_id}.json"


def douyin_remote_key_generator(archive_name: str) -> str:
    """按日期分目录上传到 COS。"""
    from datetime import datetime
    date_str = datetime.now().strftime("%Y%m%d")
    return f"douyin/aweme/{date_str}/{archive_name}"


TASK_NAME = "douyin_aweme"


def build_pipeline() -> FileWriterPipeline:
    process_count = get_meta_writer_process_count()  # 读取 META_WRITER_PROCESS_COUNT，默认 1

    # 2. 配置
    config = FileWriterConfig.legacy_meta(
        task_name=TASK_NAME,
        process_count=process_count,
        pack_threshold=1000,
        storage_root=Path("/data/douyin_output"),
        save_timeout=5.0,
        packer_concurrency=4,
        packer_interval=2.0,
        packer_max_retries=3,
    )

    # 3. Redis 状态存储
    state_store = RedisStateStore(
        host="localhost", port=6379, db=0,
        key_style="legacy_meta",
        task_name=TASK_NAME,
        node_id=config.node_id,
        slot_count=config.slot_count,
    )

    # 4. RabbitMQ 消息源
    message_source = RabbitMQSource(
        host="localhost", port=5672,
        user="guest", password="guest",
        queue_name="douyin_aweme_meta_queue",
    )

    # 5. OSS 上传器（腾讯云 COS）
    object_store = OssUploader(
        provider=OssProvider.COS,
        bucket="douyin-data-1250000000",
        region="ap-guangzhou",
        endpoint="cos.ap-guangzhou.myqcloud.com",
        access_key_id="AKID...",
        access_key_secret="...",
    )

    # 6. 构建 Pipeline
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


async def async_main() -> None:
    pipeline = build_pipeline()
    await pipeline.start()
    try:
        await pipeline.wait()
    finally:
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(async_main())
```

## 配置参考

`FileWriterConfig` 所有字段：

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `storage_root` | `Path` | **必填** | 本地存储根目录 |
| `slot_count` | `int` | `1` | 分片数（多进程写文件时避免文件锁竞争） |
| `pack_threshold` | `int` | `1000` | 每个目录最多文件数，触发封口（seal） |
| `write_concurrency` | `int` | `50` | 写文件 worker 协程数 |
| `packer_concurrency` | `int` | `2` | 打包上传 worker 协程数 |
| `save_timeout` | `float` | `30.0` | 单文件写入超时（秒） |
| `packer_interval` | `float` | `1.0` | 打包轮询间隔（秒） |
| `packer_max_retries` | `int` | `3` | 打包失败最大重试次数（0=不重试，直接丢弃） |
| `batch_size` | `int` | `100` | 批量 ACK 阈值 |
| `flush_interval` | `float` | `5.0` | 时间窗口 flush（秒） |
| `prefetch_count` | `int` | `50` | 消息预取数 |
| `meta_writer_max_tasks_per_child` | `int` | `1000` | 单个 worker 成功处理多少条后优雅轮转（0=禁用） |
| `node_id` | `str` | `NODE_ID` 或 `node_id-{本机IP}` | 当前节点 ID |
| `task_name` | `str` | `""` | legacy Redis key 使用的任务名 |
| `worker_index` | `int \| None` | 从 `NODE_ID` 解析 | 当前 worker 下标 |
| `slot_strategy` | `str` | `"hash"` | `hash` 或 `worker_index` |
| `storage_layout` | `str` | `"flat_slot"` | `flat_slot` 或 `legacy_meta` |
| `ready_dir_format` | `str` | `"colon"` | `colon` 或 `legacy_slot` |
| `resume_orphan_archives` | `bool` | `True` | 启动时是否恢复孤儿目录 |

环境变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `META_WRITER_PROCESS_COUNT` | `1` | supervisor 启动的 worker 进程数，同时也是 legacy 模式 slot 数 |
| `META_WRITER_MAX_TASKS_PER_CHILD` | `1000` | 可映射到 `meta_writer_max_tasks_per_child`，达到后 worker 优雅退出并由 supervisor 拉起 |

## 核心组件

### FileWriterPipeline

Builder 模式的一站式入口，封装完整的生命周期管理：

| 方法 | 说明 |
|------|------|
| `.with_config(config)` | 注入 `FileWriterConfig` 配置 |
| `.with_state_store(store)` | 注入 `StateStore` 实现 |
| `.with_message_source(source)` | 注入 `MessageSource` 实现 |
| `.with_object_store(store)` | 注入 `ObjectStore` 实现 |
| `.with_message_parser(parser)` | 注入消息解析策略 |
| `.with_file_name_generator(gen)` | 注入文件名生成策略 |
| `.with_packer(packer)` | 注入打包策略 |
| `.with_remote_key_generator(gen)` | 注入远端 key 生成策略 |
| `.no_recovery()` | 禁用启动时残留恢复 |
| `await pipeline.start()` | 启动 Pipeline |
| `await pipeline.wait()` | 阻塞直到收到 SIGINT / SIGTERM |
| `await pipeline.stop()` | 优雅关闭 |

### MetaWriter

文件写入服务，负责：

1. 通过 `MessageSource` 消费消息
2. 调用 `MessageParser` 解析为结构化数据
3. 调用 `FileNameGenerator` 生成文件名
4. 按文件名 hash 取模路由到对应 slot 目录
5. 将数据序列化为 JSON 写入磁盘
6. 原子递增 slot 计数并检查封口阈值
7. 达到阈值时封口（seal），将目录加入打包队列

### MetaPacker

打包上传服务，负责：

1. 轮询待打包队列
2. 通过分布式锁防止重复打包
3. 调用 `Packer` 将目录打包为归档文件
4. 调用 `RemoteKeyGenerator` 生成远端 key
5. 通过 `ObjectStore` 上传到 OSS
6. 上传成功后清理本地归档和源目录
7. 打包失败时指数退避重试，全部失败后丢弃

### ResidualRecovery

启动期间执行残留恢复：

- 扫描 `storage_root` 下所有 `slot_*` 目录
- 排除当前活跃目录（`active_dir_id`）
- 跳过空目录
- 将孤儿目录重新加入待打包队列

## 自定义策略

所有策略都是普通函数（支持同步/异步），无需继承框架基类：

```python
# 自定义消息解析器（异步版本）
async def my_parser(raw_body: bytes) -> dict:
    data = json.loads(raw_body)
    # 数据清洗、补全...
    return data

# 自定义打包器（可替换 tar.zstd）
async def my_packer(source_dir: Path) -> Path | None:
    """7z 压缩示例"""
    archive = source_dir.with_suffix(".7z")
    result = await run_7z(source_dir, archive)
    return archive if result else None

# 注入到 Pipeline
pipeline = (
    FileWriterPipeline
    .with_config(config)
    # ...其他配置...
    .with_message_parser(my_parser)
    .with_packer(my_packer)
)
```

## 适配器

### RabbitMQSource

基于 `aio-pika` 的 RabbitMQ 消费实现：

```python
source = RabbitMQSource(
    host="localhost", port=5672,
    vhost="/", user="guest", password="guest",
    queue_name="my_queue",
    prefetch_count=50,
    requeue_on_nack=True,
)
```

### RedisStateStore

基于 `redis.asyncio` 的状态存储实现。使用 Lua 脚本保证阈值检查和分布式锁的原子性：

```python
store = RedisStateStore(
    host="localhost", port=6379, db=0,
    password="",
    key_prefix="file_uploader:task1:",
    lock_ttl=300,
)
await store.connect()
```

### OssUploader

支持三种 OSS 后端的统一上传接口，各后端依赖按需安装：

```python
# 腾讯云 COS → pip install "FUploader[cos]"
uploader = OssUploader(
    provider=OssProvider.COS,
    bucket="my-bucket-1250000000",
    region="ap-guangzhou",
    endpoint="cos.ap-guangzhou.myqcloud.com",
    access_key_id="AKID...",
    access_key_secret="...",
)

# 阿里云 OSS → pip install "FUploader[oss]"
uploader = OssUploader(
    provider=OssProvider.ALIYUN_OSS,
    bucket="my-bucket",
    endpoint="oss-cn-hangzhou.aliyuncs.com",
    access_key_id="LTAI...",
    access_key_secret="...",
)

# AWS S3 / MinIO → pip install "FUploader[s3]"
uploader = OssUploader(
    provider=OssProvider.S3,
    bucket="my-bucket",
    region="us-east-1",
    endpoint="https://s3.amazonaws.com",
    access_key_id="AKIA...",
    access_key_secret="...",
)
```

## 文件消息编码

SDK 内置一套「写入前处理」的消息编码格式，支持将任意 payload 经 gzip 压缩 + base64 编码后投递到消息队列，由 `MetaWriter` 消费时自动解码。适用于需要在上游提前准备消息内容的场景。

### 编码（生产者侧）

```python
from file_uploader import (
    FileMessage,
    FileMessagePublisher,
    encode_file_message,
    publish_file_messages,
)

# 方式 1：直接编码单条消息
envelope = encode_file_message(FileMessage(file_name="data_001", payload={"key": "value"}))
# → {"file_name": "data_001.json", "compression": "gzip", "content_type": "json", "payload": "..."}

# 方式 2：批量发布到消息队列
async def my_publish(body: str) -> None:
    await channel.default_exchange.publish(aio_pika.Message(body.encode()), routing_key="queue")

await publish_file_messages(
    [FileMessage(file_name="a", payload={"x": 1}), FileMessage(file_name="b", payload="hello")],
    publish=my_publish,
)
```

### 解码（消费者侧）

```python
from file_uploader import parse_file_message, file_message_name

decoded = parse_file_message(raw_bytes)   # → DecodedFilePayload(file_name="data_001.json", payload={...}, content_type="json")
file_name = file_message_name(decoded)    # 等价于 decoded.file_name
```

### 作为 MessageParser 注入

```python
from file_uploader import parse_file_message

def parser(raw_body: bytes) -> dict:
    decoded = parse_file_message(raw_body)
    return {"file_name": decoded.file_name, "payload": decoded.payload}
```

## 独立使用核心服务

`MetaWriter` 和 `MetaPacker` 均可脱离 Pipeline 独立使用，直接构造并调用 `start()` / `stop()`：

```python
writer = MetaWriter(
    config=config,
    state_store=store,
    message_source=source,
    message_parser=parser,
    file_name_generator=name_gen,
)
await writer.start()
# ... 消费消息 ...
await writer.stop()
```

## 项目结构

```
src/file_uploader/
├── __init__.py              # 公开 API 导出
├── config.py                # FileWriterConfig 配置类
├── layout.py                # 存储目录布局辅助函数
├── messages.py              # 文件消息编码/解码 (FileMessage, parse_file_message, etc.)
├── pipeline.py              # FileWriterPipeline 入口 + Builder + run_pipeline_supervised
├── runtime.py               # 运行时环境变量读取 (NODE_ID, META_WRITER_PROCESS_COUNT, etc.)
├── interfaces/              # 抽象接口层
│   ├── message_source.py    #   MessageSource 抽象
│   ├── object_store.py      #   ObjectStore 抽象
│   ├── state_store.py       #   StateStore 抽象 + SlotRuntimeState
│   └── strategies.py        #   MessageParser / FileNameGenerator / Packer / RemoteKeyGenerator 类型
├── adapters/                # 适配器实现
│   ├── rabbitmq_source.py   #   RabbitMQSource
│   ├── redis_state_store.py #   RedisStateStore
│   └── oss_uploader.py      #   OssUploader (COS / OSS / S3)
├── packers/                 # 打包策略
│   └── tar_zstd.py          #   tar + zstd 打包器
├── services/                # 核心服务
│   ├── meta_writer.py       #   MetaWriter 写入服务
│   └── meta_packer.py       #   MetaPacker 打包上传服务
└── recovery/                # 残留恢复
    └── residual.py          #   ResidualRecovery
```

## 公开 API 总览

| 类别 | 名称 | 说明 |
|------|------|------|
| 配置 | `FileWriterConfig` | Pipeline 配置 dataclass |
| 接口 | `MessageSource` | 消息源抽象基类 |
| 接口 | `StateStore` | 状态存储抽象基类 |
| 接口 | `ObjectStore` | 对象存储抽象基类 |
| 接口 | `SlotRuntimeState` | slot 运行时状态 dataclass |
| 策略类型 | `MessageParser` | `(bytes) -> dict`（支持同步/异步） |
| 策略类型 | `FileNameGenerator` | `(dict) -> str` |
| 策略类型 | `Packer` | `(Path) -> Path \| None` |
| 策略类型 | `RemoteKeyGenerator` | `(str) -> str` |
| 适配器 | `RabbitMQSource` | RabbitMQ 消费实现 |
| 适配器 | `RedisStateStore` | Redis 状态存储实现 |
| 适配器 | `OssUploader` | 多后端 OSS 上传器 |
| 适配器 | `OssProvider` | OSS 后端枚举 (COS / ALIYUN_OSS / S3) |
| 打包器 | `tar_zstd_packer` | tar.zstd 打包函数 |
| 消息编码 | `FileMessage` | 原始文件消息 dataclass |
| 消息编码 | `DecodedFilePayload` | 解码后的文件载荷 dataclass |
| 消息编码 | `FileMessagePublisher` | 批量消息发布器 |
| 消息编码 | `encode_file_message` | 编码单条消息为队列 envelope |
| 消息编码 | `parse_file_message` | 解码队列 envelope |
| 消息编码 | `file_message_name` | 从 decoded payload 提取文件名 |
| 消息编码 | `serialize_file_payload` | 序列化 payload 为 bytes |
| 消息编码 | `publish_file_messages` | 批量编码并发布 |
| 服务 | `MetaWriter` | 文件写入服务 |
| 服务 | `MetaPacker` | 打包上传服务 |
| 恢复 | `ResidualRecovery` | 残留文件恢复 |
| Pipeline | `FileWriterPipeline` | Builder 模式 Pipeline 入口 |
| Pipeline | `run_pipeline_once` | 构建并运行一次 Pipeline |
| Pipeline | `run_pipeline_supervised` | Supervisor 模式守护 Pipeline 子进程 |
| 工具 | `get_node_id` | 获取当前节点 ID |
| 工具 | `get_meta_writer_process_count` | 读取 META_WRITER_PROCESS_COUNT 环境变量 |
| 工具 | `get_meta_writer_max_tasks_per_child` | 读取 META_WRITER_MAX_TASKS_PER_CHILD 环境变量 |

## License

MIT
