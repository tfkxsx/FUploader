"""OSS 上传器 —— 多后端统一上传接口。

支持的后端:
    - COS:   腾讯云对象存储（cos-python-sdk-v5）
    - OSS:   阿里云对象存储（oss2）
    - S3:    AWS S3 / MinIO（boto3）
"""

from __future__ import annotations

import asyncio
import enum
import logging
from pathlib import Path
from typing import Optional

from ..interfaces.object_store import ObjectStore

logger = logging.getLogger(__name__)


class OssProvider(str, enum.Enum):
    """OSS 后端提供商枚举。"""

    COS = "cos"          # 腾讯云 COS
    ALIYUN_OSS = "oss"   # 阿里云 OSS
    S3 = "s3"            # AWS S3 / MinIO


class OssUploader(ObjectStore):
    """多后端 OSS 上传器。

    通过 provider 参数选择后端，支持 COS / 阿里云 OSS / S3。

    用法::

        # 腾讯云 COS
        uploader = OssUploader(
            provider=OssProvider.COS,
            bucket="my-bucket-1250000000",
            region="ap-guangzhou",
            endpoint="cos.ap-guangzhou.myqcloud.com",
            access_key_id="AKID...",
            access_key_secret="...",
        )

        # 阿里云 OSS
        uploader = OssUploader(
            provider=OssProvider.ALIYUN_OSS,
            bucket="my-bucket",
            endpoint="oss-cn-hangzhou.aliyuncs.com",
            access_key_id="LTAI...",
            access_key_secret="...",
        )

        # AWS S3 / MinIO
        uploader = OssUploader(
            provider=OssProvider.S3,
            bucket="my-bucket",
            region="us-east-1",
            endpoint="https://s3.amazonaws.com",
            access_key_id="AKIA...",
            access_key_secret="...",
        )
    """

    def __init__(
        self,
        *,
        provider: OssProvider,
        bucket: str,
        region: str = "",
        endpoint: str,
        access_key_id: str,
        access_key_secret: str,
    ) -> None:
        self._provider = provider
        self._bucket = bucket
        self._region = region
        self._endpoint = endpoint
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._client: Optional[object] = None

    # ------------------------------------------------------------------
    # 延迟初始化（避免未安装依赖时崩溃）
    # ------------------------------------------------------------------

    def _ensure_client(self) -> None:
        """确保后端客户端已初始化。"""
        if self._client is not None:
            return

        if self._provider == OssProvider.COS:
            self._client = self._build_cos_client()
        elif self._provider == OssProvider.ALIYUN_OSS:
            self._client = self._build_oss_client()
        elif self._provider == OssProvider.S3:
            self._client = self._build_s3_client()
        else:
            raise ValueError(f"Unsupported OSS provider: {self._provider}")

    def _build_cos_client(self):
        try:
            from qcloud_cos import CosConfig, CosS3Client
        except ImportError as e:
            raise ImportError(
                "COS provider requires 'cos-python-sdk-v5'. Install: pip install file-uploader[cos]"
            ) from e
        config = CosConfig(
            Region=self._region,
            SecretId=self._access_key_id,
            SecretKey=self._access_key_secret,
            Endpoint=self._endpoint,
        )
        logger.info("OssUploader(COS) initialized bucket=%s region=%s", self._bucket, self._region)
        return CosS3Client(config)

    def _build_oss_client(self):
        try:
            import oss2
        except ImportError as e:
            raise ImportError(
                "ALIYUN_OSS provider requires 'oss2'. Install: pip install file-uploader[oss]"
            ) from e
        auth = oss2.Auth(self._access_key_id, self._access_key_secret)
        logger.info("OssUploader(ALIYUN_OSS) initialized bucket=%s", self._bucket)
        return oss2.Bucket(auth, self._endpoint, self._bucket)

    def _build_s3_client(self):
        try:
            import boto3
            from botocore.config import Config as BotoConfig
        except ImportError as e:
            raise ImportError(
                "S3 provider requires 'boto3'. Install: pip install file-uploader[s3]"
            ) from e
        endpoint_url = self._endpoint if self._endpoint.startswith("http") else f"https://{self._endpoint}"
        client = boto3.client(
            "s3",
            region_name=self._region or "us-east-1",
            endpoint_url=endpoint_url,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._access_key_secret,
            config=BotoConfig(
                signature_version="s3v4",
                connect_timeout=30,
                read_timeout=30,
            ),
        )
        logger.info("OssUploader(S3) initialized bucket=%s endpoint=%s", self._bucket, endpoint_url)
        return client

    # ------------------------------------------------------------------
    # ObjectStore 接口
    # ------------------------------------------------------------------

    async def upload(self, local_path: Path, remote_key: str) -> bool:
        self._ensure_client()
        try:
            loop = asyncio.get_running_loop()

            if self._provider == OssProvider.COS:
                await loop.run_in_executor(None, self._cos_upload, local_path, remote_key)
            elif self._provider == OssProvider.ALIYUN_OSS:
                await loop.run_in_executor(None, self._oss_upload, local_path, remote_key)
            elif self._provider == OssProvider.S3:
                await loop.run_in_executor(None, self._s3_upload, local_path, remote_key)

            logger.info("Uploaded %s -> %s", local_path, remote_key)
            return True
        except Exception:
            logger.exception("Upload failed: %s -> %s", local_path, remote_key)
            return False

    async def exists(self, remote_key: str) -> bool:
        self._ensure_client()
        try:
            loop = asyncio.get_running_loop()

            if self._provider == OssProvider.COS:
                return await loop.run_in_executor(None, self._cos_exists, remote_key)
            elif self._provider == OssProvider.ALIYUN_OSS:
                return await loop.run_in_executor(None, self._oss_exists, remote_key)
            elif self._provider == OssProvider.S3:
                return await loop.run_in_executor(None, self._s3_exists, remote_key)
        except Exception:
            logger.exception("exists check failed: %s", remote_key)
            return False

    # ------------------------------------------------------------------
    # 各后端同步上传方法
    # ------------------------------------------------------------------

    def _cos_upload(self, local_path: Path, remote_key: str) -> None:
        from qcloud_cos import CosClientError, CosServiceError
        try:
            self._client.upload_file(
                Bucket=self._bucket,
                Key=remote_key,
                LocalFilePath=str(local_path),
            )
        except (CosClientError, CosServiceError) as e:
            raise RuntimeError(f"COS upload error: {e}") from e

    def _oss_upload(self, local_path: Path, remote_key: str) -> None:
        import oss2
        try:
            self._client.put_object_from_file(remote_key, str(local_path))
        except oss2.exceptions.OssError as e:
            raise RuntimeError(f"OSS upload error: {e}") from e

    def _s3_upload(self, local_path: Path, remote_key: str) -> None:
        try:
            self._client.upload_file(
                Filename=str(local_path),
                Bucket=self._bucket,
                Key=remote_key,
            )
        except Exception as e:
            raise RuntimeError(f"S3 upload error: {e}") from e

    # ------------------------------------------------------------------
    # 各后端 exists 方法
    # ------------------------------------------------------------------

    def _cos_exists(self, remote_key: str) -> bool:
        from qcloud_cos import CosClientError, CosServiceError
        try:
            self._client.head_object(Bucket=self._bucket, Key=remote_key)
            return True
        except (CosClientError, CosServiceError):
            return False

    def _oss_exists(self, remote_key: str) -> bool:
        import oss2
        try:
            return self._client.object_exists(remote_key)
        except oss2.exceptions.OssError:
            return False

    def _s3_exists(self, remote_key: str) -> bool:
        import botocore.exceptions
        try:
            self._client.head_object(Bucket=self._bucket, Key=remote_key)
            return True
        except botocore.exceptions.ClientError:
            return False