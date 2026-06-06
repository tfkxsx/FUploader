"""对象存储抽象接口。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class ObjectStore(ABC):
    """对象存储抽象接口。

    用于替代 COS/OSS/S3 等具体云存储的硬绑定。
    """

    @abstractmethod
    async def upload(self, local_path: Path, remote_key: str) -> bool:
        """上传本地文件到远端。

        Args:
            local_path: 本地文件路径。
            remote_key: 远端对象 key。

        Returns:
            True 表示上传成功。
        """
        ...

    @abstractmethod
    async def exists(self, remote_key: str) -> bool:
        """检查远端对象是否存在。

        Args:
            remote_key: 远端对象 key。

        Returns:
            True 表示存在。
        """
        ...