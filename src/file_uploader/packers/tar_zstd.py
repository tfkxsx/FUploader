"""tar + zstd 打包器实现。"""

from __future__ import annotations

import asyncio
import logging
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

import zstandard as zstd

logger = logging.getLogger(__name__)


async def tar_zstd_packer(source_dir: Path) -> Optional[Path]:
    """将目录打包为 .tar.zst 归档文件。

    同步 IO 操作在 run_in_executor 中执行，避免阻塞事件循环。

    Args:
        source_dir: 待打包的源目录路径。

    Returns:
        归档文件路径，失败返回 None。
    """
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, _tar_zstd_sync, source_dir)
    except Exception:
        logger.exception("tar_zstd_packer failed for %s", source_dir)
        return None


def _tar_zstd_sync(source_dir: Path) -> Path:
    """同步打包逻辑。"""
    source_dir = source_dir.resolve()
    archive_path = source_dir.parent / f"{source_dir.name}.tar.zst"

    # 1. 创建 tar 文件
    with tempfile.NamedTemporaryFile(delete=False, suffix=".tar") as tmp_tar:
        tmp_name = tmp_tar.name

    try:
        with tarfile.open(tmp_name, "w") as tar:
            tar.add(str(source_dir), arcname=source_dir.name)

        # 2. zstd 压缩
        cctx = zstd.ZstdCompressor(level=3, threads=-1)
        with open(tmp_name, "rb") as tar_in:
            with open(archive_path, "wb") as zst_out:
                cctx.copy_stream(tar_in, zst_out)

        logger.info("Packed %s -> %s", source_dir, archive_path)
        return archive_path
    finally:
        # 清理临时 tar 文件
        Path(tmp_name).unlink(missing_ok=True)