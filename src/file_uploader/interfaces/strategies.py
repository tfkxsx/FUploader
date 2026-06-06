"""策略类型定义（Callable / Protocol）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional, Union

# 消息解析器：将原始消息字节解析为结构化数据
MessageParser = Callable[[bytes], Union[Dict[str, Any], Awaitable[Dict[str, Any]]]]

# 文件名生成器：根据结构化数据生成文件名
FileNameGenerator = Callable[[Dict[str, Any]], str]

# 打包函数：将目录打包成归档文件，返回归档路径（失败返回 None）
Packer = Callable[[Path], Union[Optional[Path], Awaitable[Optional[Path]]]]

# 远端 key 生成器：根据本地归档文件名生成远端对象 key
RemoteKeyGenerator = Callable[[str], str]