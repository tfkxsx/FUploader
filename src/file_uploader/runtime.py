"""Runtime identity helpers for supervised file-uploader workers."""

from __future__ import annotations

import os
import socket


def get_local_ip() -> str:
    """Return the default-route local IP, falling back to hostname resolution."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            ip_address = sock.getsockname()[0]
            if ip_address:
                return ip_address
    except OSError:
        pass
    try:
        ip_address = socket.gethostbyname(socket.gethostname())
        if ip_address:
            return ip_address
    except OSError:
        pass
    return "127.0.0.1"


def get_node_id() -> str:
    """Return configured NODE_ID or the legacy default node_id-{local_ip}."""
    configured = os.environ.get("NODE_ID", "").strip()
    if configured:
        return configured
    return f"node_id-{get_local_ip()}"


def get_meta_writer_process_count(default: int = 1) -> int:
    """Return META_WRITER_PROCESS_COUNT, falling back to default."""
    raw_value = os.environ.get("META_WRITER_PROCESS_COUNT", "").strip()
    if not raw_value:
        return max(1, default)
    try:
        return max(1, int(raw_value))
    except ValueError as exc:
        raise ValueError("META_WRITER_PROCESS_COUNT must be an integer >= 1") from exc


def get_meta_writer_rabbitmq_prefetch_count(default: int = 50) -> int:
    """Return META_WRITER_RABBITMQ_PREFETCH_COUNT, falling back to default."""
    raw_value = os.environ.get("META_WRITER_RABBITMQ_PREFETCH_COUNT", "").strip()
    if not raw_value:
        return max(1, default)
    try:
        return max(1, int(raw_value))
    except ValueError as exc:
        raise ValueError("META_WRITER_RABBITMQ_PREFETCH_COUNT must be an integer >= 1") from exc


def get_meta_writer_max_tasks_per_child(default: int = 1000) -> int:
    """Return META_WRITER_MAX_TASKS_PER_CHILD, falling back to default."""
    raw_value = os.environ.get("META_WRITER_MAX_TASKS_PER_CHILD", "").strip()
    if not raw_value:
        return max(0, default)
    try:
        return max(0, int(raw_value))
    except ValueError as exc:
        raise ValueError("META_WRITER_MAX_TASKS_PER_CHILD must be an integer >= 0") from exc


def get_residue_file_upload(default: bool = False) -> bool:
    """Return RESIDUE_FILE_UPLOADE/RESIDUE_FILE_UPLOAD as a boolean flag."""
    for env_name in ("RESIDUE_FILE_UPLOADE", "RESIDUE_FILE_UPLOAD"):
        raw_value = os.environ.get(env_name, "").strip().lower()
        if not raw_value:
            continue
        if raw_value in {"1", "true", "yes", "on"}:
            return True
        if raw_value in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"{env_name} must be a boolean value")
    return default


def node_scope(node_id: str) -> str:
    """Strip worker suffixes so node-local workers share one Redis scope."""
    normalized = node_id.strip()
    for marker in ("-meta-p", "-packer-p"):
        if marker in normalized:
            return normalized.split(marker, 1)[0]
    return normalized


def worker_index_from_node_id(node_id: str, marker: str = "-meta-p") -> int | None:
    """Parse worker index from a suffixed node id."""
    if marker not in node_id:
        return None
    suffix = node_id.rsplit(marker, 1)[-1]
    try:
        return max(0, int(suffix))
    except ValueError:
        return None
