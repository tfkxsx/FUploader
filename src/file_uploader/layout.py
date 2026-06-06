"""Storage layout helpers for file-uploader metadata directories."""

from __future__ import annotations

from pathlib import Path

from .config import FileWriterConfig


def format_dir_id(config: FileWriterConfig, slot: int, seq: int) -> str:
    if config.storage_layout == "legacy_meta":
        return f"dir-{max(1, seq):06d}"
    return f"slot_{slot}_{max(0, seq)}"


def extract_dir_seq(dir_id: str) -> int:
    try:
        return int(str(dir_id).rsplit("-", 1)[-1])
    except ValueError:
        try:
            return int(str(dir_id).rsplit("_", 1)[-1])
        except ValueError:
            return 0


def active_root(config: FileWriterConfig) -> Path:
    if config.storage_layout == "legacy_meta":
        return config.storage_root / "active"
    return config.storage_root


def sealed_root(config: FileWriterConfig) -> Path:
    if config.storage_layout == "legacy_meta":
        return config.storage_root / "sealed"
    return config.storage_root


def ensure_layout_roots(config: FileWriterConfig) -> None:
    config.storage_root.mkdir(parents=True, exist_ok=True)
    active_root(config).mkdir(parents=True, exist_ok=True)
    sealed_root(config).mkdir(parents=True, exist_ok=True)


def slot_dir_name(config: FileWriterConfig, slot: int) -> str:
    if config.storage_layout == "legacy_meta":
        return f"slot-{max(0, slot)}"
    return ""


def active_dir_path(config: FileWriterConfig, slot: int, dir_id: str) -> Path:
    ensure_layout_roots(config)
    if config.storage_layout == "legacy_meta":
        slot_root = active_root(config) / slot_dir_name(config, slot)
        slot_root.mkdir(parents=True, exist_ok=True)
        path = slot_root / dir_id
    else:
        path = active_root(config) / dir_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def sealed_dir_path(config: FileWriterConfig, slot: int, dir_id: str) -> Path:
    ensure_layout_roots(config)
    if config.storage_layout == "legacy_meta":
        slot_root = sealed_root(config) / slot_dir_name(config, slot)
        slot_root.mkdir(parents=True, exist_ok=True)
        return slot_root / dir_id
    return sealed_root(config) / dir_id


def encode_ready_dir(config: FileWriterConfig, slot: int, dir_id: str) -> str:
    if config.ready_dir_format == "legacy_slot":
        return f"slot-{max(0, slot)}:{dir_id}"
    return f"{max(0, slot)}:{dir_id}"


def decode_ready_dir(ready_dir: str) -> tuple[int, str]:
    prefix, dir_id = ready_dir.split(":", 1)
    if prefix.startswith("slot-"):
        return int(prefix.split("-", 1)[-1]), dir_id
    return int(prefix), dir_id
