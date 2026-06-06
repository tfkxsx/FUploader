"""File message encoding/decoding helpers.

This module owns the queue message format used before FileWriterPipeline
consumes and writes files.
"""

from __future__ import annotations

import base64
import gzip
import inspect
import json
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable, Mapping


PublishCallable = Callable[[str], Awaitable[None] | None]


@dataclass(frozen=True)
class FileMessage:
    """A raw file payload before it is encoded for the queue."""

    payload: Any
    file_name: str | None = None


@dataclass(frozen=True)
class DecodedFilePayload:
    """A decoded queue message ready for file name generation and writing."""

    file_name: str
    payload: Any
    content_type: str

    @property
    def file_id(self) -> str:
        """Backward compatible alias for callers still reading file_id."""

        return self.file_name


def encode_file_message(
    item: FileMessage | Mapping[str, Any],
    *,
    file_name_field: str = "file_name",
    id_field: str | None = None,
) -> dict[str, Any]:
    """Encode one raw file payload into the queue envelope.

    Accepted input forms:
        FileMessage(file_name="1", payload={...})
        {"file_name": "1", "payload": {...}}
        {"file_name": "1.json", "content": "..."}

    Already encoded legacy envelopes containing ``compression`` and ``payload``
    are returned unchanged so rolling upgrades can drain old producers safely.
    """

    if isinstance(item, FileMessage):
        file_name = item.file_name
        payload = item.payload
    else:
        if _is_encoded_envelope(item):
            return dict(item)
        fallback_field = id_field or "file_id"
        file_name = str(
            item.get(file_name_field) or item.get("file_name") or item.get(fallback_field) or item.get("aweme_id") or ""
        ).strip()
        if "payload" in item:
            payload = item["payload"]
        elif "content" in item:
            payload = item["content"]
        else:
            raise ValueError("payload is required")

    if not file_name:
        file_name = uuid.uuid4().hex
    file_name = _normalize_file_name(file_name)

    raw_bytes, content_type = _payload_to_bytes(payload)
    compressed = gzip.compress(raw_bytes)
    return {
        "file_name": file_name,
        "compression": "gzip",
        "content_type": content_type,
        "payload": base64.b64encode(compressed).decode("ascii"),
    }


def parse_file_message(
    raw_body: bytes | str,
    *,
    name_fields: tuple[str, ...] = ("file_name", "file_id", "aweme_id"),
) -> DecodedFilePayload:
    """Decode a queue envelope produced by :func:`encode_file_message`."""

    body_text = raw_body.decode("utf-8") if isinstance(raw_body, bytes) else raw_body
    data = json.loads(body_text)
    if not isinstance(data, dict):
        raise ValueError("file message must be a json object")

    file_name = ""
    for field in name_fields:
        file_name = str(data.get(field) or "").strip()
        if file_name:
            break
    if not file_name:
        file_name = uuid.uuid4().hex
    file_name = _normalize_file_name(file_name)

    compression = str(data.get("compression") or "").strip().lower()
    encoded = str(data.get("payload") or "").strip()
    if compression != "gzip":
        raise ValueError("unsupported file message compression")
    if not encoded:
        raise ValueError("payload is required")

    compressed_bytes = base64.b64decode(encoded.encode("ascii"))
    raw_bytes = gzip.decompress(compressed_bytes)
    content_type = str(data.get("content_type") or "json").strip().lower()
    payload = _bytes_to_payload(raw_bytes, content_type)
    return DecodedFilePayload(file_name=file_name, payload=payload, content_type=content_type)


def file_message_name(data: Any, *, suffix: str = ".json") -> str:
    """Generate a stable local file name from a decoded file message."""

    if not suffix.startswith("."):
        raise ValueError("suffix must start with '.'")
    file_name = str(
        getattr(data, "file_name", "") or _mapping_get(data, "file_name") or _mapping_get(data, "file_id") or ""
    ).strip()
    if not file_name:
        file_name = uuid.uuid4().hex
    file_name = _normalize_file_name(file_name)
    if file_name.endswith(suffix):
        return file_name
    return f"{file_name}{suffix}"


def serialize_file_payload(payload: Any, *, content_type: str = "json") -> bytes:
    """Serialize decoded payload content for writing to disk."""

    content_type = content_type.lower()
    if content_type == "json":
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")
    if content_type == "text":
        return str(payload).encode("utf-8")
    if content_type == "bytes":
        if isinstance(payload, bytes):
            return payload
        raise TypeError("bytes payload must be bytes")
    raise ValueError(f"unsupported content_type: {content_type}")


async def publish_file_messages(
    items: Iterable[FileMessage | Mapping[str, Any]],
    publish: PublishCallable,
    *,
    file_name_field: str = "file_name",
    id_field: str | None = None,
) -> None:
    """Encode and publish many file messages using a caller-provided publisher."""

    publisher = FileMessagePublisher(publish, file_name_field=file_name_field, id_field=id_field)
    await publisher.publish_many(items)


class FileMessagePublisher:
    """Small async publisher that hides json/gzip/base64 queue wrapping."""

    def __init__(
        self,
        publish: PublishCallable,
        *,
        file_name_field: str = "file_name",
        id_field: str | None = None,
    ) -> None:
        self._publish = publish
        self._file_name_field = file_name_field
        self._id_field = id_field

    async def publish_many(self, items: Iterable[FileMessage | Mapping[str, Any]]) -> None:
        for item in items:
            await self.publish_one(item)

    async def publish_one(self, item: FileMessage | Mapping[str, Any]) -> None:
        envelope = encode_file_message(item, file_name_field=self._file_name_field, id_field=self._id_field)
        body = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
        result = self._publish(body)
        if inspect.isawaitable(result):
            await result


def _is_encoded_envelope(item: Mapping[str, Any]) -> bool:
    return "compression" in item and "payload" in item and (
        "file_name" in item or "file_id" in item or "aweme_id" in item
    )


def _normalize_file_name(file_name: str) -> str:
    if "/" in file_name or "\\" in file_name or file_name in {".", ".."}:
        raise ValueError("file_name must be a plain file name component")
    return file_name


def _payload_to_bytes(payload: Any) -> tuple[bytes, str]:
    if isinstance(payload, bytes):
        return payload, "bytes"
    if isinstance(payload, str):
        return payload.encode("utf-8"), "text"
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), "json"


def _bytes_to_payload(raw_bytes: bytes, content_type: str) -> Any:
    if content_type == "json":
        return json.loads(raw_bytes.decode("utf-8"))
    if content_type == "text":
        return raw_bytes.decode("utf-8")
    if content_type == "bytes":
        return raw_bytes
    raise ValueError(f"unsupported content_type: {content_type}")


def _mapping_get(data: Any, key: str) -> Any:
    return data.get(key) if isinstance(data, Mapping) else None
