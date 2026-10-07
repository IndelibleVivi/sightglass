"""Read/publish adapter for Cloudflare Workers AI embeddings and Vectorize.

The adapter only ever talks to the official Cloudflare HTTPS API. It never
follows redirects, never provisions or deletes an index, never logs the server
body, account id or token, and never raises anything that embeds those secrets.
The API token is held only in process memory and is never part of ``repr``.

Vector upsert uses the multipart ``vectors`` form with an NDJSON part, matching the
observed Workers AI / Vectorize HTTP shape. Index validation reads the index
geometry and its metadata-index list (name + type) and never auto-provisions.

The transport is injectable so offline regression tests can exercise the exact
request shapes without any network egress.
"""

from __future__ import annotations

import json
import math
import struct
import uuid
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.operations import check_operation_budget, operation_remaining_seconds

from .settings import ACTIVE_DIMENSIONS, ACTIVE_METRIC, ACTIVE_MODEL

CF_API_ROOT = "https://api.cloudflare.com/client/v4"
MAX_ENCODE_BATCH = 16
READBACK_BATCH = 20


class CloudflareError(SightglassError):
    """Content-free failure raised for any Cloudflare transport/response problem."""

    def __init__(self, reason: str) -> None:
        super().__init__(
            ErrorCode.SERVICE_UNAVAILABLE,
            details={"reason": reason},
            retryable=True,
        )


class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        content: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]: ...


class HttpxTransport:
    """Minimal HTTPS transport; redirects disabled, body text never surfaced."""

    def __init__(self, client: Any | None = None) -> None:
        self._client = client

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        content: bytes | None,
        timeout: float,
    ) -> tuple[int, bytes]:
        try:
            if self._client is not None:
                response = self._client.request(
                    method, url, headers=headers, content=content, timeout=timeout
                )
            else:
                import httpx

                response = httpx.request(
                    method,
                    url,
                    headers=headers,
                    content=content,
                    timeout=timeout,
                    follow_redirects=False,
                )
            return int(response.status_code), bytes(response.content)
        except CloudflareError:
            raise
        except Exception as exc:  # transport failures are content-free
            raise CloudflareError("transport_unavailable") from exc


@dataclass(frozen=True)
class MetadataIndex:
    name: str
    index_type: str


@dataclass(frozen=True)
class IndexConfig:
    dimensions: int
    metric: str
    metadata: tuple[MetadataIndex, ...]

    def has(self, name: str, index_type: str) -> bool:
        return any(entry.name == name and entry.index_type == index_type for entry in self.metadata)


def _float32(values: list[float]) -> tuple[float, ...]:
    return struct.unpack(f"<{len(values)}f", struct.pack(f"<{len(values)}f", *values))


class CloudflareBackend:
    """Embeddings + Vectorize operations for one authorized index."""

    def __init__(
        self,
        settings: Any,
        token: str,
        *,
        transport: Transport | None = None,
    ) -> None:
        if not token or not isinstance(token, str):
            raise ValueError("cloudflare token is required")
        self._settings = settings
        self._token = token
        self._transport = transport or HttpxTransport()
        account = quote(str(settings.cf_account_id), safe="")
        self._base = f"{CF_API_ROOT}/accounts/{account}"
        self._index = quote(str(settings.index_name), safe="")
        self._vector_path = f"/vectorize/v2/indexes/{self._index}"
        self.index_name = str(settings.index_name)
        self.timeout = float(settings.timeout_seconds)

    def __repr__(self) -> str:  # never leak the token
        return f"CloudflareBackend(index_name={self.index_name!r})"

    __str__ = __repr__

    def _headers(self, *, multipart: bool = False) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._token}",
            "User-Agent": "sightglass-semantic/1",
        }
        if not multipart:
            headers["Content-Type"] = "application/json"
        return headers

    def _timeout(self) -> float:
        remaining = operation_remaining_seconds()
        if remaining is None:
            return self.timeout
        return max(0.0, min(self.timeout, remaining))

    def _call(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        multipart_body: bytes | None = None,
        multipart_content_type: str | None = None,
    ) -> Any:
        check_operation_budget()
        headers = self._headers(multipart=multipart_body is not None)
        if multipart_content_type is not None:
            headers["Content-Type"] = multipart_content_type
        payload = multipart_body
        if payload is None and body is not None:
            payload = json.dumps(body).encode()
        timeout = self._timeout()
        if timeout <= 0:
            check_operation_budget()
        try:
            status, raw = self._transport.request(
                method,
                self._base + path,
                headers=headers,
                content=payload,
                timeout=timeout,
            )
        except CloudflareError:
            raise
        except Exception as exc:
            raise CloudflareError("transport_unavailable") from exc
        check_operation_budget()
        if not 200 <= status < 300:
            raise CloudflareError(f"http_status_{status}")
        try:
            decoded = json.loads(raw or b"{}")
        except (ValueError, TypeError) as exc:
            raise CloudflareError("malformed_response") from exc
        if not isinstance(decoded, dict) or decoded.get("success") is not True:
            raise CloudflareError("api_error")
        return decoded.get("result")

    # -- discovery ---------------------------------------------------------
    def describe_index(self) -> IndexConfig:
        geometry = self._call("GET", self._vector_path)
        if not isinstance(geometry, dict):
            raise CloudflareError("index_missing")
        if geometry.get("name") != self.index_name:
            raise CloudflareError("index_missing")
        config = geometry.get("config") or {}
        listing = self._call("GET", self._vector_path + "/metadata_index/list")
        if not isinstance(listing, dict) or not isinstance(listing.get("metadataIndexes"), list):
            raise CloudflareError("invalid_metadata_index_response")
        metadata = []
        for row in listing["metadataIndexes"]:
            if not isinstance(row, dict):
                continue
            name = row.get("propertyName") or row.get("name")
            index_type = row.get("indexType") or row.get("type")
            if isinstance(name, str) and isinstance(index_type, str):
                canonical_type = index_type.lower()
                metadata.append(
                    MetadataIndex(name, "boolean" if canonical_type == "bool" else canonical_type)
                )
        return IndexConfig(
            dimensions=int(config.get("dimensions", 0)),
            metric=str(config.get("metric", "")),
            metadata=tuple(metadata),
        )

    def verify_index(self, *, required_metadata: tuple[tuple[str, str], ...]) -> IndexConfig:
        config = self.describe_index()
        if config.dimensions != ACTIVE_DIMENSIONS or config.metric != ACTIVE_METRIC:
            raise CloudflareError("index_configuration_mismatch")
        missing = [entry for entry in required_metadata if not config.has(*entry)]
        if missing:
            raise CloudflareError("metadata_indexes_missing")
        return config

    # -- embeddings --------------------------------------------------------
    def encode(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        if not texts:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        vectors: list[tuple[float, ...]] = []
        for offset in range(0, len(texts), MAX_ENCODE_BATCH):
            batch = list(texts[offset : offset + MAX_ENCODE_BATCH])
            result = self._call("POST", f"/ai/run/{ACTIVE_MODEL}", body={"text": batch})
            data = (result or {}).get("data")
            if not isinstance(data, list) or len(data) != len(batch):
                raise CloudflareError("invalid_embedding_batch")
            for vector in data:
                if not isinstance(vector, list) or len(vector) != ACTIVE_DIMENSIONS:
                    raise CloudflareError("invalid_embedding_dimensions")
                if not all(
                    isinstance(value, (int, float)) and math.isfinite(value) for value in vector
                ):
                    raise CloudflareError("invalid_embedding_values")
                vectors.append(_float32([float(value) for value in vector]))
        return tuple(vectors)

    # -- vector index ------------------------------------------------------
    @staticmethod
    def _multipart(rows: list[dict[str, Any]]) -> tuple[bytes, str]:
        boundary = "----sightglass" + uuid.uuid4().hex
        ndjson = ("\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n").encode()
        body = b"".join(
            [
                f"--{boundary}\r\n".encode(),
                b'Content-Disposition: form-data; name="vectors"; filename="vectors.ndjson"\r\n',
                b"Content-Type: application/x-ndjson\r\n\r\n",
                ndjson,
                f"\r\n--{boundary}--\r\n".encode(),
            ]
        )
        return body, f"multipart/form-data; boundary={boundary}"

    def upsert(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        body, content_type = self._multipart(rows)
        self._call(
            "POST",
            self._vector_path + "/upsert?unparsable-behavior=error",
            multipart_body=body,
            multipart_content_type=content_type,
        )

    def get_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for offset in range(0, len(ids), READBACK_BATCH):
            batch = ids[offset : offset + READBACK_BATCH]
            result = self._call("POST", self._vector_path + "/get_by_ids", body={"ids": batch})
            if not isinstance(result, list):
                raise CloudflareError("invalid_readback")
            rows.extend(row for row in result if isinstance(row, dict))
        return rows

    def query(
        self,
        vector: Any,
        namespace: str,
        filter: dict[str, Any],
        top_k: int,
    ) -> list[dict[str, Any]]:
        if not 1 <= int(top_k) <= 50:
            raise SightglassError(ErrorCode.QUERY_INVALID)
        body: dict[str, Any] = {
            "vector": [float(value) for value in vector],
            "namespace": namespace,
            "topK": int(top_k),
            "returnMetadata": "all",
            "returnValues": True,
        }
        if filter:
            body["filter"] = filter
        result = self._call("POST", self._vector_path + "/query", body=body)
        matches = (result or {}).get("matches")
        if not isinstance(matches, list):
            raise CloudflareError("invalid_query_result")
        return [row for row in matches if isinstance(row, dict)]
