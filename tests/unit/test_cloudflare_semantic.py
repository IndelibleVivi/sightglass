"""Offline regression tests for the Cloudflare semantic adapter request shapes."""

from __future__ import annotations

import json
import struct
import unittest

from sightglass.contracts.errors import ErrorCode, SightglassError
from sightglass.semantic.cloudflare import (
    ACTIVE_DIMENSIONS,
    CF_API_ROOT,
    CloudflareBackend,
    CloudflareError,
)
from sightglass.semantic.settings import SemanticSettings


def _settings(**overrides):
    base = {
        "enabled": True,
        "external_data_authorized": True,
        "cf_account_id": "a" * 32,
        "index_name": "sightglass-semantic-test",
        "source_account_id": "acct_fixture",
        "conversation_ids": ("conv_group",),
    }
    base.update(overrides)
    return SemanticSettings(**base)


def _vector(value: float = 0.5):
    return [value] * ACTIVE_DIMENSIONS


def _float32(values):
    return struct.unpack(f"<{len(values)}f", struct.pack(f"<{len(values)}f", *values))


class RecordingTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, headers, content, timeout):
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "content": content,
                "timeout": timeout,
            }
        )
        status, body = self.responses.pop(0)
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        return status, body


def _geometry():
    return {
        "success": True,
        "result": {
            "name": "sightglass-semantic-test",
            "config": {"dimensions": 1024, "metric": "cosine"},
        },
    }


def _metadata(names=("sent_at", "sender", "kind", "watermark", "has_link", "conversation")):
    types = {
        "sent_at": "number",
        "watermark": "number",
        "sender": "string",
        "kind": "string",
        "conversation": "string",
        "has_link": "bool",
    }
    return {
        "success": True,
        "result": {
            "metadataIndexes": [
                {"propertyName": name, "indexType": types[name].title()} for name in names
            ]
        },
    }


class CloudflareAdapterTests(unittest.TestCase):
    def test_token_is_never_in_repr(self) -> None:
        backend = CloudflareBackend(
            _settings(), "super-secret-token", transport=RecordingTransport([])
        )
        self.assertNotIn("super-secret-token", repr(backend))
        self.assertNotIn("super-secret-token", str(backend))

    def test_encode_uses_official_model_path_and_float32(self) -> None:
        transport = RecordingTransport(
            [(200, {"success": True, "result": {"data": [_vector(0.25)]}})]
        )
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        vectors = backend.encode(("synthetic text",))
        self.assertEqual(vectors, (_float32(_vector(0.25)),))
        call = transport.calls[0]
        self.assertEqual(call["url"], f"{CF_API_ROOT}/accounts/{'a' * 32}/ai/run/@cf/baai/bge-m3")
        self.assertEqual(json.loads(call["content"]), {"text": ["synthetic text"]})
        self.assertEqual(call["headers"]["Authorization"], "Bearer token")
        self.assertTrue(call["timeout"] > 0)

    def test_encode_rejects_wrong_dimension_and_over_batch(self) -> None:
        transport = RecordingTransport([(200, {"success": True, "result": {"data": [[1.0, 2.0]]}})])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        with self.assertRaises(CloudflareError):
            backend.encode(("synthetic",))
        backend = CloudflareBackend(_settings(), "token", transport=RecordingTransport([]))
        with self.assertRaises(SightglassError):
            backend.encode(())

    def test_encode_batches_at_16(self) -> None:
        responses = [
            (200, {"success": True, "result": {"data": [_vector(0.1)] * 16}}),
            (200, {"success": True, "result": {"data": [_vector(0.1)] * 4}}),
        ]
        transport = RecordingTransport(responses)
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        vectors = backend.encode(tuple(f"t{i}" for i in range(20)))
        self.assertEqual(len(vectors), 20)
        self.assertEqual(
            [len(json.loads(call["content"])["text"]) for call in transport.calls], [16, 4]
        )

    def test_verify_index_reads_geometry_and_typed_metadata_list(self) -> None:
        transport = RecordingTransport([(200, _geometry()), (200, _metadata())])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        config = backend.verify_index(
            required_metadata=(
                ("sent_at", "number"),
                ("sender", "string"),
                ("has_link", "boolean"),
            )
        )
        self.assertEqual(config.dimensions, 1024)
        self.assertEqual(config.metric, "cosine")
        self.assertEqual(
            [call["url"].split("/client/v4")[1] for call in transport.calls],
            [
                "/accounts/" + "a" * 32 + "/vectorize/v2/indexes/sightglass-semantic-test",
                "/accounts/"
                + "a" * 32
                + "/vectorize/v2/indexes/sightglass-semantic-test/metadata_index/list",
            ],
        )

    def test_verify_index_rejects_wrong_metric_dimensions_and_metadata_type(self) -> None:
        wrong_geometry = {
            "success": True,
            "result": {
                "name": "sightglass-semantic-test",
                "config": {"dimensions": 768, "metric": "euclidean"},
            },
        }
        backend = CloudflareBackend(
            _settings(), "token", transport=RecordingTransport([(200, wrong_geometry)])
        )
        with self.assertRaises(CloudflareError):
            backend.verify_index(required_metadata=())
        # A same-name but wrong-typed metadata index is not acceptable.
        wrong_type = {
            "success": True,
            "result": {"metadataIndexes": [{"propertyName": "sent_at", "indexType": "String"}]},
        }
        backend = CloudflareBackend(
            _settings(),
            "token",
            transport=RecordingTransport([(200, _geometry()), (200, wrong_type)]),
        )
        with self.assertRaises(CloudflareError):
            backend.verify_index(required_metadata=(("sent_at", "number"),))
        # A missing metadata index also fails closed (never auto-provisions).
        backend = CloudflareBackend(
            _settings(),
            "token",
            transport=RecordingTransport(
                [(200, _geometry()), (200, _metadata(names=("sent_at",)))]
            ),
        )
        with self.assertRaises(CloudflareError):
            backend.verify_index(required_metadata=(("sender", "string"),))

    def test_upsert_requires_an_explicit_success_envelope(self) -> None:
        for response in (b"", b"not-json", {"result": {"mutationId": "m"}}, {"success": False}):
            with self.subTest(response=response):
                backend = CloudflareBackend(
                    _settings(), "token", transport=RecordingTransport([(200, response)])
                )
                with self.assertRaises(CloudflareError):
                    backend.upsert([{"id": "synthetic", "values": [0.1]}])

    def test_upsert_uses_multipart_vectors_ndjson(self) -> None:
        transport = RecordingTransport([(200, {"success": True, "result": {"mutationId": "m"}})])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        backend.upsert(
            [{"id": "abc", "namespace": "ns", "values": [0.1], "metadata": {"kind": "text"}}]
        )
        call = transport.calls[0]
        self.assertTrue(
            call["url"].endswith(
                "/vectorize/v2/indexes/sightglass-semantic-test/upsert?unparsable-behavior=error"
            )
        )
        self.assertTrue(call["headers"]["Content-Type"].startswith("multipart/form-data"))
        body = call["content"].decode()
        self.assertIn('name="vectors"', body)
        self.assertIn("Content-Type: application/x-ndjson", body)
        payload = next(line for line in body.splitlines() if line.startswith('{"id"'))
        self.assertEqual(json.loads(payload)["id"], "abc")

    def test_get_by_ids_batches_at_20(self) -> None:
        responses = [
            (200, {"success": True, "result": []}),
            (200, {"success": True, "result": []}),
            (200, {"success": True, "result": []}),
        ]
        transport = RecordingTransport(responses)
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        backend.get_by_ids([f"id-{i}" for i in range(45)])
        sizes = [len(json.loads(call["content"])["ids"]) for call in transport.calls]
        self.assertEqual(sizes, [20, 20, 5])

    def test_query_shape_filter_and_topk_bounds(self) -> None:
        transport = RecordingTransport([(200, {"success": True, "result": {"matches": []}})])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        backend.query([0.5] * ACTIVE_DIMENSIONS, "ns", {"kind": {"$eq": "text"}}, 5)
        body = json.loads(transport.calls[0]["content"])
        self.assertEqual(body["namespace"], "ns")
        self.assertEqual(body["topK"], 5)
        self.assertEqual(body["returnMetadata"], "all")
        self.assertEqual(body["filter"], {"kind": {"$eq": "text"}})
        with self.assertRaises(SightglassError):
            backend.query([0.0] * ACTIVE_DIMENSIONS, "ns", {}, 0)
        with self.assertRaises(SightglassError):
            backend.query([0.0] * ACTIVE_DIMENSIONS, "ns", {}, 51)

    def test_http_error_does_not_leak_body_or_token(self) -> None:
        transport = RecordingTransport([(500, b'{"errors":[{"message":"secret body"}]}')])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        with self.assertRaises(CloudflareError) as caught:
            backend.encode(("synthetic",))
        self.assertNotIn("secret body", str(caught.exception))
        self.assertNotIn("token", str(caught.exception))

    def test_transport_failure_is_content_free(self) -> None:
        class Boom:
            def request(self, *args, **kwargs):
                raise OSError("host api.cloudflare.com unreachable token=xyz")

        backend = CloudflareBackend(_settings(), "token", transport=Boom())
        with self.assertRaises(CloudflareError) as caught:
            backend.encode(("synthetic",))
        self.assertNotIn("xyz", str(caught.exception))

    def test_operation_budget_bounds_timeout(self) -> None:
        from sightglass.operations import operation_budget

        transport = RecordingTransport(
            [(200, {"success": True, "result": {"data": [_vector(0.1)]}})]
        )
        backend = CloudflareBackend(_settings(timeout_seconds=8.0), "token", transport=transport)
        with operation_budget(0.5):
            backend.encode(("synthetic",))
        self.assertLessEqual(transport.calls[0]["timeout"], 0.5)

    def test_cancelled_budget_aborts_before_request(self) -> None:
        import threading

        from sightglass.operations import operation_budget

        transport = RecordingTransport([])
        backend = CloudflareBackend(_settings(), "token", transport=transport)
        cancelled = threading.Event()
        cancelled.set()
        with self.assertRaises(SightglassError) as caught:
            with operation_budget(5.0, cancelled=cancelled):
                backend.encode(("synthetic",))
        self.assertEqual(caught.exception.code, ErrorCode.SERVICE_TIMEOUT)
        self.assertEqual(transport.calls, [])


if __name__ == "__main__":
    unittest.main()
