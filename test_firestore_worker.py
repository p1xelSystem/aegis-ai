import asyncio
import hashlib
import os
import unittest
from unittest.mock import AsyncMock, patch

import httpx

import firestore_worker as worker
from firestore_http import error_detail, fetch, retry_after


class SerializationTests(unittest.TestCase):
    def test_pack_unpack_round_trip(self):
        values = [
            None,
            True,
            False,
            0,
            42,
            -5,
            "hello",
            [],
            ["hello", 42, False],
            {},
            {
                "name": "Aegis",
                "enabled": True,
                "count": 3,
                "nested": {
                    "items": ["one", "two"],
                },
            },
        ]

        for value in values:
            with self.subTest(value=value):
                self.assertEqual(
                    worker.unpack(worker.pack(value)),
                    value,
                )

    def test_unpack_double(self):
        self.assertEqual(
            worker.unpack({"doubleValue": 1.5}),
            1.5,
        )

    def test_fields(self):
        doc = {
            "fields": {
                "content": {"stringValue": "hello"},
                "count": {"integerValue": "7"},
                "active": {"booleanValue": True},
            }
        }

        self.assertEqual(
            worker.fields(doc),
            {
                "content": "hello",
                "count": 7,
                "active": True,
            },
        )

    def test_unsupported_field_rejected(self):
        with self.assertRaises(RuntimeError):
            worker.unpack({"unknownValue": "no"})


class OutcomeTests(unittest.TestCase):
    def test_public_reply(self):
        self.assertEqual(
            worker.outcome(
                "[FINAL RESPONSE]Hello.[END FINAL RESPONSE]"
            ),
            "Hello.",
        )

    def test_silence(self):
        self.assertIsNone(
            worker.outcome(
                "[NO RESPONSE][END NO RESPONSE]"
            )
        )

    def test_invalid_outputs_rejected(self):
        invalid = [
            "Hello.",
            "[FINAL RESPONSE][END FINAL RESPONSE]",
            (
                "extra text "
                "[FINAL RESPONSE]Hello.[END FINAL RESPONSE]"
            ),
            (
                "[FINAL RESPONSE]/ban someone"
                "[END FINAL RESPONSE]"
            ),
            (
                "[FINAL RESPONSE]@help"
                "[END FINAL RESPONSE]"
            ),
            (
                "[FINAL RESPONSE]"
                + "x" * 1801
                + "[END FINAL RESPONSE]"
            ),
            (
                "[FINAL RESPONSE]One.[END FINAL RESPONSE]"
                "[FINAL RESPONSE]Two.[END FINAL RESPONSE]"
            ),
        ]

        for text in invalid:
            with self.subTest(text=text[:80]):
                with self.assertRaises(RuntimeError):
                    worker.outcome(text)

    def test_secret_overlap_rejected(self):
        secret = "example-secret-value-for-test-only"

        with patch.dict(
            os.environ,
            {"GROQ_API_KEY": secret},
        ):
            with self.assertRaises(RuntimeError):
                worker.outcome(
                    "[FINAL RESPONSE]"
                    + secret
                    + "[END FINAL RESPONSE]"
                )


class HttpTests(unittest.IsolatedAsyncioTestCase):
    def test_error_redacts_secret_and_untrusted_url(self):
        secret = "example-private-value-for-test"

        response = httpx.Response(
            400,
            json={
                "error": {
                    "status": "INVALID_ARGUMENT",
                    "message": (
                        secret
                        + " https://example.com/private"
                        + " user@example.com"
                    ),
                }
            },
        )

        detail = error_detail(
            response,
            secrets=(secret,),
        )

        self.assertIn("HTTP 400", detail)
        self.assertIn("INVALID_ARGUMENT", detail)
        self.assertNotIn(secret, detail)
        self.assertNotIn(
            "https://example.com/private",
            detail,
        )
        self.assertNotIn("user@example.com", detail)

    def test_firebase_index_link_preserved(self):
        # This synthetic payload tests URL filtering only.
        # It is not an actual index-creation configuration.
        url = (
            "https://console.firebase.google.com"
            "/v1/r/project/test-project/firestore/indexes"
            "?create_exemption=TEST_PAYLOAD"
        )

        response = httpx.Response(
            400,
            json={
                "error": {
                    "status": "FAILED_PRECONDITION",
                    "message": (
                        "The query requires an index. "
                        "You can create it here: " + url
                    ),
                }
            },
        )

        self.assertIn(url, error_detail(response))

    def test_lookalike_index_host_redacted(self):
        url = (
            "https://console.firebase.google.com.example.com"
            "/v1/r/project/test-project/firestore/indexes"
            "?create_exemption=TEST_PAYLOAD"
        )

        response = httpx.Response(
            400,
            json={
                "error": {
                    "status": "FAILED_PRECONDITION",
                    "message": url,
                }
            },
        )

        self.assertNotIn(url, error_detail(response))

    def test_retry_after_seconds(self):
        response = httpx.Response(
            429,
            headers={"Retry-After": "12"},
        )

        self.assertEqual(retry_after(response), 12)

    def test_invalid_retry_after(self):
        response = httpx.Response(
            429,
            headers={"Retry-After": "not-a-date"},
        )

        self.assertEqual(retry_after(response), 0)

    async def test_quota_response_not_retried(self):
        response = httpx.Response(
            429,
            json={
                "error": {
                    "status": "RESOURCE_EXHAUSTED",
                }
            },
        )

        client = AsyncMock()
        client.request.return_value = response

        result = await fetch(
            client,
            "GET",
            (
                "https://firestore.googleapis.com/v1"
                "/projects/test-project"
                "/databases/(default)/documents/users/test"
            ),
        )

        self.assertIs(result, response)
        client.request.assert_awaited_once()

    async def test_writes_not_retried(self):
        response = httpx.Response(503)

        client = AsyncMock()
        client.request.return_value = response

        result = await fetch(
            client,
            "POST",
            (
                "https://firestore.googleapis.com/v1"
                "/projects/test-project"
                "/databases/(default)/documents:commit"
            ),
            json={"writes": []},
        )

        self.assertIs(result, response)
        client.request.assert_awaited_once()


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    def make_worker(self, client=None):
        w = worker.Worker(client)
        w.uid = "test-aegis-user"
        w.token = "test-token"

        w.checks = AsyncMock(
            return_value=(
                None,
                {
                    "role": "aegis",
                    "tier": "free",
                },
                0,
            )
        )

        return w

    def source_path(self, server):
        return (
            f"projects/{server}"
            "/databases/(default)/documents/"
            "conversations/test-room/messages/test-source"
        )

    async def test_blocked_check_prevents_send(self):
        w = self.make_worker()

        w.checks.side_effect = RuntimeError(
            "Sending blocked by staff lock"
        )

        with patch.object(
            worker,
            "fetch",
            new_callable=AsyncMock,
        ) as mocked_fetch:
            with self.assertRaises(RuntimeError):
                await w.send(
                    worker.MESSAGE_SERVERS[0],
                    "aegis-ai",
                    self.source_path(
                        worker.MESSAGE_SERVERS[0]
                    ),
                    {},
                    "Hello.",
                )

            mocked_fetch.assert_not_awaited()

    async def test_cross_project_source_rejected(self):
        w = self.make_worker()

        source = (
            "projects/unrelated-project"
            "/databases/(default)/documents/messages/test"
        )

        with patch.object(
            worker,
            "fetch",
            new_callable=AsyncMock,
        ) as mocked_fetch:
            with self.assertRaisesRegex(
                RuntimeError,
                "project scope",
            ):
                await w.send(
                    worker.MESSAGE_SERVERS[0],
                    "aegis-ai",
                    source,
                    {},
                    "Hello.",
                )

            mocked_fetch.assert_not_awaited()

    async def test_non_message_source_rejected(self):
        w = self.make_worker()
        server = worker.MESSAGE_SERVERS[0]

        source = (
            f"projects/{server}"
            "/databases/(default)/documents/users/test"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "source collection",
        ):
            await w.send(
                server,
                "aegis-ai",
                source,
                {},
                "Hello.",
            )

    async def test_existing_reply_prevents_write(self):
        w = self.make_worker()
        w.request = AsyncMock()

        server = worker.MESSAGE_SERVERS[2]
        source = self.source_path(server)

        with patch.object(
            worker,
            "fetch",
            new_callable=AsyncMock,
            return_value=httpx.Response(200),
        ) as mocked_fetch:
            await w.send(
                server,
                "aegis-ai",
                source,
                {},
                "Hello.",
            )

            mocked_fetch.assert_awaited_once()
            w.request.assert_not_awaited()

    async def test_nested_reply_targets_found_collection(self):
        w = self.make_worker()

        w.request = AsyncMock(
            return_value=httpx.Response(200)
        )

        server = worker.MESSAGE_SERVERS[0]
        source = self.source_path(server)

        with patch.object(
            worker,
            "fetch",
            new_callable=AsyncMock,
            return_value=httpx.Response(404),
        ) as mocked_fetch:
            await w.send(
                server,
                "aegis-ai",
                source,
                {
                    "uid": "test-member",
                    "username": "Member",
                    "content": "@Aegis hello",
                },
                "Hello.",
            )

        reply_id = "aegis_reply_" + hashlib.sha256(
            (
                w.uid + ":" + server + ":" + source
            ).encode()
        ).hexdigest()

        expected_name = (
            source.rsplit("/", 1)[0]
            + "/"
            + reply_id
        )

        expected_url = (
            "https://firestore.googleapis.com/v1/" + expected_name
        )

        self.assertEqual(
            mocked_fetch.await_args.args[2],
            expected_url,
        )

        w.request.assert_awaited_once()

        args = w.request.await_args.args
        kwargs = w.request.await_args.kwargs

        self.assertEqual(args[0], "POST")
        self.assertEqual(
            args[1],
            worker.base(server) + ":commit",
        )

        write = kwargs["json"]["writes"][0]

        self.assertEqual(
            write["update"]["name"],
            expected_name,
        )

        self.assertEqual(
            write["currentDocument"],
            {"exists": False},
        )

        message = worker.fields(
            write["update"]
        )

        self.assertEqual(
            message["channelId"],
            "aegis-ai",
        )

        self.assertEqual(
            message["content"],
            "Hello.",
        )

        self.assertEqual(
            w.checks.await_count,
            2,
        )

    async def test_failed_deduplication_read_prevents_write(self):
        w = self.make_worker()
        w.request = AsyncMock()

        server = worker.MESSAGE_SERVERS[0]

        with patch.object(
            worker,
            "fetch",
            new_callable=AsyncMock,
            return_value=httpx.Response(
                403,
                json={
                    "error": {
                        "status": "PERMISSION_DENIED",
                        "message": "Denied",
                    }
                },
            ),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "deduplication",
            ):
                await w.send(
                    server,
                    "aegis-ai",
                    self.source_path(server),
                    {},
                    "Hello.",
                )

        w.request.assert_not_awaited()

    async def test_already_replied_uses_canonical_document_url(self):
        w = self.make_worker()
        server = worker.MESSAGE_SERVERS[0]
        source = self.source_path(server)
        reply_id = "aegis_reply_" + hashlib.sha256(
            (w.uid + ":" + server + ":" + source).encode()
        ).hexdigest()
        expected_url = (
            "https://firestore.googleapis.com/v1/"
            + source.rsplit("/", 1)[0] + "/" + reply_id
        )
        for status, expected in ((200, True), (404, False)):
            with self.subTest(status=status):
                with patch.object(
                    worker, "fetch", new_callable=AsyncMock,
                    return_value=httpx.Response(status),
                ) as mocked_fetch:
                    self.assertIs(
                        await w.already_replied(server, source), expected
                    )
                    self.assertEqual(
                        mocked_fetch.await_args.args[2], expected_url
                    )

    async def test_recent_rejects_unknown_server(self):
        w = worker.Worker(None)
        w.request = AsyncMock()

        with self.assertRaisesRegex(
            RuntimeError,
            "Server not allowed",
        ):
            await w.recent("unrelated-project")

        w.request.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
