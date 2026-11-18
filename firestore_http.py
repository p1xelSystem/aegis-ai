import asyncio
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit


log = logging.getLogger("aegis")


def _filter_error_url(match):
    """Keep only Firebase Console index-creation links."""
    url = match.group(0)

    try:
        parsed = urlsplit(url)
    except ValueError:
        return "[URL OMITTED]"

    allowed_path = re.fullmatch(
        r"/(?:v1/r/)?project/[a-z0-9-]+/firestore/indexes/?",
        parsed.path,
    )

    allowed_query = re.fullmatch(
        r"(?:create_composite|create_single_field|create_exemption)="
        r"[A-Za-z0-9_%=+-]+",
        parsed.query,
    )

    if (
        parsed.scheme == "https"
        and parsed.netloc == "console.firebase.google.com"
        and allowed_path
        and allowed_query
        and not parsed.fragment
    ):
        return url

    return "[URL OMITTED]"


def error_detail(response, secrets=()):
    status = "UNKNOWN"
    message = "No structured error available."

    try:
        payload = response.json()
        rows = payload if isinstance(payload, list) else [payload]

        error = next(
            (
                row["error"]
                for row in rows
                if isinstance(row, dict)
                and isinstance(row.get("error"), dict)
            ),
            {},
        )

        candidate = error.get("status", "")

        if (
            isinstance(candidate, str)
            and re.fullmatch(r"[A-Z_]{1,64}", candidate)
        ):
            status = candidate

        if isinstance(error.get("message"), str):
            message = error["message"][:8000]

    except (ValueError, TypeError):
        pass

    values = list(secrets) + [
        os.getenv(key, "")
        for key in (
            "SITE_USERNAME",
            "SITE_PASSWORD",
            "GROQ_API_KEY",
            "FIREBASE_ID_TOKEN",
            "FIREBASE_REFRESH_TOKEN",
        )
    ]

    for value in sorted(
        (value for value in values if value),
        key=len,
        reverse=True,
    ):
        message = message.replace(value, "[REDACTED]")

    message = re.sub(
        r"https?://\S+",
        _filter_error_url,
        message,
    )

    message = re.sub(
        r"Bearer\s+\S+",
        "Bearer [REDACTED]",
        message,
        flags=re.I,
    )

    message = re.sub(
        r"eyJ[\w-]+\.[\w-]+\.[\w-]+",
        "[TOKEN REDACTED]",
        message,
    )

    message = re.sub(
        r"[\w.+-]+@[\w.-]+",
        "[EMAIL REDACTED]",
        message,
    )

    message = re.sub(
        r"(?:projects/|/documents/)\S+",
        "[RESOURCE OMITTED]",
        message,
    )

    return "HTTP %s; status=%s; message=%s" % (
        response.status_code,
        status,
        " ".join(message.split())[:8000],
    )


def retry_after(response):
    raw = response.headers.get("Retry-After", "")

    try:
        value = float(raw)
    except ValueError:
        try:
            date = parsedate_to_datetime(raw)

            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)

            value = (
                date - datetime.now(timezone.utc)
            ).total_seconds()

        except (ValueError, TypeError, OverflowError):
            return 0

    if value != value:
        return 0

    return max(0, value)


async def fetch(client, method, url, *, secrets=(), **kwargs):
    parsed = urlsplit(url)

    read = (
        parsed.scheme == "https"
        and parsed.hostname == "firestore.googleapis.com"
        and (
            method.upper() == "GET"
            or (
                method.upper() == "POST"
                and parsed.path.endswith(":runQuery")
            )
        )
    )

    if not read:
        return await client.request(method, url, **kwargs)

    deadline = time.monotonic() + 120

    for attempt in range(5):
        remaining = deadline - time.monotonic()

        call_kwargs = dict(kwargs)
        call_kwargs["timeout"] = min(
            45,
            max(0.1, remaining),
        )

        response = await client.request(
            method,
            url,
            **call_kwargs,
        )

        if (
            response.status_code not in (502, 503, 504)
            or attempt == 4
        ):
            return response

        delay = max(
            5 * 2 ** attempt + random.uniform(0, 1),
            retry_after(response),
        )

        if delay + 1 >= deadline - time.monotonic():
            log.warning(
                "Firestore read retry budget exhausted; %s",
                error_detail(response, secrets),
            )
            return response

        log.warning(
            "Firestore read retry %s/4 in %.1fs; %s",
            attempt + 1,
            delay,
            error_detail(response, secrets),
        )

        await asyncio.sleep(delay)

    raise RuntimeError("Read retry loop exhausted")
