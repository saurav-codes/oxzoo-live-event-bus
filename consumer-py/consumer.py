"""consumer-py: the durable "forward" consumer.

Each event on zoo.events is POSTed, signed with SEARCH_INGEST_KEY, to
${SEARCH_URL}/api/ingest. Only after search-svc accepts it (and the
py-forwarded hop is stored) is the message acked; failures are nak'ed with a
growing delay and JetStream stops after MAX_DELIVER deliveries.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import signal
import sys
import time

import httpx
import nats
import psycopg
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

WORKER = "consumer-py"
CALLER = "event-bus"
STREAM, SUBJECT, DURABLE = "ZOO", "zoo.events", "forward"
MAX_DELIVER = 5
INGEST_PATH = "/api/ingest"

log = logging.getLogger(WORKER)


def sign(key: str, t: int, method: str, path: str, body: bytes) -> str:
    body_hash = hashlib.sha256(body).hexdigest()
    msg = f"{t}.{method.upper()}.{path}.{body_hash}".encode()
    return hmac.new(key.encode(), msg, hashlib.sha256).hexdigest()


def sign_header(key: str, t: int, method: str, path: str, body: bytes) -> str:
    return f"t={t},caller={CALLER},sig={sign(key, t, method, path, body)}"


def parse_event(data: bytes) -> dict:
    """Validate a zoo.events message; raise ValueError on a poison message."""
    ev = json.loads(data)
    if not isinstance(ev, dict) or not isinstance(ev.get("id"), int) or ev["id"] <= 0:
        raise ValueError("event without a positive id")
    if not isinstance(ev.get("kind"), str) or not isinstance(ev.get("text"), str):
        raise ValueError("event without kind or text")
    trace = ev.get("trace") or ""
    return {"event_id": ev["id"], "trace": trace, "kind": ev["kind"], "text": ev["text"]}


def ingest_request(key: str, ev: dict, now: int) -> tuple[bytes, dict]:
    body = json.dumps(ev, separators=(",", ":")).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Zoo-Signature": sign_header(key, now, "POST", INGEST_PATH, body),
    }
    if ev["trace"]:
        headers["X-Zoo-Trace"] = ev["trace"]
    return body, headers


class Forwarder:
    def __init__(self, base: str, key: str, dsn: str):
        self.base, self.key, self.dsn = base.rstrip("/"), key, dsn
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(8.0))
        self.db: psycopg.AsyncConnection | None = None
        self.handled = 0

    async def conn(self) -> psycopg.AsyncConnection:
        if self.db is None or self.db.closed or self.db.broken:
            self.db = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True, connect_timeout=5)
        return self.db

    async def execute(self, sql: str, args: tuple) -> None:
        db = await self.conn()
        async with asyncio.timeout(5):
            await db.execute(sql, args)

    async def beat(self) -> None:
        await self.execute(
            "INSERT INTO heartbeats (worker, at, detail) VALUES (%s, now(), %s) "
            "ON CONFLICT (worker) DO UPDATE SET at = now(), detail = EXCLUDED.detail",
            (WORKER, f"{self.handled} events forwarded"),
        )

    async def handle(self, msg) -> None:
        try:
            ev = parse_event(msg.data)
        except ValueError as e:
            log.warning("poison message dropped: %s", e)
            await msg.term()
            return
        attempt = msg.metadata.num_delivered
        try:
            body, headers = ingest_request(self.key, ev, int(time.time()))
            resp = await self.http.post(self.base + INGEST_PATH, content=body, headers=headers)
            if resp.status_code == 400:
                log.warning("event %d refused by search-svc: %s", ev["event_id"], resp.text[:200])
                await msg.term()
                return
            resp.raise_for_status()
            if ev["trace"]:
                await self.execute(
                    "INSERT INTO hops (trace, step, detail) VALUES (%s, 'py-forwarded', %s) ON CONFLICT DO NOTHING",
                    (ev["trace"], f"event {ev['event_id']} to search-svc, status {resp.status_code}"),
                )
        except Exception as e:  # network, HTTP status, or database: retry later
            last = " (giving up)" if attempt >= MAX_DELIVER else ""
            log.warning("event %d attempt %d/%d: %s%s", ev["event_id"], attempt, MAX_DELIVER, e, last)
            await msg.nak(delay=5 * attempt)
            return
        await msg.ack()
        self.handled += 1


def require(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        sys.exit(f"{name} is not set")
    return value


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(name)s %(levelname)s %(message)s")
    fwd = Forwarder(require("SEARCH_URL"), require("SEARCH_INGEST_KEY"), require("DATABASE_URL"))
    nc = await nats.connect(require("NATS_URL"), name=f"event-bus-{WORKER}", connect_timeout=5, max_reconnect_attempts=-1)
    js = nc.jetstream()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    while not stop.is_set():  # the API creates the stream; wait for it
        try:
            await js.stream_info(STREAM)
            break
        except Exception as e:
            log.info("waiting for stream %s: %s", STREAM, e)
            await asyncio.sleep(3)
    config = ConsumerConfig(
        durable_name=DURABLE, filter_subject=SUBJECT, ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30, max_deliver=MAX_DELIVER, deliver_policy=DeliverPolicy.ALL,
    )
    await js.add_consumer(STREAM, config)
    sub = await js.pull_subscribe_bind(DURABLE, stream=STREAM)
    log.info("consuming %s as durable %s", SUBJECT, DURABLE)
    last_beat = 0.0
    while not stop.is_set():
        if time.monotonic() - last_beat >= 10:
            try:
                await fwd.beat()
            except Exception as e:
                log.warning("heartbeat: %s", e)
            last_beat = time.monotonic()
        try:
            msgs = await sub.fetch(10, timeout=5)
        except (TimeoutError, nats.errors.TimeoutError):
            continue
        except Exception as e:
            log.warning("fetch: %s", e)
            await asyncio.sleep(2)
            continue
        for msg in msgs:
            await fwd.handle(msg)
    try:
        await nc.drain()
    except nats.errors.Error:  # NATS already gone: nothing left to drain
        await nc.close()
    await fwd.http.aclose()
    log.info("stopped after %d events", fwd.handled)


if __name__ == "__main__":
    asyncio.run(main())
