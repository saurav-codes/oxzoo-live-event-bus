# event-bus

> **Role in the zoo:** project `event-bus` of [oxzoo-live](https://github.com/saurav-codes/oxzoo-live-control/blob/main/zoo/README.md#projects), deployed with ox on server s3 at https://event-bus.s3.zoo.sorv.dev. The contract it follows is [DESIGN.md](https://github.com/saurav-codes/oxzoo-live-control/blob/main/zoo/DESIGN.md).

A Go ingest API on s3 that stores signed events in Postgres and publishes them to NATS JetStream, where two durable consumers in two languages pick them up: a Go worker writes a ledger row, a Python worker forwards each event, signed, to search-svc. It is the middle of the `event-river` chain (`ingested`, `go-consumer`, `py-forwarded`).

## What it proves

- A custom service installed from apt (`packages = ["nats-server"]`) with JetStream on its own data dir, a token from the generated password, and a reference-style `provides` (`NATS_URL = "nats://${password}@${host}:${port}"`).
- One repo, three processes, two languages: the Go app, a Go worker, and a Python worker managed by uv in `consumer-py/` (`uv sync --frozen`, `uv.lock` committed).
- At-least-once delivery: the stream `ZOO` (subject `zoo.events`, 72 h, 256 MB, 2 min duplicate window keyed by event id) is created idempotently at start; consumers ack only after their work is stored, nak with a growing delay on failure, and stop after 5 deliveries; poison messages are terminated.
- zoo-sig v1 both ways: verifies `EVENTS_KEY`, signs `SEARCH_INGEST_KEY`.

## ox features

`[app] health`, `[build] commands` (three builds) and `migrate`, `[workers] consumer-go` and `consumer-py`, `[services] postgres` (shared) and `[services.nats]` (custom, apt package, memory cap, `provides`), `[tools] uv`. The NATS token goes into a 0600 config file instead of the command line.

## Routes

| Route | What |
|---|---|
| `GET /` | the last 20 events and whether the ledger has them |
| `POST /api/events` | signed (`EVENTS_KEY`, callers `celery-hub`, `pipeline-py`), body `{"trace","kind","text"}` up to 64 KB; `kind` matches `^[a-z][a-z0-9._-]{0,63}$`, `text` is 1 to 2000 characters, `trace` (or `X-Zoo-Trace`) a uuid. Stored, then published; if the publish fails the row is removed and the answer is 503. 201 `{"id","trace","seq"}`, hop `ingested` |
| `GET /api/events?after=<id>&limit=<1..100>` | signed (`EVENTS_KEY`, caller `pipeline-py`). `{"events":[...],"next":id}` |
| `GET /_zoo/verify` | signed with `EVENTS_KEY` |
| `GET /_zoo/trace/{id}` | hops from Postgres; a malformed id is 400 |
| `GET /_zoo/health` | contract health |
| `GET /_zoo/probe` | `postgres` (zoo_probe round trip), `jetstream` (publish and fetch on a probe subject with a throwaway consumer, deleted after), `lag:ledger` and `lag:forward` (pending under 500), `worker:consumer-go` and `worker:consumer-py` (heartbeat rows under 30 s), `peer:search-svc` (signed `/_zoo/verify` to `SEARCH_URL`, matching name, key fp, and `public_url`) |

## Variables

| Variable | Who sets it | What |
|---|---|---|
| `DATABASE_URL` | ox, `[services] postgres` | probe role `service` |
| `NATS_URL` | ox, `[services.nats] provides` | probe role `service` |
| `PORT`, `HOST`, `OX_*`, `PUBLIC_URL`, `PUBLIC_HOST` | ox | |
| `EVENTS_KEY` | you, the shared secret | role `verifies` |
| `SEARCH_INGEST_KEY` | you, the shared secret | role `signs` (consumer-py) |
| `SEARCH_URL` | you | `https://search-svc.s2.zoo.sorv.dev`, role `url` |
| `ZOO_PANEL_ORIGIN` | you | `https://zoo-control.s1.zoo.sorv.dev` |

## Tests

```sh
go vet ./... && go test ./...
cd consumer-py && uv run pytest
```

Recorded on 2026-10-09: `ok oxzoo/event-bus`, `ok oxzoo/event-bus/internal/zoo` (signing vectors, verify reasons, CORS, trace id and server label, event validation) and `8 passed` in consumer-py (the signing vectors in Python, the header, event parsing and poison messages, the ingest request).

Local integration (Homebrew Postgres 18.6, nats-server 2.15 and Meilisearch 1.54.3 started with the exact `ox.toml` run commands, search-svc running next to it): a signed event got hops `ingested`, `go-consumer`, `py-forwarded` here and `received`, `indexed` on search-svc; unknown caller and bad signature 401, `limit=500` 400, verify 200; the probe answered `ok: true` in 23 ms with the peer check passing; preflight 204 with the CORS headers, no headers for an unlisted origin; two concurrent probes both 200; with search-svc stopped, consumer-py retried (attempts 1/5, 2/5) and delivered after it came back. The `service` vars were added after that run and are covered by building and the unit tests only.

## ox check

```
ox check . (manifest: ox.toml)

  app.start                  .ox/bin/app                                          detected:go.mod
  app.health                 /_zoo/health                                         declared
  build.commands[0]          go build -o .ox/bin/app .                            declared
  build.commands[1]          go build -o .ox/bin/consumer ./cmd/consumer          declared
  build.commands[2]          cd consumer-py && uv sync --frozen --no-dev          declared
  build.migrate              .ox/bin/app migrate                                  declared
  workers.consumer-go        exec .ox/bin/consumer                                declared
  workers.consumer-py        cd consumer-py && exec .venv/bin/python consumer.py  declared
  tools.go                   1.27                                                 detected:go.mod
  tools.uv                   0.11                                                 declared
  services.nats              custom (only for this project)                       declared
  services.postgres          postgres 18 (shared)                                 default

  Provided by ox: PORT, HOST, OX_ENV, OX_PROJECT, OX_RELEASE, OX_DATA_DIR, PUBLIC_URL, PUBLIC_HOST, NATS_URL, DATABASE_URL
  Set on the dashboard before the first deploy: EVENTS_KEY, SEARCH_INGEST_KEY, SEARCH_URL, ZOO_PANEL_ORIGIN

Ready to deploy.
```

Not verified: the `nats-server` apt package on Ubuntu 26.04 (version, and whether it starts its own system unit next to ox's).
