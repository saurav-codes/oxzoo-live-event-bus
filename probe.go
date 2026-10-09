package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"

	"oxzoo/event-bus/internal/bus"
	"oxzoo/event-bus/internal/zoo"
)

const maxLag = 500

func token() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (a *app) prober() *zoo.Prober {
	pg, nats := []string{"DATABASE_URL"}, []string{"NATS_URL"}
	return &zoo.Prober{
		Info: a.info,
		Checks: []zoo.Check{
			{ID: "postgres", Label: "Postgres write, read, delete", Env: pg, Run: a.checkPG},
			{ID: "jetstream", Label: "JetStream publish and fetch on a probe subject", Env: nats, Run: a.checkJetStream},
			{ID: "lag:ledger", Label: "Lag of durable ledger (Go consumer)", Env: nats, Run: a.checkLag("ledger")},
			{ID: "lag:forward", Label: "Lag of durable forward (Python consumer)", Env: nats, Run: a.checkLag("forward")},
			{ID: "worker:consumer-go", Label: "Go consumer heartbeat", Env: pg, Run: a.checkBeat("consumer-go")},
			{ID: "worker:consumer-py", Label: "Python consumer heartbeat", Env: pg, Run: a.checkBeat("consumer-py")},
			{ID: "peer:search-svc", Label: "Signed call to search-svc", Env: []string{"SEARCH_URL", "SEARCH_INGEST_KEY"},
				Peer: "search-svc", PeerURL: "SEARCH_URL", Run: func(ctx context.Context) (string, error) {
					return zoo.PeerVerify(ctx, "SEARCH_URL", "SEARCH_INGEST_KEY", "event-bus", "search-svc")
				}},
		},
		Vars: func() []zoo.Var {
			return []zoo.Var{
				zoo.SecretVar("EVENTS_KEY", "verifies"),
				zoo.SecretVar("SEARCH_INGEST_KEY", "signs"),
				zoo.SecretVar("DATABASE_URL", "service"),
				zoo.SecretVar("NATS_URL", "service"),
				zoo.PlainVar("SEARCH_URL", "url", "search-svc"),
				zoo.PlainVar("ZOO_PANEL_ORIGIN", "plain", ""),
			}
		},
	}
}

func (a *app) checkPG(ctx context.Context) (string, error) {
	id, want := "probe-"+token(), token()
	if _, err := a.db.Exec(ctx, `INSERT INTO zoo_probe (id, v) VALUES ($1, $2)`, id, want); err != nil {
		return "", err
	}
	defer a.db.Exec(context.Background(), `DELETE FROM zoo_probe WHERE id = $1`, id)
	var got, version string
	if err := a.db.QueryRow(ctx, `SELECT v, current_setting('server_version') FROM zoo_probe WHERE id = $1`, id).Scan(&got, &version); err != nil {
		return "", err
	}
	if got != want {
		return "", fmt.Errorf("read back %q, wrote %q", got, want)
	}
	tag, err := a.db.Exec(ctx, `DELETE FROM zoo_probe WHERE id = $1`, id)
	if err != nil || tag.RowsAffected() != 1 {
		return "", fmt.Errorf("delete: %v", err)
	}
	return "zoo_probe row round trip, server " + version, nil
}

func (a *app) checkJetStream(ctx context.Context) (string, error) {
	stream, err := bus.EnsureStream(ctx, a.js)
	if err != nil {
		return "", err
	}
	subject, want := "zoo.probe."+token(), token()
	ack, err := a.js.Publish(ctx, subject, []byte(want))
	if err != nil {
		return "", fmt.Errorf("publish: %w", err)
	}
	defer stream.Purge(context.Background(), jetstream.WithPurgeSubject(subject))
	cons, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject, AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: time.Minute,
	})
	if err != nil {
		return "", fmt.Errorf("consumer: %w", err)
	}
	defer stream.DeleteConsumer(context.Background(), cons.CachedInfo().Name)
	msg, err := cons.Next(jetstream.FetchMaxWait(3 * time.Second))
	if err != nil {
		return "", fmt.Errorf("fetch: %w", err)
	}
	if string(msg.Data()) != want {
		return "", fmt.Errorf("fetched %q, published %q", msg.Data(), want)
	}
	if err := msg.Ack(); err != nil {
		return "", err
	}
	return fmt.Sprintf("published seq %d on %s, fetched it back, consumer deleted", ack.Sequence, bus.Stream), nil
}

func (a *app) checkLag(durable string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		c, err := a.js.Consumer(ctx, bus.Stream, durable)
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			return "", fmt.Errorf("durable %s not created yet (is its worker running?)", durable)
		}
		if err != nil {
			return "", err
		}
		info, err := c.Info(ctx)
		if err != nil {
			return "", err
		}
		lag := info.NumPending + uint64(info.NumAckPending)
		detail := fmt.Sprintf("pending %d, unacked %d, redelivered %d", info.NumPending, info.NumAckPending, info.NumRedelivered)
		if lag > maxLag {
			return "", fmt.Errorf("lag %d over %d: %s", lag, maxLag, detail)
		}
		return detail, nil
	}
}

func (a *app) checkBeat(worker string) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		var at time.Time
		var detail string
		err := a.db.QueryRow(ctx, `SELECT at, detail FROM heartbeats WHERE worker = $1`, worker).Scan(&at, &detail)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("%s never sent a heartbeat", worker)
		}
		if err != nil {
			return "", err
		}
		age := time.Since(at).Round(time.Second)
		if age > 30*time.Second {
			return "", fmt.Errorf("last heartbeat %s ago", age)
		}
		return fmt.Sprintf("heartbeat %s ago: %s", age, detail), nil
	}
}
