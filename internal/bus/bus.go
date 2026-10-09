// Package bus holds what the API and the Go consumer share: the schema, the
// JetStream stream, hops, and heartbeats.
package bus

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	Stream       = "ZOO"
	Subject      = "zoo.events"
	ProbeSubject = "zoo.probe.>"
)

// Event is the message published on zoo.events.
type Event struct {
	ID    int64  `json:"id"`
	Trace string `json:"trace,omitempty"`
	Kind  string `json:"kind"`
	Text  string `json:"text"`
}

const Schema = `
CREATE TABLE IF NOT EXISTS events (
  id bigserial PRIMARY KEY, trace uuid, kind text NOT NULL, text text NOT NULL,
  caller text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS ledger (
  event_id bigint PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
  kind text NOT NULL, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS hops (
  trace uuid NOT NULL, step text NOT NULL, detail text NOT NULL,
  at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (trace, step));
CREATE TABLE IF NOT EXISTS heartbeats (
  worker text PRIMARY KEY, at timestamptz NOT NULL, detail text NOT NULL);
CREATE TABLE IF NOT EXISTS zoo_probe (
  id text PRIMARY KEY, v text NOT NULL, at timestamptz NOT NULL DEFAULT now());
`

func OpenPG(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 5
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	return pgxpool.NewWithConfig(ctx, cfg)
}

func OpenNATS(name string) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := nats.Connect(os.Getenv("NATS_URL"), nats.Name(name), nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1), nats.RetryOnFailedConnect(true))
	if err != nil {
		return nil, nil, fmt.Errorf("nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

// EnsureStream creates or updates the ZOO stream; safe to run on every start.
func EnsureStream(ctx context.Context, js jetstream.JetStream) (jetstream.Stream, error) {
	return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: Stream, Subjects: []string{Subject, ProbeSubject}, Storage: jetstream.FileStorage,
		MaxAge: 72 * time.Hour, MaxBytes: 256 << 20, Duplicates: 2 * time.Minute,
	})
}

// Durable is the consumer config both workers use, so redelivery is bounded
// the same way: 5 deliveries, 30 s to ack. Workers nak with a growing delay.
func Durable(name string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: name, FilterSubject: Subject, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 30 * time.Second, MaxDeliver: MaxDeliver, DeliverPolicy: jetstream.DeliverAllPolicy,
	}
}

const MaxDeliver = 5

// AddHop records a chain step once; a redelivery does not repeat it.
func AddHop(ctx context.Context, db *pgxpool.Pool, trace, step, detail string) error {
	if trace == "" {
		return nil
	}
	_, err := db.Exec(ctx, `INSERT INTO hops (trace, step, detail) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, trace, step, detail)
	return err
}

func Beat(ctx context.Context, db *pgxpool.Pool, worker, detail string) error {
	_, err := db.Exec(ctx, `INSERT INTO heartbeats (worker, at, detail) VALUES ($1, now(), $2)
		ON CONFLICT (worker) DO UPDATE SET at = now(), detail = $2`, worker, detail)
	return err
}
