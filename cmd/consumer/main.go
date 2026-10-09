// consumer-go: the durable "ledger" consumer. Each event becomes a ledger
// row and, when it carries a trace, the go-consumer hop. Acks only after the
// row is committed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"oxzoo/event-bus/internal/bus"
)

const worker = "consumer-go"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	db, err := bus.OpenPG(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	nc, js, err := bus.OpenNATS("event-bus-" + worker)
	if err != nil {
		return err
	}
	defer nc.Drain()
	if _, err := bus.EnsureStream(ctx, js); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, bus.Stream, bus.Durable("ledger"))
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	log.Printf("%s: consuming %s as durable ledger", worker, bus.Subject)
	handled := 0
	lastBeat := time.Time{}
	for ctx.Err() == nil {
		if time.Since(lastBeat) >= 10*time.Second {
			if err := bus.Beat(ctx, db, worker, fmt.Sprintf("%d events handled", handled)); err != nil {
				log.Printf("heartbeat: %v", err)
			}
			lastBeat = time.Now()
		}
		batch, err := cons.Fetch(10, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			log.Printf("fetch: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for msg := range batch.Messages() {
			if handle(ctx, db, msg) {
				handled++
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("batch: %v", err)
		}
	}
	return ctx.Err()
}

func handle(ctx context.Context, db *pgxpool.Pool, msg jetstream.Msg) bool {
	var ev bus.Event
	if err := json.Unmarshal(msg.Data(), &ev); err != nil || ev.ID <= 0 {
		log.Printf("poison message dropped: %v", err)
		_ = msg.Term()
		return false
	}
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := write(wctx, db, ev)
	if err != nil {
		meta, _ := msg.Metadata()
		attempt := uint64(1)
		if meta != nil {
			attempt = meta.NumDelivered
		}
		log.Printf("event %d attempt %d/%d: %v", ev.ID, attempt, bus.MaxDeliver, err)
		_ = msg.NakWithDelay(time.Duration(attempt) * 5 * time.Second)
		return false
	}
	if err := msg.DoubleAck(wctx); err != nil {
		log.Printf("ack event %d: %v", ev.ID, err)
	}
	return true
}

func write(ctx context.Context, db *pgxpool.Pool, ev bus.Event) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO ledger (event_id, kind) VALUES ($1, $2) ON CONFLICT DO NOTHING`, ev.ID, ev.Kind); err != nil {
		return err
	}
	if ev.Trace != "" {
		if _, err := tx.Exec(ctx, `INSERT INTO hops (trace, step, detail) VALUES ($1, 'go-consumer', $2) ON CONFLICT DO NOTHING`,
			ev.Trace, fmt.Sprintf("ledger row for event %d", ev.ID)); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
