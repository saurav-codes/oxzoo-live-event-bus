// event-bus: a signed ingest API that stores events in Postgres and
// publishes them to NATS JetStream for two consumer workers.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"

	"oxzoo/event-bus/internal/bus"
	"oxzoo/event-bus/internal/zoo"
)

var (
	kindRe   = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	ingestV  = zoo.Verifier{KeyName: "EVENTS_KEY", Callers: []string{"celery-hub", "pipeline-py"}}
	readV    = zoo.Verifier{KeyName: "EVENTS_KEY", Callers: []string{"pipeline-py"}}
	verifyV  = zoo.Verifier{KeyName: "EVENTS_KEY", Callers: []string{"celery-hub", "pipeline-py"}}
	maxText  = 2000
	pageTmpl = template.Must(template.New("page").Parse(page))
)

type app struct {
	info zoo.Info
	db   *pgxpool.Pool
	js   jetstream.JetStream
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	db, err := bus.OpenPG(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if _, err := db.Exec(ctx, bus.Schema); err != nil {
			log.Fatalf("migrate: %v", err)
		}
		log.Print("schema up to date")
		return
	}
	nc, js, err := bus.OpenNATS("event-bus-api")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()
	go ensureStream(ctx, js)
	a := &app{info: zoo.Info{Name: "event-bus", Stack: "Go net/http + NATS JetStream + pgx", Started: time.Now(), BuiltAt: zoo.BuiltAt()}, db: db, js: js}
	if err := zoo.Serve(ctx, a.routes()); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// ensureStream retries until NATS answers, so the app serves health even
// while the bus is still starting.
func ensureStream(ctx context.Context, js jetstream.JetStream) {
	for ctx.Err() == nil {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := bus.EnsureStream(sctx, js)
		cancel()
		if err == nil {
			log.Printf("stream %s ready", bus.Stream)
			return
		}
		log.Printf("stream %s: %v (retrying)", bus.Stream, err)
		time.Sleep(3 * time.Second)
	}
}

func (a *app) routes() http.Handler {
	panel := zoo.ParseOrigins(os.Getenv("ZOO_PANEL_ORIGIN"))
	get := []string{http.MethodGet}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.index)
	mux.HandleFunc("POST /api/events", a.ingest)
	mux.HandleFunc("GET /api/events", a.list)
	mux.Handle("/_zoo/health", panel.Wrap(get, http.HandlerFunc(a.info.Health)))
	mux.Handle("/_zoo/probe", panel.Wrap(get, a.prober()))
	mux.Handle("/_zoo/trace/{id}", panel.Wrap(get, zoo.TraceHandler(a.hops)))
	mux.HandleFunc("GET /_zoo/verify", a.info.VerifyHandler(verifyV))
	return mux
}

type eventIn struct {
	Trace string `json:"trace"`
	Kind  string `json:"kind"`
	Text  string `json:"text"`
}

func (in *eventIn) validate(header string) error {
	if header != "" && !zoo.TraceRe.MatchString(header) {
		return errors.New("bad X-Zoo-Trace")
	}
	if in.Trace == "" {
		in.Trace = header
	}
	switch {
	case in.Trace != "" && !zoo.TraceRe.MatchString(in.Trace):
		return errors.New("bad trace")
	case !kindRe.MatchString(in.Kind):
		return errors.New("kind must match " + kindRe.String())
	case in.Text == "" || !utf8.ValidString(in.Text) || utf8.RuneCountInString(in.Text) > maxText:
		return fmt.Errorf("text must be 1 to %d characters", maxText)
	}
	return nil
}

func (a *app) ingest(w http.ResponseWriter, r *http.Request) {
	caller, body, ok := ingestV.Guard(w, r)
	if !ok {
		return
	}
	var in eventIn
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		zoo.WriteJSON(w, 400, map[string]string{"error": "body must be {trace, kind, text}"})
		return
	}
	if err := in.validate(r.Header.Get("X-Zoo-Trace")); err != nil {
		zoo.WriteJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), zoo.LocalTimeout)
	defer cancel()
	var trace *string
	if in.Trace != "" {
		trace = &in.Trace
	}
	ev := bus.Event{Trace: in.Trace, Kind: in.Kind, Text: in.Text}
	if err := a.db.QueryRow(ctx, `INSERT INTO events (trace, kind, text, caller) VALUES ($1, $2, $3, $4) RETURNING id`,
		trace, in.Kind, in.Text, caller).Scan(&ev.ID); err != nil {
		log.Printf("insert event: %v", err)
		zoo.WriteJSON(w, 503, map[string]string{"error": "store unavailable"})
		return
	}
	data, _ := json.Marshal(ev)
	ack, err := a.js.Publish(ctx, bus.Subject, data, jetstream.WithMsgID(strconv.FormatInt(ev.ID, 10)))
	if err != nil {
		log.Printf("publish event %d: %v", ev.ID, err)
		// The event never reached the bus: drop the row so a retry is clean.
		if _, derr := a.db.Exec(context.Background(), `DELETE FROM events WHERE id = $1`, ev.ID); derr != nil {
			log.Printf("undo event %d: %v", ev.ID, derr)
		}
		zoo.WriteJSON(w, 503, map[string]string{"error": "bus unavailable"})
		return
	}
	if err := bus.AddHop(ctx, a.db, in.Trace, "ingested", fmt.Sprintf("event %d from %s, stream seq %d", ev.ID, caller, ack.Sequence)); err != nil {
		log.Printf("hop: %v", err)
	}
	zoo.WriteJSON(w, 201, map[string]any{"id": ev.ID, "trace": in.Trace, "seq": ack.Sequence})
}

type eventOut struct {
	ID        int64     `json:"id"`
	Trace     *string   `json:"trace"`
	Kind      string    `json:"kind"`
	Text      string    `json:"text"`
	Caller    string    `json:"caller"`
	CreatedAt time.Time `json:"created_at"`
	Ledger    bool      `json:"-"`
}

func queryInt(r *http.Request, name string, def, lo, hi int64) (int64, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be an integer from %d to %d", name, lo, hi)
	}
	return n, nil
}

func (a *app) list(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := readV.Guard(w, r); !ok {
		return
	}
	after, err := queryInt(r, "after", 0, 0, 1<<62)
	limit, err2 := queryInt(r, "limit", 50, 1, 100)
	if err = errors.Join(err, err2); err != nil {
		zoo.WriteJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), zoo.LocalTimeout)
	defer cancel()
	events, err := a.events(ctx, `WHERE e.id > $1 ORDER BY e.id LIMIT $2`, after, limit)
	if err != nil {
		log.Printf("list events: %v", err)
		zoo.WriteJSON(w, 503, map[string]string{"error": "store unavailable"})
		return
	}
	next := after
	if len(events) > 0 {
		next = events[len(events)-1].ID
	}
	zoo.WriteJSON(w, 200, map[string]any{"events": events, "next": next})
}

func (a *app) events(ctx context.Context, where string, args ...any) ([]eventOut, error) {
	rows, err := a.db.Query(ctx, `SELECT e.id, e.trace::text, e.kind, e.text, e.caller, e.created_at, l.event_id IS NOT NULL
		FROM events e LEFT JOIN ledger l ON l.event_id = e.id `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []eventOut{}
	for rows.Next() {
		var e eventOut
		if err := rows.Scan(&e.ID, &e.Trace, &e.Kind, &e.Text, &e.Caller, &e.CreatedAt, &e.Ledger); err != nil {
			return nil, err
		}
		e.CreatedAt = e.CreatedAt.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}

func (a *app) hops(ctx context.Context, id string) ([]zoo.Hop, error) {
	rows, err := a.db.Query(ctx, `SELECT at, step, detail FROM hops WHERE trace = $1 ORDER BY at`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []zoo.Hop
	for rows.Next() {
		var h zoo.Hop
		var at time.Time
		if err := rows.Scan(&at, &h.Step, &h.Detail); err != nil {
			return nil, err
		}
		h.At = at.UTC().Format(time.RFC3339Nano)
		out = append(out, h)
	}
	return out, rows.Err()
}

func (a *app) index(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), zoo.LocalTimeout)
	defer cancel()
	events, err := a.events(ctx, `ORDER BY e.id DESC LIMIT 20`)
	data := map[string]any{"Events": events, "Error": err, "Release": zoo.Release(), "Server": zoo.Server()}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pageTmpl.Execute(w, data); err != nil {
		log.Printf("page: %v", err)
	}
}

const page = `<!doctype html><html><head><meta charset="utf-8"><title>event-bus</title>
<style>body{font:14px system-ui;margin:2rem;max-width:60rem}td,th{padding:.2rem .6rem;text-align:left}code{color:#555}</style></head>
<body><h1>event-bus</h1>
<p>Signed events land in Postgres, go out on JetStream subject <code>zoo.events</code> (stream <code>ZOO</code>),
and two durable consumers pick them up: <code>ledger</code> (Go, writes a ledger row) and <code>forward</code> (Python, forwards to search-svc).
Server {{.Server}}, release <code>{{.Release}}</code>.</p>
{{if .Error}}<p>Store unavailable.</p>{{end}}
<table><tr><th>id</th><th>at</th><th>caller</th><th>kind</th><th>text</th><th>ledger</th></tr>
{{range .Events}}<tr><td>{{.ID}}</td><td>{{.CreatedAt.Format "15:04:05"}}</td><td>{{.Caller}}</td><td>{{.Kind}}</td><td>{{.Text}}</td><td>{{if .Ledger}}yes{{else}}pending{{end}}</td></tr>
{{else}}<tr><td colspan="6">No events yet.</td></tr>{{end}}</table></body></html>`
