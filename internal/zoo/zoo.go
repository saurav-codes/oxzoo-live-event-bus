// Package zoo implements the oxzoo-live contract from DESIGN.md: health,
// probe, verify, trace, zoo-sig v1 signing, and CORS.
package zoo

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	MaxBody      = 64 << 10
	LocalTimeout = 5 * time.Second
	PeerTimeout  = 8 * time.Second
	probeTimeout = 20 * time.Second
	maxSkew      = 300
)

var (
	TraceRe  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	serverRe = regexp.MustCompile(`^s[0-9]+$`)
)

// Info describes this process for health, probe, and verify answers.
type Info struct {
	Name, Stack string
	Started     time.Time
	BuiltAt     string
}

// BuiltAt is the binary's modification time: it is written by the build step.
func BuiltAt() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	st, err := os.Stat(exe)
	if err != nil {
		return ""
	}
	return st.ModTime().UTC().Format(time.RFC3339)
}

func Release() string {
	r := os.Getenv("OX_RELEASE")
	if r == "" {
		return "unknown"
	}
	return r[:min(12, len(r))]
}

func Env() string {
	if e := os.Getenv("OX_ENV"); e != "" {
		return e
	}
	return "local"
}

// ServerLabel returns the sN label of a host name, else "local".
func ServerLabel(host string) string {
	for _, l := range strings.Split(host, ".") {
		if serverRe.MatchString(l) {
			return l
		}
	}
	return "local"
}

func Server() string { return ServerLabel(os.Getenv("PUBLIC_HOST")) }

// FP is the last 4 hex characters of sha256(value).
func FP(v string) string {
	s := sha256.Sum256([]byte(v))
	return hex.EncodeToString(s[:])[60:]
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

func (i Info) Health(w http.ResponseWriter, r *http.Request) {
	build := map[string]string{"runtime": runtime.Version()}
	if i.BuiltAt != "" {
		build["built_at"] = i.BuiltAt
	}
	WriteJSON(w, 200, map[string]any{
		"name": i.Name, "stack": i.Stack, "server": Server(), "release": Release(), "env": Env(),
		"uptime_s": int(time.Since(i.Started).Seconds()), "started_at": i.Started.UTC().Format(time.RFC3339),
		"build": build,
	})
}

// Origins is a CORS allowlist read from a comma list.
type Origins []string

func ParseOrigins(s string) Origins {
	var o Origins
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			o = append(o, p)
		}
	}
	return o
}

// Wrap answers CORS for listed origins and the OPTIONS preflight, and lets
// only the given methods through to h.
func (o Origins) Wrap(methods []string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && slices.Contains(o, origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.Header().Set("Access-Control-Max-Age", "600")
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !slices.Contains(methods, r.Method) {
			WriteJSON(w, 405, map[string]string{"error": "method not allowed"})
			return
		}
		h.ServeHTTP(w, r)
	})
}

// Sign returns the zoo-sig v1 hex signature.
func Sign(key string, t int64, method, pathQuery string, body []byte) string {
	bh := sha256.Sum256(body)
	m := hmac.New(sha256.New, []byte(key))
	fmt.Fprintf(m, "%d.%s.%s.%s", t, strings.ToUpper(method), pathQuery, hex.EncodeToString(bh[:]))
	return hex.EncodeToString(m.Sum(nil))
}

func SignHeader(key, caller string, t int64, method, pathQuery string, body []byte) string {
	return fmt.Sprintf("t=%d,caller=%s,sig=%s", t, caller, Sign(key, t, method, pathQuery, body))
}

// Verifier checks zoo-sig v1 signatures with one key and a caller allowlist.
type Verifier struct {
	KeyName string
	Callers []string
	Now     func() time.Time
}

var ErrTooLarge = errors.New("body too large")

// Verify reads the body (64 KB cap) and checks the signature. It returns the
// caller and the body; a non-nil error is one of the DESIGN.md reasons.
func (v Verifier) Verify(r *http.Request) (string, []byte, error) {
	key := os.Getenv(v.KeyName)
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	if err != nil {
		return "", nil, errors.New("bad format")
	}
	if len(body) > MaxBody {
		return "", nil, ErrTooLarge
	}
	h := r.Header.Get("X-Zoo-Signature")
	if h == "" {
		return "", nil, errors.New("missing signature")
	}
	parts := map[string]string{}
	for _, kv := range strings.Split(h, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok {
			return "", nil, errors.New("bad format")
		}
		parts[k] = val
	}
	t, err := strconv.ParseInt(parts["t"], 10, 64)
	sig, herr := hex.DecodeString(parts["sig"])
	if err != nil || herr != nil || len(sig) != sha256.Size || parts["caller"] == "" {
		return "", nil, errors.New("bad format")
	}
	now := time.Now
	if v.Now != nil {
		now = v.Now
	}
	if d := now().Unix() - t; d > maxSkew || d < -maxSkew {
		return "", nil, errors.New("expired")
	}
	caller := parts["caller"]
	if !slices.Contains(v.Callers, caller) {
		return "", nil, errors.New("unknown caller")
	}
	want, _ := hex.DecodeString(Sign(key, t, r.Method, r.URL.RequestURI(), body))
	if key == "" || !hmac.Equal(sig, want) {
		return "", nil, errors.New("bad signature")
	}
	return caller, body, nil
}

// Guard verifies a request and writes the 401 (or 413, 503) answer itself.
// ok is false when the handler must stop.
func (v Verifier) Guard(w http.ResponseWriter, r *http.Request) (caller string, body []byte, ok bool) {
	if os.Getenv(v.KeyName) == "" {
		WriteJSON(w, 503, map[string]any{"ok": false, "error": v.KeyName + " is not set"})
		return "", nil, false
	}
	caller, body, err := v.Verify(r)
	switch {
	case errors.Is(err, ErrTooLarge):
		WriteJSON(w, 413, map[string]any{"ok": false, "error": err.Error()})
	case err != nil:
		WriteJSON(w, 401, map[string]any{"ok": false, "error": err.Error()})
	default:
		return caller, body, true
	}
	return "", nil, false
}

// VerifyHandler serves GET /_zoo/verify.
func (i Info) VerifyHandler(v Verifier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, _, ok := v.Guard(w, r)
		if !ok {
			return
		}
		WriteJSON(w, 200, map[string]any{
			"ok": true, "name": i.Name, "public_url": os.Getenv("PUBLIC_URL"), "verified_by": v.KeyName,
			"key_fp": FP(os.Getenv(v.KeyName)), "caller": caller, "server": Server(), "release": Release(),
		})
	}
}

// Hop is one step of a chain trace.
type Hop struct {
	At     string `json:"at"`
	Step   string `json:"step"`
	Detail string `json:"detail"`
}

// TraceHandler serves GET /_zoo/trace/{id}.
func TraceHandler(lookup func(ctx context.Context, id string) ([]Hop, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !TraceRe.MatchString(id) {
			WriteJSON(w, 400, map[string]string{"error": "bad trace id"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), LocalTimeout)
		defer cancel()
		hops, err := lookup(ctx, id)
		if err != nil {
			log.Printf("trace %s: %v", id, err)
			WriteJSON(w, 500, map[string]string{"error": "trace lookup failed"})
			return
		}
		if hops == nil {
			hops = []Hop{}
		}
		WriteJSON(w, 200, map[string]any{"trace": id, "found": len(hops) > 0, "hops": hops})
	}
}

// Check is one probe check definition.
type Check struct {
	ID, Label string
	Env       []string
	Peer      string // expected peer name for cross-server checks
	PeerURL   string // the env var holding the peer URL
	Run       func(ctx context.Context) (detail string, err error)
}

// Var is one entry of the probe's vars list.
type Var struct {
	Name    string `json:"name"`
	FP      string `json:"fp,omitempty"`
	Value   string `json:"value,omitempty"`
	Role    string `json:"role"`
	Peer    string `json:"peer,omitempty"`
	Missing bool   `json:"missing,omitempty"`
}

// SecretVar and the helpers below build vars entries from the environment.
func SecretVar(name, role string) Var {
	if v := os.Getenv(name); v != "" {
		return Var{Name: name, FP: FP(v), Role: role}
	}
	return Var{Name: name, Missing: true, Role: role}
}

func PlainVar(name, role, peer string) Var {
	if v := os.Getenv(name); v != "" {
		return Var{Name: name, Value: v, Role: role, Peer: peer}
	}
	return Var{Name: name, Missing: true, Role: role, Peer: peer}
}

type checkResult struct {
	ID     string   `json:"id"`
	Label  string   `json:"label"`
	OK     bool     `json:"ok"`
	MS     int64    `json:"ms"`
	Detail string   `json:"detail,omitempty"`
	Error  string   `json:"error,omitempty"`
	Env    []string `json:"env"`
	Hops   []string `json:"hops"`
}

// Prober serves GET /_zoo/probe, one at a time.
type Prober struct {
	Info   Info
	Checks []Check
	Vars   func() []Var
	once   sync.Once
	slot   chan struct{}
}

func (p *Prober) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.once.Do(func() { p.slot = make(chan struct{}, 1) })
	select {
	case p.slot <- struct{}{}:
		defer func() { <-p.slot }()
	case <-time.After(5 * time.Second):
		WriteJSON(w, 429, map[string]string{"error": "probe busy"})
		return
	case <-r.Context().Done():
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
	defer cancel()
	start := time.Now()
	self := p.Info.Name + "@" + Server()
	results := make([]checkResult, len(p.Checks))
	var wg sync.WaitGroup
	for i, c := range p.Checks {
		wg.Go(func() { results[i] = runCheck(ctx, c, self) })
	}
	wg.Wait()
	ok := true
	for _, c := range results {
		ok = ok && c.OK
	}
	WriteJSON(w, 200, map[string]any{
		"name": p.Info.Name, "stack": p.Info.Stack, "server": Server(), "release": Release(), "env": Env(),
		"ok": ok, "ms": time.Since(start).Milliseconds(), "at": time.Now().UTC().Format(time.RFC3339),
		"checks": results, "vars": p.Vars(),
	})
}

func runCheck(ctx context.Context, c Check, self string) checkResult {
	res := checkResult{ID: c.ID, Label: c.Label, Env: c.Env, Hops: []string{self}}
	timeout := LocalTimeout
	if c.Peer != "" {
		timeout = PeerTimeout
		res.Hops = append(res.Hops, c.Peer+"@"+peerServer(os.Getenv(c.PeerURL)))
	}
	if res.Env == nil {
		res.Env = []string{}
	}
	for _, name := range c.Env {
		if os.Getenv(name) == "" {
			res.Error = name + " is not set"
			return res
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	detail, err := c.Run(ctx)
	res.MS = time.Since(start).Milliseconds()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			res.Error = fmt.Sprintf("timeout after %d ms", timeout.Milliseconds())
		} else {
			res.Error = err.Error()
		}
		return res
	}
	res.OK, res.Detail = true, detail
	return res
}

func peerServer(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "local"
	}
	return ServerLabel(u.Hostname())
}

// Serve listens on $HOST:$PORT until ctx ends, then drains in-flight
// requests for up to 25 s.
func Serve(ctx context.Context, h http.Handler) error {
	host, port := os.Getenv("HOST"), os.Getenv("PORT")
	if host == "" {
		host = "127.0.0.1"
	}
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: host + ":" + port, Handler: h, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Printf("listening on %s", srv.Addr)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

var PeerClient = &http.Client{Timeout: PeerTimeout}

// PeerVerify calls the peer's /_zoo/verify signed with keyName and checks the
// name, the key fingerprint, and that its public_url equals our URL variable.
func PeerVerify(ctx context.Context, urlVar, keyName, caller, peer string) (string, error) {
	base := strings.TrimRight(os.Getenv(urlVar), "/")
	key := os.Getenv(keyName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/_zoo/verify", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-Zoo-Signature", SignHeader(key, caller, time.Now().Unix(), "GET", "/_zoo/verify", nil))
	resp, err := PeerClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, MaxBody))
	var got struct {
		Name, Error string
		PublicURL   string `json:"public_url"`
		KeyFP       string `json:"key_fp"`
	}
	_ = json.NewDecoder(bytes.NewReader(raw)).Decode(&got)
	switch {
	case resp.StatusCode != 200:
		return "", fmt.Errorf("status %d %s", resp.StatusCode, got.Error)
	case got.Name != peer:
		return "", fmt.Errorf("name %q, want %q", got.Name, peer)
	case got.KeyFP != FP(key):
		return "", fmt.Errorf("key_fp %s, ours %s", got.KeyFP, FP(key))
	case strings.TrimRight(got.PublicURL, "/") != base:
		return "", fmt.Errorf("public_url %q differs from %s", got.PublicURL, urlVar)
	}
	return fmt.Sprintf("%s verified %s (fp %s)", peer, keyName, got.KeyFP), nil
}
