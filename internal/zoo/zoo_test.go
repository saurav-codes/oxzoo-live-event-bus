package zoo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testKey = "zoo-test-key-0123456789abcdef"

func TestSigningVectors(t *testing.T) {
	if got := Sign(testKey, 1760000000, "POST", "/api/items?x=1", []byte(`{"sku":"ZOO-1"}`)); got != "50c22839fe6a06cb51a9fd25167d9e457eb0b5ee63ce696f4c5428a6b9271da1" {
		t.Errorf("POST vector: %s", got)
	}
	if got := Sign(testKey, 1760000000, "get", "/_zoo/verify", nil); got != "9a404bebaa32497c5ed39ef8990e8466428f8023d6aa9f5acc94f16fb7670ecb" {
		t.Errorf("GET vector: %s", got)
	}
	if got := FP(testKey); got != "915a" {
		t.Errorf("fp: %s", got)
	}
}

func TestVerifyReasons(t *testing.T) {
	t.Setenv("ZOO_KEY", testKey)
	v := Verifier{KeyName: "ZOO_KEY", Callers: []string{"celery-hub"}, Now: func() time.Time { return time.Unix(1760000000, 0) }}
	body := `{"sku":"ZOO-1"}`
	good := SignHeader(testKey, "celery-hub", 1760000000, "POST", "/api/items?x=1", []byte(body))
	cases := []struct{ header, body, want string }{
		{good, body, ""},
		{"", body, "missing signature"},
		{"garbage", body, "bad format"},
		{"t=abc,caller=celery-hub,sig=00", body, "bad format"},
		{SignHeader(testKey, "celery-hub", 1760000000-301, "POST", "/api/items?x=1", []byte(body)), body, "expired"},
		{SignHeader(testKey, "mesh-shop", 1760000000, "POST", "/api/items?x=1", []byte(body)), body, "unknown caller"},
		{good, body + " ", "bad signature"},
		{SignHeader("other-key", "celery-hub", 1760000000, "POST", "/api/items?x=1", []byte(body)), body, "bad signature"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("POST", "/api/items?x=1", strings.NewReader(c.body))
		if c.header != "" {
			r.Header.Set("X-Zoo-Signature", c.header)
		}
		caller, _, err := v.Verify(r)
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != c.want {
			t.Errorf("header %q: got %q, want %q", c.header, got, c.want)
		}
		if c.want == "" && caller != "celery-hub" {
			t.Errorf("caller %q", caller)
		}
	}
	big := httptest.NewRequest("POST", "/x", strings.NewReader(strings.Repeat("a", MaxBody+1)))
	if _, _, err := v.Verify(big); err != ErrTooLarge {
		t.Errorf("big body: %v", err)
	}
}

func TestCORS(t *testing.T) {
	o := ParseOrigins("https://zoo-control.s1.zoo.sorv.dev, http://localhost:5173")
	h := o.Wrap([]string{"GET"}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	r := httptest.NewRequest("GET", "/_zoo/health", nil)
	r.Header.Set("Origin", "http://localhost:5173")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" || w.Header().Get("Vary") != "Origin" {
		t.Errorf("allowed origin headers: %v", w.Header())
	}
	r = httptest.NewRequest("OPTIONS", "/_zoo/probe", nil)
	r.Header.Set("Origin", "https://zoo-control.s1.zoo.sorv.dev")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 204 || w.Header().Get("Access-Control-Max-Age") != "600" || w.Header().Get("Access-Control-Allow-Methods") != "GET, POST, OPTIONS" {
		t.Errorf("preflight: %d %v", w.Code, w.Header())
	}
	r = httptest.NewRequest("GET", "/_zoo/health", nil)
	r.Header.Set("Origin", "https://evil.example")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get("Access-Control-Allow-Origin") != "" || w.Code != 200 {
		t.Errorf("unlisted origin got CORS headers: %v", w.Header())
	}
	r = httptest.NewRequest("DELETE", "/_zoo/health", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 405 {
		t.Errorf("DELETE: %d", w.Code)
	}
}

func TestTraceIDAndServer(t *testing.T) {
	for id, want := range map[string]bool{
		"3f2b8c1e-9a4d-4e2b-8f1a-0c9d8e7f6a5b": true,
		"3F2B8C1E-9A4D-4E2B-8F1A-0C9D8E7F6A5B": false,
		"3f2b8c1e-9a4d-4e2b-8f1a-0c9d8e7f6a5":  false,
		"../etc/passwd":                        false,
	} {
		if TraceRe.MatchString(id) != want {
			t.Errorf("%q: want %v", id, want)
		}
	}
	if ServerLabel("event-bus.s3.zoo.sorv.dev") != "s3" || ServerLabel("localhost") != "local" {
		t.Error("server label")
	}
}
