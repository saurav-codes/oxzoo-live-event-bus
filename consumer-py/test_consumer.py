import hashlib
import json

import pytest

from consumer import ingest_request, parse_event, sign

KEY = "zoo-test-key-0123456789abcdef"


def test_signing_vectors():
    body = b'{"sku":"ZOO-1"}'
    assert hashlib.sha256(body).hexdigest() == "cc2860a77ea231854ea58f9cb05f3217059f80a8e95d7b69a204293ae4f3a444"
    assert sign(KEY, 1760000000, "POST", "/api/items?x=1", body) == "50c22839fe6a06cb51a9fd25167d9e457eb0b5ee63ce696f4c5428a6b9271da1"
    assert sign(KEY, 1760000000, "get", "/_zoo/verify", b"") == "9a404bebaa32497c5ed39ef8990e8466428f8023d6aa9f5acc94f16fb7670ecb"
    assert hashlib.sha256(KEY.encode()).hexdigest()[-4:] == "915a"


def test_ingest_request_signs_exact_body():
    ev = parse_event(b'{"id":7,"trace":"3f2b8c1e-9a4d-4e2b-8f1a-0c9d8e7f6a5b","kind":"zoo.chain","text":"hi"}')
    body, headers = ingest_request(KEY, ev, 1760000000)
    assert json.loads(body) == {"event_id": 7, "trace": "3f2b8c1e-9a4d-4e2b-8f1a-0c9d8e7f6a5b", "kind": "zoo.chain", "text": "hi"}
    assert headers["X-Zoo-Signature"] == f"t=1760000000,caller=event-bus,sig={sign(KEY, 1760000000, 'POST', '/api/ingest', body)}"
    assert headers["X-Zoo-Trace"] == ev["trace"]


def test_untraced_event_has_no_trace_header():
    _, headers = ingest_request(KEY, parse_event(b'{"id":1,"kind":"k","text":"t"}'), 1)
    assert "X-Zoo-Trace" not in headers


@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"id":0,"kind":"k","text":"t"}', b'{"id":"1","kind":"k","text":"t"}', b'{"id":1}'])
def test_poison_messages(raw):
    with pytest.raises(ValueError):
        parse_event(raw)
