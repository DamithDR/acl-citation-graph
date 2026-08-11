"""
s2_client.py — a rate-limited Semantic Scholar HTTP client.

Enforces the introductory limit of 1 request/second across ALL endpoints,
globally (a single limiter shared by every call site), and handles HTTP 429
with Retry-After / exponential backoff. Import `get` and `post` from here
instead of calling requests directly.

Usage:
    from s2_client import get, post, set_rate
    r = get("https://api.semanticscholar.org/graph/v1/paper/...")
    r = post(url, json={...})

Set a slower/faster rate if your key changes:
    set_rate(1.0)   # requests per second (default 1.0)
"""

import os, time, threading
import requests

_API_KEY = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
_HEADERS = {"x-api-key": _API_KEY} if _API_KEY else {}

# --- global limiter -------------------------------------------------------
_lock = threading.Lock()
_min_interval = 1.0          # seconds between requests (1 req/sec)
_last = 0.0                  # monotonic timestamp of last request start

def set_rate(per_second: float):
    """Set the sustained request rate (requests per second)."""
    global _min_interval
    _min_interval = 1.0 / per_second if per_second > 0 else 0.0

def _throttle():
    """Block until at least _min_interval has elapsed since the last request."""
    global _last
    with _lock:
        now = time.monotonic()
        wait = _min_interval - (now - _last)
        if wait > 0:
            time.sleep(wait)
        _last = time.monotonic()

# --- request wrappers with 429 backoff ------------------------------------
def _request(method, url, *, max_retries=6, **kwargs):
    kwargs.setdefault("timeout", 60)
    headers = dict(_HEADERS)
    headers.update(kwargs.pop("headers", {}) or {})
    backoff = 2.0
    for attempt in range(max_retries):
        _throttle()                       # never exceed 1 req/sec
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 429:       # rate limited despite throttle
            retry_after = resp.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else backoff
            time.sleep(delay)
            backoff = min(backoff * 2, 60)
            continue
        if resp.status_code in (500, 502, 503, 504):
            time.sleep(backoff); backoff = min(backoff * 2, 60)
            continue
        return resp
    return resp                           # give caller the last response

def get(url, **kwargs):
    return _request("GET", url, **kwargs)

def post(url, **kwargs):
    return _request("POST", url, **kwargs)
