#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""测试用 mock OpenAI 兼容上游：网关开发自测（非流式/流式/models/延迟模拟）。
用法: python test_mock_openai.py [port]   默认 8199
"""
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"running": 0, "gen": 0, "prompt": 0}     # /metrics 用：供大屏作为监控目标抓取
LOCK = threading.Lock()


def _metrics_text():
    with LOCK:
        running, gen, prompt = STATE["running"], STATE["gen"], STATE["prompt"]
    kv = min(0.95, 0.05 + gen / 20000.0)
    return (("# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running %d\n"
             "# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting 0\n"
             "# TYPE vllm:kv_cache_usage_perc gauge\nvllm:kv_cache_usage_perc %.4f\n"
             "# TYPE vllm:generation_tokens_total counter\nvllm:generation_tokens_total %d\n"
             "# TYPE vllm:prompt_tokens_total counter\nvllm:prompt_tokens_total %d\n")
            % (running, kv, gen, prompt))


class MockHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/v1/models"):
            self._json(200, {"object": "list", "data": [
                {"id": "mock-model", "object": "model", "owned_by": "mock"}]})
        elif self.path == "/health":
            self._json(200, {"ok": True})
        elif self.path.split("?")[0] == "/metrics":
            body = _metrics_text().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n) if n else b""
        with LOCK:
            STATE["running"] += 1
        try:
            self._mock_post(body)
        finally:
            with LOCK:
                STATE["running"] -= 1
                STATE["gen"] += getattr(self, "_c_tok", 0)
                STATE["prompt"] += getattr(self, "_p_tok", 0)

    def _mock_post(self, body):
        try:
            j = json.loads(body.decode("utf-8", "replace"))
        except Exception:
            j = {}
        msgs = j.get("messages") or [{"role": "user", "content": str(j.get("prompt", ""))}]
        last = ""
        for m in reversed(msgs):
            if m.get("role") == "user":
                last = m.get("content") or ""
                break
        if not last:
            last = str(j.get("prompt", ""))
        reply = "echo: " + str(last)[:80]
        self._p_tok = max(1, len(str(last)) // 4 + 4)
        self._c_tok = max(1, len(reply) // 4 + 4)
        p_tok, c_tok = self._p_tok, self._c_tok
        path = self.path.split("?")[0]
        if path.endswith("/chat/completions"):
            if j.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True
                chunks = ["ech", "o: ", "th", "is", " is", " a", " strea", "m test"]
                for i, c in enumerate(chunks):
                    d = json.dumps({"id": "mock", "object": "chat.completion.chunk",
                                    "choices": [{"index": 0, "delta": {"content": c},
                                                 "finish_reason": None}]}, ensure_ascii=False)
                    self.wfile.write(("data: " + d + "\n\n").encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(0.05)
                done = json.dumps({"id": "mock", "object": "chat.completion.chunk",
                                   "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
                self.wfile.write(("data: " + done + "\n\n").encode("utf-8"))
                if (j.get("stream_options") or {}).get("include_usage"):
                    u = json.dumps({"id": "mock", "object": "chat.completion.chunk",
                                    "choices": [],
                                    "usage": {"prompt_tokens": p_tok, "completion_tokens": c_tok,
                                              "total_tokens": p_tok + c_tok}})
                    self.wfile.write(("data: " + u + "\n\n").encode("utf-8"))
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                return
            self._json(200, {
                "id": "mock", "object": "chat.completion",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": reply},
                             "finish_reason": "stop"}],
                "usage": {"prompt_tokens": p_tok, "completion_tokens": c_tok,
                          "total_tokens": p_tok + c_tok}})
        elif path.endswith("/completions"):
            self._json(200, {
                "id": "mock", "object": "text_completion",
                "choices": [{"index": 0, "text": reply, "finish_reason": "length"}],
                "usage": {"prompt_tokens": p_tok, "completion_tokens": c_tok,
                          "total_tokens": p_tok + c_tok}})
        else:
            self._json(404, {"error": "not found"})


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8199
    print("mock openai upstream on :%d" % port)
    ThreadingHTTPServer(("127.0.0.1", port), MockHandler).serve_forever()
