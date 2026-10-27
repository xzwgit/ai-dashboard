#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""AI 推理性能监控大屏 —— Prometheus /metrics 抓取 + Web 大屏 + API 分发网关（单文件纯标准库）。

用法:
  # 监控一个 vLLM 服务（服务端口即 OpenAI 端口，/metrics 自动拼接）:
  python vllm_dashboard.py --target my-vllm=http://127.0.0.1:8123 --api-key sk-xxxx
  # 多目标 + 指定端口:
  python vllm_dashboard.py --port 8124 --target a=http://10.0.0.1:8123 --target b=http://10.0.0.2:8123
  # 无服务时先看界面效果（内置仿真数据源）:
  python vllm_dashboard.py --demo

  目标也可在页面里随时增删（存 targets.json，与 --target 共存合并）。

指标口径:
  - 生成/预填充吞吐 = 计数器差分（trailing ~12s 窗口）
  - TTFT/TPOT/E2E/排队延迟分位 = 直方图 bucket 差分（trailing ~30s 窗口，仅统计新完成请求）
  - 投机接受率 = Δaccepted/Δdraft（窗口）；前缀缓存命中 = Δhits/Δqueries
  - 兼容 vLLM 新旧指标名（token_generation_tokens_total/generation_tokens_total、
    e2e_request_latency_seconds/request_latency_seconds、request_queue_time_seconds/time_in_queue_requests）

依赖: Python 3.8+ 标准库；远程机/本地机均可运行。
"""
import argparse
import base64
import hashlib
import json
import math
import os
import random
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ============================== 配置 ==============================

VERSION = "2.1.1"    # v2.1.1：小屏打磨（图表3+2/KPI铺满/文本左对齐/下拉首点修复/占位文案）；v1 纯监控见 vllm_dashboard_v1.py

W_RATE = 12.0        # 吞吐差分窗口（秒）
W_PCT = 30.0         # 延迟分位窗口（秒）
DEFAULT_INTERVAL = 2.0
DEFAULT_HISTORY = 5400  # 每目标最多保留点数（2s 间隔 ≈ 3 小时）
NODE_INFO_PORT = 8124   # 引擎机宿主机简况端口：目标机上跑同款大屏（默认端口）即自动接入 CPU/GPU/内存信息

# 计数器/仪表别名（新名在前，旧名在后，命中哪个用哪个）
ALIAS = {
    "gen_tokens": ["vllm:token_generation_tokens_total", "vllm:generation_tokens_total"],
    "prompt_tokens": ["vllm:prompt_tokens_total"],
    "running": ["vllm:num_requests_running"],
    "waiting": ["vllm:num_requests_waiting"],
    "swapped": ["vllm:num_requests_swapped"],
    "kv_perc": ["vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc"],
    "kv_used_tokens": ["vllm:kv_cache_usage_num_tokens", "vllm:gpu_cache_usage_num_tokens"],
    "kv_max_tokens": ["vllm:kv_cache_max_num_tokens", "vllm:gpu_cache_max_num_tokens"],
    "preemptions": ["vllm:num_preemptions_total"],
    "spec_accepted": ["vllm:spec_decode_num_accepted_tokens_total"],
    "spec_draft": ["vllm:spec_decode_num_draft_tokens_total"],
    "pc_queries": ["vllm:prefix_cache_queries_total"],
    "pc_hits": ["vllm:prefix_cache_hits_total"],
}
# 直方图别名
ALIAS_HIST = {
    "ttft": ["vllm:time_to_first_token_seconds"],
    "tpot": ["vllm:request_time_per_output_token_seconds", "vllm:time_per_output_token_seconds"],
    "e2e": ["vllm:e2e_request_latency_seconds", "vllm:request_latency_seconds"],
    "queue": ["vllm:request_queue_time_seconds", "vllm:time_in_queue_requests"],
}

SERIES_KEYS = [
    "gen_rate", "prompt_rate", "running", "waiting", "kv_perc",
    "ttft_p50", "ttft_p95", "tpot_p50", "tpot_p95", "e2e_p95", "queue_p95",
    "spec_accept", "prefix_hit", "gen_tokens_total", "prompt_tokens_total",
]

# ============================== Prometheus 文本解析 ==============================

_RE_LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+([^\s]+)\s*$')
_RE_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def _parse_value(s):
    if s in ("+Inf", "Inf"):
        return math.inf
    if s == "-Inf":
        return -math.inf
    if s == "NaN":
        return math.nan
    try:
        return float(s)
    except ValueError:
        return None


def _unesc(v):
    return v.replace('\\"', '"').replace("\\\\", "\\").replace("\\n", "\n")


def parse_prom_text(text):
    """解析 Prometheus exposition 文本 → (simple{name:val}, labeled{(name,labelvals):val}, hist{prefix:{buckets,sum,count}})"""
    simple, labeled, hist = {}, {}, {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _RE_LINE.match(line)
        if not m:
            continue
        name, _, lbl_src, val_src = m.group(1), m.group(2), m.group(3), m.group(4)
        val = _parse_value(val_src)
        if val is None:
            continue
        labels = {}
        if lbl_src:
            for k, v in _RE_LABEL.findall(lbl_src):
                labels[k] = _unesc(v)
        if name.endswith("_bucket") and "le" in labels:
            le = _parse_value(labels["le"])
            if le is None:
                continue
            h = hist.setdefault(name[:-7], {"buckets": [], "sum": 0.0, "count": 0.0})
            h["buckets"].append((le, val))
        elif name.endswith("_sum") or name.endswith("_count"):
            base = name[:-4] if name.endswith("_sum") else name[:-6]
            h = hist.setdefault(base, {"buckets": [], "sum": 0.0, "count": 0.0})
            if name.endswith("_sum"):
                h["sum"] = val
            else:
                h["count"] = val
        elif labels:
            labeled[(name, tuple(sorted(labels.items())))] = val
        else:
            simple[name] = val
    for h in hist.values():
        h["buckets"].sort(key=lambda x: x[0])
    return simple, labeled, hist


def hist_quantile(buckets, q):
    """Prometheus histogram_quantile 插值。buckets=[(le,cum)...] 已按 le 升序。"""
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    rank = q * total
    prev_le, prev_cum = 0.0, 0.0
    for le, cum in buckets:
        if cum >= rank:
            if cum <= prev_cum:
                return le
            frac = (rank - prev_cum) / (cum - prev_cum)
            if math.isinf(le):
                return prev_le
            qv = prev_le + (le - prev_le) * frac
            return qv if math.isfinite(qv) else None
        prev_le, prev_cum = le, cum
    return buckets[-1][0]


def hist_delta(b_now, b_ref):
    """两份累计 bucket 快照差分 → 窗口直方图。"""
    if not b_now:
        return None
    if not b_ref:
        return b_now
    ref_map = {le: c for le, c in b_ref}
    out = []
    for le, c in b_now:
        out.append((le, max(0.0, c - ref_map.get(le, c))))
    return out


# ============================== 目标与抓取 ==============================

def wire_local_node(t):
    """本机目标（127.0.0.1/localhost）直连本进程 NodeCollector，省一次 HTTP。"""
    try:
        from urllib.parse import urlparse
        h = (urlparse(t.url).hostname or "").lower()
        if h in ("127.0.0.1", "localhost", "::1"):

            def _prov():
                if Handler.node and Handler.node.available:
                    return Handler.node.snapshot()
                return None
            t._node_provider = _prov
    except Exception:
        pass


class Target:
    _seq = 0

    def __init__(self, name, url, api_key="", interval=DEFAULT_INTERVAL, hist_max=DEFAULT_HISTORY,
                 model_path="", context_len=""):
        Target._seq += 1
        self.id = "t%d_%s" % (Target._seq, base64.b16encode(os.urandom(3)).decode().lower())
        self.name = name
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.model_path = model_path
        self.context_len = str(context_len or "")
        self.meta_info = {}       # 自动抓取: version / model_ids
        self.interval = interval
        self.hist_max = hist_max
        self.status = "init"     # init/up/down
        self.last_error = None
        self.last_ts = 0.0
        self.monitor_since = time.time()
        self.n_scrapes = 0
        # 原始点（供差分）与派生序列
        self._t = []            # 每次抓取的 ts
        self._raw = []          # [{gauge/counter 快照}, ...]（别名解析后）
        self._hist = []         # [{prefix: {"buckets":..., "sum":.., "count":..}}, ...]
        self.series = {k: [] for k in SERIES_KEYS}
        self._finished = {}
        self.has_spec = False
        self.has_prefix_cache = False
        self.kv_max_tokens = 0
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.node = None          # 宿主机简况（同款大屏实例的 /api/node；无则 None）
        self._node_provider = None  # 本机目标直连进程内 NodeCollector（不走 HTTP）
        from urllib.parse import urlparse as _up
        _pu = _up(self.url)
        _host = _pu.hostname or "127.0.0.1"
        self.node_url = "%s://%s:%d/api/node" % (_pu.scheme or "http", _host, NODE_INFO_PORT)
        self.thread = threading.Thread(target=self._loop, daemon=True)

    # ---------- 抓取 ----------
    def metrics_url(self):
        u = self.url
        from urllib.parse import urlparse
        path = urlparse(u).path
        if path in ("", "/"):
            return u.rstrip("/") + "/metrics"
        return u  # 已带路径（如 /api/demo-metrics 或完整 /metrics 地址）则不拼

    def _scrape(self):
        req = urllib.request.Request(self.metrics_url())
        if self.api_key:
            req.add_header("Authorization", "Bearer " + self.api_key)
        with urllib.request.urlopen(req, timeout=min(8.0, self.interval * 3 + 2)) as r:
            return r.read().decode("utf-8", "replace")

    def _loop(self):
        while not self._stop.wait(self.interval):
            try:
                text = self._scrape()
                simple, labeled, hist = parse_prom_text(text)
                self._ingest(simple, labeled, hist)
                self.status = "up"
                self.last_error = None
            except Exception as e:
                self.status = "down"
                self.last_error = self._friendly_error(e)
            if self.n_scrapes % 30 == 1:      # 模型元数据 ~60s 刷新一次
                threading.Thread(target=self._fetch_meta, daemon=True).start()
            if self.n_scrapes % 5 == 1:       # 宿主机简况 ~10s 刷新一次（引擎机上跑同款大屏即有）
                threading.Thread(target=self._fetch_node, daemon=True).start()

    def _fetch_node(self):
        if self._node_provider is not None:
            try:
                self.node = self._node_provider()
            except Exception:
                self.node = None
            return
        try:
            with urllib.request.urlopen(self.node_url, timeout=3) as r:
                j = json.loads(r.read().decode("utf-8", "replace"))
            if j.get("available"):
                self.node = j
            else:
                self.node = None
        except Exception:
            self.node = None

    @staticmethod
    def _friendly_error(e):
        """把抓取异常翻译成人话+排查建议，附原始错误短摘要。"""
        raw = ("%s: %s" % (type(e).__name__, e))[:110]
        try:
            if isinstance(e, urllib.error.HTTPError):
                c = e.code
                if c in (401, 403):
                    return "鉴权失败(HTTP %d)：/metrics 需要 API Key 或 Key 错误——⚙ 添加目标时填上 Key（原始: %s）" % (c, raw)
                if c == 404:
                    return "路径不存在(HTTP 404)：该地址没有 /metrics——确认填的是 vLLM 服务端口（原始: %s）" % raw
                return "HTTP %d（原始: %s）" % (c, raw)
            if isinstance(e, urllib.error.URLError):
                r = str(e.reason)
                if ("refused" in r or "Errno 111" in r or "10061" in r
                        or "积极拒绝" in r or "No connection could be made" in r):
                    return "连接被拒：vLLM 服务未启动或已退出，或端口不对（原始: %s）" % raw
                if "timed out" in r or "timeout" in r.lower():
                    return "连接超时：IP/端口错误或网络不通（原始: %s）" % raw
                if "getaddrinfo" in r or "Name or service" in r:
                    return "域名解析失败：主机名写错了（原始: %s）" % raw
                return "URL 错误：%s（原始: %s）" % (r, raw)
        except Exception:
            pass
        return raw

    def _fetch_meta(self):
        meta = {}
        try:
            req = urllib.request.Request(self.url + "/version")
            if self.api_key:
                req.add_header("Authorization", "Bearer " + self.api_key)
            with urllib.request.urlopen(req, timeout=4) as r:
                meta["version"] = r.read().decode("utf-8", "replace").strip()[:60]
        except Exception:
            pass
        try:
            req = urllib.request.Request(self.url + "/v1/models")
            if self.api_key:
                req.add_header("Authorization", "Bearer " + self.api_key)
            with urllib.request.urlopen(req, timeout=4) as r:
                j = json.loads(r.read().decode("utf-8", "replace"))
                meta["model_ids"] = [m.get("id") for m in j.get("data", []) if m.get("id")][:4]
        except Exception:
            pass
        if meta:
            self.meta_info = meta

    # ---------- 摄入与派生 ----------
    @staticmethod
    def _pick(simple, names):
        for n in names:
            if n in simple:
                return simple[n]
        return None

    def _ingest(self, simple, labeled, hist):
        now = time.time()
        # finish_reason 计数
        finished = {}
        for (name, lk), v in labeled.items():
            if name.endswith("request_success_total") or name.endswith("request_failure_total"):
                d = dict(lk)
                key = d.get("finish_reason") or d.get("error_type") or "other"
                finished[key] = finished.get(key, 0.0) + v
        # 带标签同名 gauge/counter（如 model_name）求和
        agg = dict(simple)
        for (name, lk), v in labeled.items():
            if name.endswith("request_success_total"):
                continue
            agg[name] = agg.get(name, 0.0) + v
        snap = {}
        for key, names in ALIAS.items():
            v = self._pick(agg, names)
            if v is not None and not math.isfinite(v):
                v = None
            snap[key] = v
        hsnap = {}
        for key, names in ALIAS_HIST.items():
            for n in names:
                if n in hist:
                    hsnap[key] = hist[n]
                    break
        self._finished = finished
        if snap.get("spec_accepted") is not None:
            self.has_spec = True
        if snap.get("pc_queries") is not None:
            self.has_prefix_cache = True
        if snap.get("kv_max_tokens"):
            self.kv_max_tokens = int(snap["kv_max_tokens"])

        with self.lock:
            self._t.append(now)
            self._raw.append(snap)
            self._hist.append(hsnap)
            self.n_scrapes += 1
            self._derive_latest(now)
            # 裁剪
            if len(self._t) > self.hist_max:
                cut = len(self._t) - self.hist_max
                del self._t[:cut]
                del self._raw[:cut]
                del self._hist[:cut]
                for k in SERIES_KEYS:
                    del self.series[k][:cut]

    def _idx_before(self, now, window):
        """返回不早于 now-window 的最旧点下标；不足窗口时用最旧可用点。"""
        lo = now - window
        for i, t in enumerate(self._t):
            if t >= lo:
                return i
        return 0

    @staticmethod
    def _rate(t0, t1, v0, v1):
        if v0 is None or v1 is None:
            return None
        if t1 - t0 < 1e-6:
            return None
        d = v1 - v0
        if d < 0:            # 计数器重启
            return None
        r = d / (t1 - t0)
        return r if math.isfinite(r) else None

    def _derive_latest(self, now):
        i = len(self._t) - 1
        t = self._t
        j_rate = self._idx_before(now, W_RATE)
        j_pct = self._idx_before(now, W_PCT)
        raw, hist = self._raw, self._hist
        cur = raw[i]
        s = {k: None for k in SERIES_KEYS}
        s["running"] = cur.get("running")
        s["waiting"] = cur.get("waiting")
        s["kv_perc"] = cur.get("kv_perc")
        s["gen_rate"] = self._rate(t[j_rate], t[i], raw[j_rate].get("gen_tokens"), cur.get("gen_tokens"))
        s["prompt_rate"] = self._rate(t[j_rate], t[i], raw[j_rate].get("prompt_tokens"), cur.get("prompt_tokens"))
        for key, p50_key, p95_key in (("ttft", "ttft_p50", "ttft_p95"),
                                      ("tpot", "tpot_p50", "tpot_p95")):
            h_now = hist[i].get(key)
            h_ref = hist[j_pct].get(key) if j_pct < len(hist) else None
            if h_now:
                delta = hist_delta(h_now["buckets"], h_ref["buckets"] if h_ref else None)
                s[p50_key] = hist_quantile(delta, 0.50)
                s[p95_key] = hist_quantile(delta, 0.95)
        for key, out in (("e2e", "e2e_p95"), ("queue", "queue_p95")):
            h_now = hist[i].get(key)
            h_ref = hist[j_pct].get(key) if j_pct < len(hist) else None
            if h_now:
                delta = hist_delta(h_now["buckets"], h_ref["buckets"] if h_ref else None)
                s[out] = hist_quantile(delta, 0.95)
        acc = self._rate(t[j_pct], t[i], raw[j_pct].get("spec_accepted"), cur.get("spec_accepted"))
        drf = self._rate(t[j_pct], t[i], raw[j_pct].get("spec_draft"), cur.get("spec_draft"))
        if acc is not None and drf is not None and drf > 0:
            s["spec_accept"] = min(1.0, acc / drf)
        hq = self._rate(t[j_pct], t[i], raw[j_pct].get("pc_hits"), cur.get("pc_hits"))
        qq = self._rate(t[j_pct], t[i], raw[j_pct].get("pc_queries"), cur.get("pc_queries"))
        if hq is not None and qq is not None and qq > 0:
            s["prefix_hit"] = min(1.0, hq / qq)
        s["gen_tokens_total"] = cur.get("gen_tokens")          # 累计值序列（各机对比图用；计数器重启会归零跳变，如实记录）
        s["prompt_tokens_total"] = cur.get("prompt_tokens")
        for k, v in s.items():
            self.series[k].append(v)
        self.last_ts = t[i]

    def now_brief(self):
        """最新指标简表（网关按引擎负载路由 / 负载均衡总表 / 全场总览用；无数据返回 None）。"""
        with self.lock:
            if not self._raw:
                return None
            cur = self._raw[-1]

            def last(k):
                for v in reversed(self.series[k]):
                    if v is not None:
                        return v
                return None
            return {"status": self.status, "running": cur.get("running"),
                    "waiting": cur.get("waiting"), "kv_perc": cur.get("kv_perc"),
                    "gen_rate": last("gen_rate"), "prompt_rate": last("prompt_rate"),
                    "prefix_hit": last("prefix_hit"), "spec_accept": last("spec_accept"),
                    "ttft_p95": last("ttft_p95"), "tpot_p95": last("tpot_p95"),
                    "gen_tokens_total": cur.get("gen_tokens"),
                    "prompt_tokens_total": cur.get("prompt_tokens"),
                    "preemptions": cur.get("preemptions"),
                    "finished": sum(self._finished.values()) if self._finished else 0}

    # ---------- 输出 ----------
    def snapshot(self, maxpoints=1200):
        with self.lock:
            n = len(self._t)
            stride = max(1, math.ceil(n / maxpoints))
            sl = slice(0, n, stride)
            series = {}
            t = list(self._t)[sl]
            for k in SERIES_KEYS:
                col = self.series[k]
                series[k] = [col[i] for i in range(0, n, stride)]
            cur = dict(self._raw[-1]) if self._raw else {}
            fin = dict(self._finished)
            payload = {
                "id": self.id, "name": self.name, "url": self.url,
                "status": self.status, "last_error": self.last_error,
                "last_ts": self.last_ts, "interval": self.interval,
                "monitor_since": self.monitor_since, "n_scrapes": self.n_scrapes,
                "has_spec": self.has_spec, "has_prefix_cache": self.has_prefix_cache,
                "kv_max_tokens": self.kv_max_tokens,
                "model_path": self.model_path, "context_len": self.context_len,
                "api_key": self.api_key, "meta_info": self.meta_info,
                "meta": {"points": n, "stride": stride},
                "series": {"t": t, **series},
            }
            # KPI 当前值
            def last(k):
                col = self.series[k]
                for v in reversed(col):
                    if v is not None:
                        return v
                return None
            payload["now"] = {
                "running": cur.get("running"), "waiting": cur.get("waiting"),
                "swapped": cur.get("swapped"),
                "gen_rate": last("gen_rate"), "prompt_rate": last("prompt_rate"),
                "kv_perc": cur.get("kv_perc"),
                "kv_used_tokens": cur.get("kv_used_tokens"),
                "ttft_p50": last("ttft_p50"), "ttft_p95": last("ttft_p95"),
                "tpot_p50": last("tpot_p50"), "tpot_p95": last("tpot_p95"),
                "e2e_p95": last("e2e_p95"), "queue_p95": last("queue_p95"),
                "spec_accept": last("spec_accept"), "prefix_hit": last("prefix_hit"),
                "preemptions": cur.get("preemptions"),
                "gen_tokens_total": cur.get("gen_tokens"),
                "prompt_tokens_total": cur.get("prompt_tokens"),
                "_fin_total": sum(fin.values()) or None,
                "_fin_stop": fin.get("stop"),
                "_fin_len": fin.get("length"),
            }
            return payload


# ============================== 宿主机资源采集（CPU/内存/GPU） ==============================

class NodeCollector:
    """采集大屏所在宿主机（部署在引擎机上=引擎宿主机）的 CPU/内存/GPU 实时使用率。
    Linux 读 /proc，GPU 走 nvidia-smi；缺失的源显示为不可用。"""

    def __init__(self, interval=3.0, hist_max=1200):
        import platform as _plat
        self.interval = interval
        self.hist_max = hist_max
        self.hostname = _plat.node() or socket.gethostname()
        self.platform = _plat.platform(terse=True)
        self.os_pretty = self._os_pretty() or self.platform
        self.kernel = "%s (%s)" % (_plat.release(), _plat.machine())
        self.cpu_name, self.cpu_cores = self._cpu_model()
        self.ip = self._outbound_ip()
        self.has_proc = os.path.exists("/proc/stat")
        self.has_gpu = self._gpu_ok()
        self.series = {"t": [], "cpu": [], "mem_perc": [], "gpu_util": [], "gpu_mem_perc": []}
        self.power_w = None         # 整机功率（ipmitool 后台采样；无权限/无工具=None）
        self.power_ts = 0.0
        self.per_gpu = []
        self._cpu_prev = None
        self._stop = threading.Event()
        if self.has_proc or self.has_gpu:
            self._prime()
            threading.Thread(target=self._loop, daemon=True).start()
        threading.Thread(target=self._power_loop, daemon=True).start()

    @property
    def available(self):
        return self.has_proc or self.has_gpu

    def _cpu_model(self):
        name, cores = "", 0
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name") and not name:
                        name = line.split(":", 1)[1].strip()
                    elif line.startswith("processor"):
                        cores += 1
        except Exception:
            pass
        return (name.replace("(R)", "").replace("(TM)", "").replace("  ", " "), cores)

    def _os_pretty(self):
        try:
            with open("/etc/os-release") as f:
                for line in f:
                    if line.startswith("PRETTY_NAME="):
                        return line.split("=", 1)[1].strip().strip('"')
        except Exception:
            pass
        return ""

    def _outbound_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return "127.0.0.1"
        finally:
            s.close()

    def _gpu_ok(self):
        try:
            r = subprocess.run(["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
                               capture_output=True, text=True, timeout=5)
            line = (r.stdout or "").strip().splitlines()
            self.gpu_name = line[0].strip() if line else ""
            return True
        except Exception:
            self.gpu_name = ""
            return False

    def _uptime(self):
        try:
            with open("/proc/uptime") as f:
                return float(f.read().split()[0])
        except Exception:
            return None

    def _cpu_snap(self):
        with open("/proc/stat") as f:
            vals = [float(x) for x in f.readline().split()[1:]]
        idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
        return sum(vals), idle

    def _mem(self):
        GB = 1024.0 ** 3
        info = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                info[k] = float(v.strip().split()[0]) * 1024.0
        total = info.get("MemTotal", 0.0)
        avail = info.get("MemAvailable", total)
        return total / GB, max(0.0, total - avail) / GB

    def _gpus(self):
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=index,utilization.gpu,memory.used,memory.total",
             "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=5)
        gpus = []
        for ln in (r.stdout or "").strip().splitlines():
            p = [x.strip() for x in ln.split(",")]
            if len(p) == 4:
                try:
                    gpus.append({"i": p[0], "util": float(p[1]),
                                 "mem_used": float(p[2]) / 1024.0, "mem_total": float(p[3]) / 1024.0})
                except ValueError:
                    pass
        return gpus

    def _prime(self):
        if self.has_proc:
            self._cpu_prev = self._cpu_snap()
        if self.has_gpu:
            self.per_gpu = self._gpus()

    def _sample(self):
        now = time.time()
        cpu = mem_perc = None
        mem_used = mem_total = 0.0
        if self.has_proc:
            total, idle = self._cpu_snap()
            if self._cpu_prev:
                dt = total - self._cpu_prev[0]
                di = idle - self._cpu_prev[1]
                if dt > 0:
                    cpu = max(0.0, min(100.0, (dt - di) / dt * 100.0))
            self._cpu_prev = (total, idle)
            mem_total, mem_used = self._mem()
            if mem_total > 0:
                mem_perc = mem_used / mem_total * 100.0
        gpu_util = gpu_mem_perc = None
        if self.has_gpu:
            self.per_gpu = self._gpus()
            if self.per_gpu:
                gpu_util = sum(g["util"] for g in self.per_gpu) / len(self.per_gpu)
                mu = sum(g["mem_used"] for g in self.per_gpu)
                mt = sum(g["mem_total"] for g in self.per_gpu)
                gpu_mem_perc = mu / mt * 100.0 if mt > 0 else None
                self._last_gpu_mem = (mu, mt)
        with _NODE_LOCK:
            for k, v in (("cpu", cpu), ("mem_perc", mem_perc),
                         ("gpu_util", gpu_util), ("gpu_mem_perc", gpu_mem_perc)):
                self.series[k].append(v)
            self.series["t"].append(now)
            for k in self.series:
                if len(self.series[k]) > self.hist_max:
                    del self.series[k][:len(self.series[k]) - self.hist_max]
            self._last = {"cpu": cpu, "mem_perc": mem_perc, "mem_used": mem_used,
                          "mem_total": mem_total, "gpu_util": gpu_util,
                          "gpu_mem_perc": gpu_mem_perc,
                          "gpu_mem_used": getattr(self, "_last_gpu_mem", (0, 0))[0],
                          "gpu_mem_total": getattr(self, "_last_gpu_mem", (0, 0))[1]}

    def _power_loop(self):
        """整机功率后台采样：ipmitool dcmi 优先、sdr 兜底，优先取非零值；10s 一轮（子进程慢，独立线程不卡主循环）。"""
        while not self._stop.wait(10):
            try:
                w = self._sample_power()
                if w is not None:
                    self.power_w = w
                    self.power_ts = time.time()
            except Exception:
                pass

    @staticmethod
    def _sample_power():
        """整机功率读取链：dcmi 直读 → sensor list 按 PSU PIN 求和（整机输入功率）→ POUT 求和 → 单功率传感器。"""
        # 1) dcmi 直读（最准，部分 BMC 支持）
        try:
            r = subprocess.run(["ipmitool", "dcmi", "power reading"], capture_output=True, timeout=8)
            m = re.search(r"Instantaneous power reading:\s*([0-9]+(?:\.[0-9]+)?)", r.stdout.decode("utf-8", "replace"))
            if m and float(m.group(1)) > 0:
                return float(m.group(1))
        except Exception:
            pass
        # 2) sensor list / sdr elist 瓦特行聚合
        for args in (["sensor", "list"], ["sdr", "elist"]):
            pins, pouts, others = [], [], []
            try:
                r = subprocess.run(["ipmitool"] + args, capture_output=True, timeout=8)
                for ln in r.stdout.decode("utf-8", "replace").splitlines():
                    parts = [x.strip() for x in ln.split("|")]
                    if len(parts) < 3 or not parts[2].lower().startswith("watt"):
                        continue
                    try:
                        v = float(parts[1])
                    except ValueError:
                        continue
                    nm = parts[0].upper()
                    if "PIN" in nm:
                        pins.append(v)
                    elif "POUT" in nm:
                        pouts.append(v)
                    else:
                        others.append(v)
                if pins:
                    return sum(pins)      # 各 PSU 输入功率之和 = 整机输入功率
                if pouts:
                    return sum(pouts)
                if others:
                    return max(others)   # 单一整机功率传感器
            except Exception:
                continue
        return None

    def _loop(self):
        while not self._stop.wait(self.interval):
            try:
                self._sample()
            except Exception:
                pass

    def snapshot(self):
        s = {k: list(v)[-600:] for k, v in self.series.items()}
        return {"available": self.available, "hostname": self.hostname, "ip": self.ip,
                "platform": self.platform, "os_pretty": getattr(self, "os_pretty", ""),
                "kernel": getattr(self, "kernel", ""),
                "cpu_name": getattr(self, "cpu_name", ""),
                "cpu_cores": getattr(self, "cpu_cores", 0), "time": time.time(),
                "has_proc": self.has_proc, "has_gpu": self.has_gpu,
                "gpu_name": getattr(self, "gpu_name", ""), "uptime_s": self._uptime(),
                "power_w": self.power_w, "power_ts": self.power_ts,
                "now": getattr(self, "_last", {}), "per_gpu": self.per_gpu,
                "series": s}


_NODE_LOCK = threading.Lock()


# ============================== 目标注册表（持久化） ==============================

class Registry:
    def __init__(self, path):
        self.path = path
        self.targets = []
        self.lock = threading.Lock()
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for it in json.load(f).get("targets", []):
                        self._add(it["name"], it["url"], it.get("api_key", ""), start=True,
                                  model_path=it.get("model_path", ""), context_len=it.get("context_len", ""))
            except Exception:
                pass

    def _add(self, name, url, api_key="", start=True, transient=False, model_path="", context_len=""):
        t = Target(name, url, api_key, model_path=model_path, context_len=context_len)
        t.transient = transient
        if start:
            t.thread.start()
        with self.lock:
            self.targets.append(t)
        return t

    def add(self, name, url, api_key="", transient=False, model_path="", context_len=""):
        t = self._add(name, url, api_key, transient=transient,
                      model_path=model_path, context_len=context_len)
        self.save()
        return t

    def remove(self, tid):
        with self.lock:
            keep = []
            for t in self.targets:
                if t.id == tid:
                    t._stop.set()
                else:
                    keep.append(t)
            self.targets = keep
        self.save()

    def get(self, tid):
        for t in self.targets:
            if t.id == tid:
                return t
        return None

    def save(self):
        if not self.path:
            return
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"targets": [{"name": t.name, "url": t.url, "api_key": t.api_key,
                                        "model_path": t.model_path, "context_len": t.context_len}
                                       for t in self.targets if not getattr(t, "transient", False)]},
                          f, ensure_ascii=False, indent=1)
        except Exception:
            pass


# ============================== 内置演示数据源（--demo） ==============================

class DemoEngine:
    """仿真 vLLM /metrics：周期性压测负载（空闲→爬坡→c32 满载→排空），数值口径贴近 96G PRO6000 实测。"""

    EDGES = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60]

    def __init__(self, phase_shift=0.0):
        self.t0 = time.time() - phase_shift
        self.last = self.t0
        self.reqs = []          # [ctx_in, gen, out_target, speed, done]
        self.backlog = 0
        self.gen_total = 0.0
        self.prompt_total = 0.0
        self.fin = {"stop": 0, "length": 0}
        self.pc_q, self.pc_h = 0.0, 0.0
        self.draft_total, self.acc_total = 0.0, 0.0
        self.preempt = 0.0
        self.ttft_s, self.tpot_s, self.e2e_s, self.q_s = [], [], [], []
        self.kv_used, self.kv_max = 20000, 2_600_000
        self._advance(time.time(), force=True)

    def _phase(self, el):
        c = el % 150.0
        if c < 12:
            return ("idle", 0.0)
        if c < 25:
            return ("ramp", (c - 12) / 13.0)
        if c < 105:
            return ("load", 1.0)
        return ("drain", 0.0)

    def _advance(self, now, force=False):
        dt = min(10.0, now - self.last) if not force else 0.0
        if dt <= 0:
            return
        self.last = now
        el = now - self.t0
        ph, k = self._phase(el)
        # 新请求到达（1K/4K/16K 混合输入）
        arr_rate = 0.0
        if ph == "ramp":
            arr_rate = 8.0 * k
        elif ph == "load":
            arr_rate = 0.3
        arrivals = max(0, int(random.gauss(arr_rate * dt, 1.2)))
        for _ in range(arrivals):
            r = random.random()
            ctx = 1024 if r < 0.6 else (4096 if r < 0.9 else 16384)
            self.backlog += 1
            self.prompt_total += ctx
            self.pc_q += 1
            if random.random() < 0.45:
                self.pc_h += 1
            ttft = random.lognormvariate(math.log(0.035 + 0.004 * self.backlog), 0.4)
            self.ttft_s.append((now, ttft))
            self.q_s.append((now, max(0.0, random.gauss(0.002 + 0.02 * self.backlog, 0.004))))
        # 调度
        while self.backlog > 0 and len(self.reqs) < 32:
            ctx = random.choice([1024] * 6 + [4096] * 3 + [16384])
            self.reqs.append([ctx, 0, random.randint(700, 1200) if ctx == 1024 else random.randint(600, 1500), 0, now])
            self.backlog -= 1
        n = len(self.reqs)
        # 单流解码速度：c1 ~90 tok/s → c32 ~27 tok/s
        base = 90.0 if n <= 2 else max(26.0, 90.0 - (n - 2) * (90.0 - 26.0) / 30.0)
        gen_dt = 0.0
        for r in self.reqs:
            if r[3] == 0:
                r[3] = max(8.0, random.gauss(base, base * 0.12))
            g = r[3] * dt
            r[1] += g
            gen_dt += g
            self.tpot_s.append((now, 1.0 / r[3]))
        self.gen_total += gen_dt
        # 投机解码 k=4，接受率 ~0.55
        steps = gen_dt / 3.2
        self.draft_total += steps * 4.0
        self.acc_total += steps * 4.0 * max(0.3, min(0.95, random.gauss(0.55, 0.05)))
        # 完成
        done = [r for r in self.reqs if r[1] >= r[2]]
        for r in done:
            self.fin["length" if random.random() < 0.8 else "stop"] += 1
            self.e2e_s.append((now, now - r[4]))
        if done:
            self.reqs = [r for r in self.reqs if r[1] < r[2]]
        # KV
        KV_MAX = 2_600_000
        used = sum(r[0] + r[1] for r in self.reqs) + 20000 * self.backlog
        if used > KV_MAX * 0.95:
            self.preempt += 1
            if self.reqs:
                self.reqs.pop(random.randrange(len(self.reqs)))
        self.kv_used, self.kv_max = used, KV_MAX
        # 样本裁剪
        for lst in (self.ttft_s, self.tpot_s, self.e2e_s, self.q_s):
            if len(lst) > 20000:
                del lst[: len(lst) - 12000]

    def _hist_lines(self, prefix, samples, now):
        vals = [v for t, v in samples if now - t <= 600]
        out = ["# TYPE %s histogram" % prefix]
        for e in self.EDGES:
            c = sum(1 for v in vals if v <= e)
            out.append('%s_bucket{le="%s"} %d' % (prefix, ("+Inf" if math.isinf(e) else str(e)), c))
        out.append('%s_bucket{le="+Inf"} %d' % (prefix, len(vals)))
        out.append("%s_sum %f" % (prefix, sum(vals)))
        out.append("%s_count %d" % (prefix, len(vals)))
        return out

    def render(self):
        now = time.time()
        self._advance(now)
        n = len(self.reqs)
        L = ["# HELP vllm:num_requests_running Requests currently running on the GPU.",
             "# TYPE vllm:num_requests_running gauge",
             "vllm:num_requests_running %d" % n,
             "# TYPE vllm:num_requests_waiting gauge",
             "vllm:num_requests_waiting %d" % self.backlog,
             "# TYPE vllm:num_requests_swapped gauge",
             "vllm:num_requests_swapped 0",
             "# TYPE vllm:kv_cache_usage_perc gauge",
             'vllm:kv_cache_usage_perc{model_name="demo",engine="0"} %.6f' % (self.kv_used / self.kv_max),
             "# TYPE vllm:generation_tokens_total counter",
             "vllm:generation_tokens_total %.0f" % self.gen_total,
             "# TYPE vllm:prompt_tokens_total counter",
             "vllm:prompt_tokens_total %.0f" % self.prompt_total,
             "# TYPE vllm:num_preemptions_total counter",
             "vllm:num_preemptions_total %.0f" % self.preempt,
             '# TYPE vllm:request_success_total counter',
             'vllm:request_success_total{finish_reason="stop"} %.0f' % self.fin["stop"],
             'vllm:request_success_total{finish_reason="length"} %.0f' % self.fin["length"],
             "# TYPE vllm:spec_decode_num_accepted_tokens_total counter",
             "vllm:spec_decode_num_accepted_tokens_total %.0f" % self.acc_total,
             "# TYPE vllm:spec_decode_num_draft_tokens_total counter",
             "vllm:spec_decode_num_draft_tokens_total %.0f" % self.draft_total,
             "# TYPE vllm:prefix_cache_queries_total counter",
             "vllm:prefix_cache_queries_total %.0f" % self.pc_q,
             "# TYPE vllm:prefix_cache_hits_total counter",
             "vllm:prefix_cache_hits_total %.0f" % self.pc_h]
        L += self._hist_lines("vllm:time_to_first_token_seconds", self.ttft_s, now)
        L += self._hist_lines("vllm:request_time_per_output_token_seconds", self.tpot_s, now)
        L += self._hist_lines("vllm:e2e_request_latency_seconds", self.e2e_s, now)
        L += self._hist_lines("vllm:request_queue_time_seconds", self.q_s, now)
        return "\n".join(L) + "\n"


# ============================== 内置轻量网关（API 分发） ==============================

GW_CONNECT_TIMEOUT = 15.0
GW_STREAM_IDLE_TIMEOUT = 300.0    # 流式两 chunk 间空闲上限（长思考不误杀）
GW_REQ_TIMEOUT = 600.0
GW_BREAK_NFAIL = 3                # 连接类连续失败 N 次 → 熔断
GW_BREAK_SECONDS = 60.0
GW_PROBE_INTERVAL = 10.0          # 渠道主动探活间隔（GET /health）
GW_PROBE_TIMEOUT = 3.0
GW_LB_POLICIES = ("weight", "round_robin", "p2c", "least_requests", "session")
GW_AFF_TTL = 1800.0      # 会话亲和记录空闲过期（秒）
GW_AFF_MAX = 20000       # 会话表上限（超过先清过期）


class Gateway:
    """API 分发网关：渠道 + Key 管理 + 限流记账，线程安全，持久化 gateway.json（含用量）。"""

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.enabled = True            # 转发开关（面板可暂停）
        self.data = {"channels": [], "keys": []}
        self.rpm_win = {}
        self.chan = {}
        self.reqwin = {}               # 渠道 → 请求时间窗（QPS 统计）
        self.aff = {}                  # 会话亲和表：md5(对话前缀) → {"ch": 渠道名, "ts": 最近使用}
        self.metrics_fn = None         # base_url → 引擎指标简表（main 注入；指标感知路由/总表用）
        self._rr = 0                   # round_robin 轮询计数
        self.totals = {"requests": 0, "ok": 0, "fail": 0, "tokens": 0}
        self.started = time.time()
        self._load()
        threading.Thread(target=self._flush_loop, daemon=True).start()
        threading.Thread(target=self._probe_loop, daemon=True).start()

    def _load(self):
        if self.path and os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                pass
        self.data.setdefault("channels", [])
        self.data.setdefault("keys", [])
        self.data.setdefault("policy", "weight")
        if self.data.get("policy") not in GW_LB_POLICIES:
            self.data["policy"] = "weight"
        for c in self.data["channels"]:
            self.chan[c["name"]] = self._blank_chan()

    @staticmethod
    def _blank_chan():
        return {"requests": 0, "ok": 0, "fail": 0, "tokens": 0,
                "lat_ms": 0.0, "consec": 0, "broken_until": 0.0,
                "inflight": 0, "probe_ok": None, "probe_ms": 0.0, "probe_ts": 0.0}

    def _flush_loop(self):
        while True:
            time.sleep(30)
            try:
                self.save()
            except Exception:
                pass

    def save(self):
        if not self.path:
            return
        with self.lock:
            try:
                d = os.path.dirname(self.path)
                if d:
                    os.makedirs(d, exist_ok=True)
                with open(self.path, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=1)
            except Exception:
                pass

    # ---------- Key ----------
    def gen_key(self, name, rpm=0, quota_tokens=0):
        with self.lock:
            k = "sk-" + secrets.token_hex(12)
            self.data["keys"].append({
                "key": k, "name": (name or "").strip() or "unnamed",
                "rpm": int(rpm or 0), "quota_tokens": int(quota_tokens or 0),
                "used_tokens": 0, "used_requests": 0, "enabled": True,
                "created": time.time()})
            self.save()
            return k

    def get_key(self, key):
        for k in self.data["keys"]:
            if k["key"] == key:
                return k
        return None

    def key_op(self, key, op, rpm=None, quota_tokens=None):
        with self.lock:
            for k in self.data["keys"]:
                if k["key"] == key:
                    if op == "toggle":
                        k["enabled"] = not k["enabled"]
                    elif op == "delete":
                        self.data["keys"].remove(k)
                    elif op == "quota":
                        if rpm is not None:
                            k["rpm"] = int(rpm or 0)
                        if quota_tokens is not None:
                            k["quota_tokens"] = int(quota_tokens or 0)
                    self.save()
                    return True
            return False

    # ---------- 渠道 ----------
    def add_channel(self, name, base_url, api_key="", priority=1, weight=10):
        with self.lock:
            name = (name or "").strip()
            if not name or not base_url or any(c["name"] == name for c in self.data["channels"]):
                return False
            self.data["channels"].append({
                "name": name, "base_url": base_url.rstrip("/"), "api_key": api_key or "",
                "priority": int(priority or 1), "weight": max(1, int(weight or 10)),
                "enabled": True})
            self.chan[name] = self._blank_chan()
            self.save()
            return True

    def channel_op(self, name, op):
        with self.lock:
            for c in self.data["channels"]:
                if c["name"] == name:
                    if op == "toggle":
                        c["enabled"] = not c["enabled"]
                    elif op == "delete":
                        self.data["channels"].remove(c)
                        self.chan.pop(name, None)
                    self.save()
                    return True
            return False

    def set_policy(self, policy):
        with self.lock:
            if policy not in GW_LB_POLICIES:
                return False
            self.data["policy"] = policy
            self.save()
            return True

    def _probe_loop(self):
        """渠道主动探活：GET /health；有 HTTP 应答即算在线（404 也算进程活着），连接失败才判死。"""
        while True:
            time.sleep(GW_PROBE_INTERVAL)
            for c in list(self.data.get("channels", [])):
                if not c.get("enabled", True):
                    continue
                t0 = time.time()
                try:
                    urllib.request.urlopen(c["base_url"] + "/health", timeout=GW_PROBE_TIMEOUT)
                    ok = True
                except urllib.error.HTTPError:
                    ok = True
                except Exception:
                    ok = False
                with self.lock:
                    st = self.chan.setdefault(c["name"], self._blank_chan())
                    st["probe_ok"] = ok
                    st["probe_ms"] = round((time.time() - t0) * 1000, 1)
                    st["probe_ts"] = time.time()

    # ---------- 在途请求计数（P2C/最少负载策略与总表用） ----------
    def chan_begin(self, name):
        with self.lock:
            self.chan.setdefault(name, self._blank_chan())["inflight"] = \
                self.chan[name].get("inflight", 0) + 1

    def chan_end(self, name):
        with self.lock:
            st = self.chan.get(name)
            if st:
                st["inflight"] = max(0, st.get("inflight", 0) - 1)

    def pick_channel(self, exclude=(), hint=None):
        """按负载均衡策略选渠道：同优先级层内分流，跨层按优先级（低者优先，留给故障转移）。
        策略：weight=加权随机（默认） / round_robin=轮询 / p2c=随机抽二选在途少 /
        least_requests=选引擎运行请求最少（监控指标感知，取不到用在途数） /
        session=会话亲和（同对话粘同机保前缀缓存；新会话分给粘性会话数最少的机器，防挤一台）。"""
        now = time.time()
        with self.lock:
            cands = [c for c in self.data["channels"]
                     if c.get("enabled", True) and c["name"] not in exclude
                     and self.chan.get(c["name"], {}).get("broken_until", 0) <= now]
            pol = self.data.get("policy", "weight")
        if not cands:
            return None
        alive = [c for c in cands if self.chan.get(c["name"], {}).get("probe_ok", True)]
        if alive:
            cands = alive
        cands.sort(key=lambda c: c.get("priority", 1))
        p0 = cands[0].get("priority", 1)
        same = [c for c in cands if c.get("priority", 1) == p0]
        if len(same) == 1:
            return same[0]

        def inflight(c):
            return self.chan.get(c["name"], {}).get("inflight", 0)

        if pol == "session":
            if hint:
                h = hashlib.md5(hint.encode("utf-8", "replace")).hexdigest()
                with self.lock:
                    rec = self.aff.get(h)
                    if rec and rec["ch"] in {c["name"] for c in same}:
                        rec["ts"] = now
                        for c in same:
                            if c["name"] == rec["ch"]:
                                return c
                    # 新会话（或原机已不可用）→ 分给粘性会话数最少的机器，并列加权随机
                    if len(self.aff) > GW_AFF_MAX:
                        for k in [k for k, v in self.aff.items() if now - v["ts"] > GW_AFF_TTL]:
                            del self.aff[k]
                    cnt = {c["name"]: 0 for c in same}
                    for v in self.aff.values():
                        if now - v["ts"] <= GW_AFF_TTL and v["ch"] in cnt:
                            cnt[v["ch"]] += 1
                    m0 = min(cnt.values())
                    pool = [c for c in same if cnt[c["name"]] == m0]
                    tw = sum(c.get("weight", 10) for c in pool)
                    r = random.uniform(0, tw)
                    acc = 0
                    pick = pool[-1]
                    for c in pool:
                        acc += c.get("weight", 10)
                        if r <= acc:
                            pick = c
                            break
                    self.aff[h] = {"ch": pick["name"], "ts": now}
                    return pick
            # 无请求体（GET 等）→ 按加权随机
        if pol == "round_robin":
            with self.lock:
                self._rr = (self._rr + 1) % len(same)
                return same[self._rr]
        if pol in ("p2c", "least_requests"):
            def load(c):
                if pol == "least_requests" and self.metrics_fn:
                    m = None
                    try:
                        m = self.metrics_fn(c["base_url"])
                    except Exception:
                        pass
                    if m and m.get("running") is not None:
                        return (m["running"], inflight(c))
                    return (100000 + inflight(c), 0)   # 引擎指标未知 → 用在途数兜底排序
                return (inflight(c), 0)
            if pol == "p2c" and len(same) >= 2:
                a, b = random.sample(same, 2)
                return a if load(a) <= load(b) else b
            return min(same, key=load)
        # weight（默认）：同层加权随机
        tw = sum(c.get("weight", 10) for c in same)
        r = random.uniform(0, tw)
        acc = 0
        for c in same:
            acc += c.get("weight", 10)
            if r <= acc:
                return c
        return same[-1]

    # ---------- 鉴权限流 ----------
    def check_key(self, key):
        with self.lock:
            k = self.get_key(key)
            if k is None:
                return (401, "无效的 API Key")
            if not k.get("enabled", True):
                return (403, "该 Key 已被停用，请联系管理员")
            q = k.get("quota_tokens", 0)
            if q and k.get("used_tokens", 0) >= q:
                return (429, "token 配额已用尽（配额 %d）" % q)
            rpm = k.get("rpm", 0)
            if rpm:
                now = time.time()
                w = self.rpm_win.setdefault(key, deque())
                while w and w[0] < now - 60:
                    w.popleft()
                if len(w) >= rpm:
                    return (429, "超过 RPM 限额（%d 次/分钟）" % rpm)
                w.append(now)
            return None

    # ---------- 记账 ----------
    def chan_hit(self, name, ok, lat_ms=0.0, tokens=0, fail_pre=False):
        now = time.time()
        with self.lock:
            st = self.chan.setdefault(name, self._blank_chan())
            w = self.reqwin.setdefault(name, deque())
            w.append(now)
            while w and w[0] < now - 60:
                w.popleft()
            self.totals["requests"] += 1
            st["requests"] += 1
            if ok:
                self.totals["ok"] += 1
                st["ok"] += 1
                st["lat_ms"] += lat_ms
                if tokens:
                    self.totals["tokens"] += tokens
                    st["tokens"] += tokens
                st["consec"] = 0
                st["broken_until"] = 0.0
            else:
                self.totals["fail"] += 1
                st["fail"] += 1
                if fail_pre:
                    st["consec"] += 1
                    if st["consec"] >= GW_BREAK_NFAIL:
                        st["broken_until"] = time.time() + GW_BREAK_SECONDS

    def account_key(self, key, tokens):
        with self.lock:
            k = self.get_key(key)
            if k:
                k["used_requests"] = k.get("used_requests", 0) + 1
                k["used_tokens"] = k.get("used_tokens", 0) + (tokens or 0)

    def snapshot(self, port):
        now = time.time()
        with self.lock:
            chans = []
            tot_qps = 0
            tot_run = tot_dec = 0
            n_eng = 0
            for c in self.data["channels"]:
                st = self.chan.get(c["name"], self._blank_chan())
                bu = st.get("broken_until", 0)
                status = "paused" if not c.get("enabled", True) else ("broken" if bu > now else "ok")
                avg = (st.get("lat_ms", 0) / st["ok"]) if st.get("ok") else 0
                w = self.reqwin.get(c["name"])
                qps = len(w) if w else 0
                eng = None
                if self.metrics_fn:
                    try:
                        eng = self.metrics_fn(c["base_url"])
                    except Exception:
                        eng = None
                tot_qps += qps
                if eng and eng.get("running") is not None:
                    tot_run += eng["running"] or 0
                    tot_dec += eng.get("gen_rate") or 0
                    n_eng += 1
                chans.append({"name": c["name"], "base_url": c["base_url"],
                              "priority": c.get("priority", 1), "weight": c.get("weight", 10),
                              "status": status, "broken_until": round(bu - now) if bu > now else 0,
                              "requests": st.get("requests", 0), "ok": st.get("ok", 0),
                              "fail": st.get("fail", 0), "tokens": st.get("tokens", 0),
                              "avg_latency_ms": round(avg, 1),
                              "inflight": st.get("inflight", 0),
                              "probe_ok": st.get("probe_ok"), "probe_ms": st.get("probe_ms", 0),
                              "qps_1m": qps, "engine": eng})
            return {"enabled": self.enabled, "port": port, "since": self.started,
                    "policy": self.data.get("policy", "weight"),
                    "totals": dict(self.totals), "channels": chans,
                    "qps_1m": tot_qps,
                    "eng_running": tot_run if n_eng else None,
                    "eng_gen_rate": round(tot_dec, 1) if n_eng else None,
                    "keys": [dict(k) for k in self.data["keys"]]}


class GatewayHandler(BaseHTTPRequestHandler):
    gw = None          # type: Gateway
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def _api_err(self, code, msg, etype="invalid_request_error"):
        self._json(code, {"error": {"message": msg, "type": etype, "code": code}})

    def _client_key(self):
        h = self.headers.get("Authorization", "")
        if h.startswith("Bearer "):
            return h[7:].strip()
        return self.headers.get("x-api-key", "").strip()

    @staticmethod
    def _usage_from_json(body):
        try:
            u = (json.loads(body.decode("utf-8", "replace")).get("usage")) or {}
            return int(u.get("prompt_tokens", 0)) + int(u.get("completion_tokens", 0))
        except Exception:
            return 0

    @staticmethod
    def _usage_from_sse_tail(tail):
        try:
            for ln in reversed(tail.decode("utf-8", "replace").splitlines()):
                ln = ln.strip()
                if ln.startswith("data:") and "[DONE]" not in ln and '"usage"' in ln:
                    u = (json.loads(ln[5:].strip()).get("usage")) or {}
                    return int(u.get("prompt_tokens", 0)) + int(u.get("completion_tokens", 0))
        except Exception:
            pass
        return 0

    def do_GET(self):
        if self.path.startswith("/gw/health"):
            g = self.gw
            self._json(200, {"ok": True, "enabled": g.enabled,
                             "channels": len(g.data["channels"]), "keys": len(g.data["keys"])})
        elif self.path.startswith("/gw/v1/"):
            self._proxy("GET")
        else:
            self._api_err(404, "not found")

    def do_POST(self):
        if self.path.startswith("/gw/v1/"):
            self._proxy("POST")
        else:
            self._api_err(404, "not found")

    def _proxy(self, method):
        gw = self.gw
        if not gw.enabled:
            self._api_err(503, "网关转发已暂停（管理面板可开启）", "server_error")
            return
        key = self._client_key()
        if not key:
            self._api_err(401, "缺少 API Key：请带 Authorization: Bearer sk-...")
            return
        err = gw.check_key(key)
        if err:
            self._api_err(err[0], err[1],
                          "insufficient_quota" if err[0] == 429 else "invalid_request_error")
            return
        body = b""
        if method == "POST":
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n) if n else b""
        is_stream = False
        aff_key = None
        if method == "POST" and body[:1] == b"{":
            try:
                j = json.loads(body.decode("utf-8", "replace"))
                is_stream = bool(j.get("stream"))
                # 会话亲和键：对话首条消息（system/首条 user）前 1000 字——同会话各轮此键不变
                msgs = j.get("messages")
                if isinstance(msgs, list) and msgs and isinstance(msgs[0], dict):
                    c0 = msgs[0].get("content")
                    if c0:
                        aff_key = str(c0)[:1000]
                if not aff_key and j.get("prompt"):
                    aff_key = str(j["prompt"])[:1000]
                if is_stream:
                    so = j.setdefault("stream_options", {})
                    if not isinstance(so, dict):
                        so = {}
                        j["stream_options"] = so
                    so.setdefault("include_usage", True)
                    body = json.dumps(j, ensure_ascii=False).encode("utf-8")
            except Exception:
                pass
        with gw.lock:
            nch = max(1, len(gw.data["channels"]))
        tried = 0
        tried_names = []
        last_err = "无可用渠道"
        while tried < nch:
            tried += 1
            ch = gw.pick_channel(exclude=tried_names, hint=aff_key)
            if ch is None:
                break
            tried_names.append(ch["name"])
            gw.chan_begin(ch["name"])
            try:
                t0 = time.time()
                req = urllib.request.Request(ch["base_url"] + self.path[len("/gw"):],
                                             data=body if method == "POST" else None,
                                             method=method)
                req.add_header("Content-Type", "application/json")
                if ch.get("api_key"):
                    req.add_header("Authorization", "Bearer " + ch["api_key"])
                try:
                    resp = urllib.request.urlopen(
                        req, timeout=GW_STREAM_IDLE_TIMEOUT if is_stream else GW_REQ_TIMEOUT)
                except urllib.error.HTTPError as e:
                    ebody = b""
                    try:
                        ebody = e.read()
                    except Exception:
                        pass
                    if e.code >= 502 and tried < nch:
                        gw.chan_hit(ch["name"], False, fail_pre=True)
                        last_err = "上游 HTTP %d" % e.code
                        continue
                    gw.chan_hit(ch["name"], False)
                    self._passthrough(e.code, ebody,
                                      e.headers.get("Content-Type", "application/json"))
                    return
                except Exception as e:
                    gw.chan_hit(ch["name"], False, fail_pre=True)
                    last_err = "%s: %s" % (type(e).__name__, e)
                    continue
                try:
                    if is_stream:
                        self._relay_stream(resp, key, ch["name"], t0)
                    else:
                        self._relay_plain(resp, key, ch["name"], t0)
                except (BrokenPipeError, ConnectionResetError):
                    gw.chan_hit(ch["name"], True, (time.time() - t0) * 1000)
                    self.close_connection = True
                    return
                return
            finally:
                gw.chan_end(ch["name"])
        self._api_err(502, "所有渠道不可用：%s" % last_err, "server_error")

    def _passthrough(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype or "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)
        self.close_connection = True

    def _relay_plain(self, resp, key, name, t0):
        body = resp.read()
        tokens = self._usage_from_json(body)
        self._passthrough(resp.status, body, resp.headers.get("Content-Type", "application/json"))
        self.gw.chan_hit(name, True, (time.time() - t0) * 1000, tokens)
        self.gw.account_key(key, tokens)

    def _relay_stream(self, resp, key, name, t0):
        self.send_response(resp.status)
        self.send_header("Content-Type", resp.headers.get("Content-Type", "text/event-stream"))
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        tail = b""
        try:
            while True:
                chunk = resp.read(8192)
                if not chunk:
                    break
                self.wfile.write(chunk)
                try:
                    self.wfile.flush()
                except Exception:
                    raise BrokenPipeError()
                tail = (tail + chunk)[-32768:]
        except socket.timeout:
            pass    # 上游空闲超时：按流结束处理（已发出的数据保留）
        tokens = self._usage_from_sse_tail(tail)
        self.close_connection = True
        self.gw.chan_hit(name, True, (time.time() - t0) * 1000, tokens)
        self.gw.account_key(key, tokens)

# ============================== Web 服务 ==============================

HTML = None  # 见文件底部 _build_html()


CONFIG_EXAMPLE = {
    "targets": [
        {"name": "svc-a", "url": "http://10.0.0.1:8123", "api_key": "",
         "model_path": "/data/models/Your-Model-A", "context_len": "1048576"},
        {"name": "svc-b", "url": "http://10.0.0.2:8123", "api_key": "",
         "model_path": "/data/models/Your-Model-B", "context_len": "1048576"},
    ],
    "gateway": {
        "policy": "round_robin",
        "channels": [
            {"name": "a", "url": "http://10.0.0.1:8123", "api_key": "", "priority": 1, "weight": 10},
            {"name": "b", "url": "http://10.0.0.2:8123", "api_key": "", "priority": 1, "weight": 10},
        ],
    },
}

AGENT_MD = """# ai-dashboard 配置编写指南（给 AI agent）

大屏（AI 推理性能监控大屏）支持一键导入一份 JSON 配置，完成监控目标与网关渠道的初始化。
入口：页面 ⚙ 菜单 →「⬆ 导入配置」，或 POST /api/config/import（body: {"config": <下方结构>}）。

## 配置结构

```json
{
  "targets": [
    {"name": "svc-a", "url": "http://10.0.0.1:8123", "api_key": "",
     "model_path": "/data/models/Your-Model-A", "context_len": "1048576"}
  ],
  "gateway": {
    "policy": "round_robin",
    "channels": [
      {"name": "a", "url": "http://10.0.0.1:8123", "api_key": "", "priority": 1, "weight": 10}
    ]
  }
}
```

## 字段说明

- targets[]（监控目标，即每个 AI 推理服务）
  - name：显示名（页签/AI服务详情/服务器列表里的名字），必填、唯一
  - url：vLLM 服务根地址（OpenAI 端口；大屏自动拼 /metrics 抓指标），必填，http(s)://host:port
  - api_key：该服务 /metrics 需要 Bearer 时填（vLLM 的 /metrics 通常匿名，留空即可）
  - model_path：权重目录完整路径——**建议填**：AI服务详情表的「模型」列显示的是该路径的最后一段全名
  - context_len：上下文长度（显示用，字符串或数字）
- gateway（API 分发网关，--gw 启动时生效）
  - policy：负载均衡策略，五选一：weight（加权随机）/ round_robin（轮询）/ p2c（二选一）/ least_requests（最少负载）/ session（会话亲和，多轮对话保前缀缓存，推荐对话类负载）
  - channels[]：上游渠道（即各推理服务）
    - name：渠道名，必填、唯一；url：同 targets.url；api_key：转发时携带的上游 Bearer（无鉴权留空）
    - priority：数字越小越优先；同优先级内按 policy 分流，高优先级整体不可用时自动落到低优先级（灾备用）
    - weight：同优先级内的权重比例（weight 策略下生效）

## 语义与注意

- 导入为**整体替换**：现有 targets 与 channels 会被清空后按本文件重建；网关已生成的客户端 Key（sk-…）不受影响
- secrets 口径：targets/channels 的 api_key 会随「导出配置」一并导出（内网服务凭据）；客户端 Key 永不导出
- 同机多服务：每台引擎机如在 :8124 跑一个同款大屏实例，主屏「服务器列表」会自动接入该机 CPU/GPU/内存简况
- 示例可直接 GET /api/config/example 获取；当前配置可 GET /api/config/export 导出

## 生成后自检清单

1. 每个 url 可达（curl <url>/health 返回 200）
2. channels 与 targets 的 url 通常一一对应（同名同址）；灾备场景才用不同 priority 分层
3. policy 按负载类型选：多轮对话=session；一次性高并发短输出=round_robin/least_requests；混合=weight
"""


def build_config_export(reg, gw):
    """导出当前配置（targets + 网关策略/渠道；不含客户端 Key）。"""
    with reg.lock:
        targets = [{"name": t.name, "url": t.url, "api_key": t.api_key,
                    "model_path": t.model_path, "context_len": t.context_len}
                   for t in reg.targets]
    out = {"targets": targets, "gateway": {"policy": "weight", "channels": []}}
    if gw is not None:
        with gw.lock:
            out["gateway"] = {
                "policy": gw.data.get("policy", "weight"),
                "channels": [{"name": c["name"], "url": c["base_url"], "api_key": c.get("api_key", ""),
                              "priority": c.get("priority", 1), "weight": c.get("weight", 10)}
                             for c in gw.data["channels"]],
            }
    return out


def apply_config_import(reg, gw, cfg):
    """整体替换 targets 与渠道/策略；返回 (ok, 摘要)。客户端 Key 保留。"""
    try:
        tlist = cfg.get("targets") or []
        gcfg = cfg.get("gateway") or {}
        for t in list(reg.targets):   # remove() 自带锁（非重入锁，勿外包 reg.lock 否则死锁）
            reg.remove(t.id)
        for it in tlist:
            reg.add((it.get("name") or "target").strip(), (it.get("url") or "").strip(),
                    it.get("api_key") or "", model_path=(it.get("model_path") or "").strip(),
                    context_len=str(it.get("context_len") or ""))
        reg.save()
        if gw is not None and gcfg:
            with gw.lock:
                gw.data["policy"] = gcfg.get("policy") if gcfg.get("policy") in GW_LB_POLICIES else "weight"
                gw.data["channels"] = []
                for c in (gcfg.get("channels") or []):
                    url = (c.get("url") or "").strip().rstrip("/")
                    if not url:
                        continue
                    gw.data["channels"].append({
                        "name": (c.get("name") or "ch").strip(), "base_url": url,
                        "api_key": c.get("api_key") or "",
                        "priority": int(c.get("priority") or 1),
                        "weight": max(1, int(c.get("weight") or 10)), "enabled": True})
                gw.chan = {c["name"]: gw._blank_chan() for c in gw.data["channels"]}
                gw.reqwin, gw.aff = {}, {}
                gw.save()
        nch = len(gw.data["channels"]) if gw is not None else 0
        return True, "导入完成：%d 个监控目标，%d 个渠道" % (len(tlist), nch)
    except Exception as e:
        return False, "导入失败：%s" % e


class Handler(BaseHTTPRequestHandler):
    reg = None       # type: Registry
    demos = []       # type: list  仿真引擎列表（demo 模式）
    node = None      # type: NodeCollector
    gw = None        # type: Gateway
    gw_port = 0
    started = time.time()

    def log_message(self, fmt, *args):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/targets":
            self._json({"targets": [
                {"id": t.id, "name": t.name, "url": t.url, "status": t.status,
                 "last_error": t.last_error, "last_ts": t.last_ts}
                for t in Handler.reg.targets]})
        elif path == "/api/config/export":
            self._json(build_config_export(Handler.reg, Handler.gw))
        elif path == "/api/config/example":
            self._json(CONFIG_EXAMPLE)
        elif path == "/agent.md":
            body = AGENT_MD.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/markdown; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="agent.md"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/overview":
            rows = []
            tot = {"gen_rate": 0.0, "prompt_rate": 0.0, "running": 0, "waiting": 0,
                   "gen_tokens_total": 0, "prompt_tokens_total": 0,
                   "preemptions": 0, "finished": 0}
            n_up = n_tot = 0
            kv_max = None
            kv_name = ""
            tpots = []
            pcs = []
            specs = []
            for t in Handler.reg.targets:
                n_tot += 1
                nb = t.now_brief() or {}
                row = {"id": t.id, "name": t.name, "status": t.status, "url": t.url,
                       "model": "", "version": ""}
                mi = t.meta_info or {}
                if t.model_path:      # 权重路径名优先（完整版本名，如 DeepSeek-V4.1-Flash-1017）
                    row["model"] = t.model_path.rstrip("/").split("/")[-1]
                elif mi.get("model_ids"):
                    row["model"] = mi["model_ids"][0]
                try:
                    v = json.loads(mi.get("version") or "")
                    row["version"] = v.get("version", "")
                except Exception:
                    row["version"] = (mi.get("version") or "")[:24]
                if t.status == "up":
                    n_up += 1
                    tot["gen_rate"] += nb.get("gen_rate") or 0
                    tot["prompt_rate"] += nb.get("prompt_rate") or 0
                    tot["running"] += nb.get("running") or 0
                    tot["waiting"] += nb.get("waiting") or 0
                    tot["gen_tokens_total"] += nb.get("gen_tokens_total") or 0
                    tot["prompt_tokens_total"] += nb.get("prompt_tokens_total") or 0
                    tot["preemptions"] += nb.get("preemptions") or 0
                    tot["finished"] += nb.get("finished") or 0
                    if nb.get("kv_perc") is not None and (kv_max is None or nb["kv_perc"] > kv_max):
                        kv_max = nb["kv_perc"]
                        kv_name = t.name
                    if nb.get("tpot_p95") is not None:
                        tpots.append(nb["tpot_p95"])
                    if nb.get("prefix_hit") is not None:
                        pcs.append(nb["prefix_hit"])
                    if nb.get("spec_accept") is not None:
                        specs.append(nb["spec_accept"])
                for k in ("running", "waiting", "kv_perc", "gen_rate", "prompt_rate",
                          "tpot_p95", "ttft_p95", "gen_tokens_total",
                          "prompt_tokens_total", "preemptions", "finished",
                          "prefix_hit", "spec_accept"):
                    row[k] = nb.get(k)
                row["has_spec"] = t.has_spec
                row["has_pc"] = t.has_prefix_cache
                row["node"] = getattr(t, "node", None)
                row["context_len"] = getattr(t, "context_len", "")
                rows.append(row)
            tot["tpot_p95"] = (sum(tpots) / len(tpots)) if tpots else None
            tot["prefix_hit"] = (sum(pcs) / len(pcs)) if pcs else None
            tot["spec_accept"] = (sum(specs) / len(specs)) if specs else None
            gw = None
            if Handler.gw:
                gw = {"enabled": Handler.gw.enabled, "port": Handler.gw_port,
                      "policy": Handler.gw.data.get("policy", "weight")}
            self._json({"targets": rows, "n_up": n_up, "n_total": n_tot,
                        "totals": tot, "kv_max": kv_max, "kv_max_name": kv_name, "gw": gw})
        elif path == "/api/gateway":
            if Handler.gw:
                self._json(Handler.gw.snapshot(Handler.gw_port))
            else:
                self._json({"enabled": False, "off": True})
        elif path == "/api/node":
            if Handler.node:
                self._json(Handler.node.snapshot())
            else:
                self._json({"available": False})
        elif path == "/api/data":
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            tid = (q.get("id") or [""])[0]
            maxp = int((q.get("maxpoints") or ["1200"])[0])
            t = Handler.reg.get(tid)
            if not t:
                self._json({"error": "no such target"}, 404)
                return
            self._json(t.snapshot(maxpoints=maxp))
        elif path.startswith("/api/demo-metrics"):
            tail = path[len("/api/demo-metrics"):]
            idx = int(tail) if tail.isdigit() else 0   # 只认纯数字后缀（防 /v1/models 之类拼接路径炸解析）
            if idx < len(Handler.demos) and Handler.demos[idx]:
                body = Handler.demos[idx].render().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(404)
        else:
            self.send_error(404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        try:
            payload = json.loads(self.rfile.read(n).decode("utf-8")) if n else {}
        except Exception:
            self._json({"error": "bad json"}, 400)
            return
        path = self.path.split("?")[0]
        if path == "/api/targets":
            name = (payload.get("name") or "").strip() or "target"
            url = (payload.get("url") or "").strip()
            if not url:
                self._json({"error": "url required"}, 400)
                return
            if not re.match(r"^https?://", url):
                self._json({"error": "url must start with http(s)://"}, 400)
                return
            t = Handler.reg.add(name, url, payload.get("api_key") or "",
                                model_path=(payload.get("model_path") or "").strip(),
                                context_len=(payload.get("context_len") or "").strip())
            wire_local_node(t)
            self._json({"ok": True, "id": t.id})
        elif path == "/api/config/import":
            cfg = payload.get("config")
            if not isinstance(cfg, dict) or "targets" not in cfg:
                self._json({"error": "config 需要 {targets:[...], gateway:{...}} 结构"}, 400)
                return
            ok, msg = apply_config_import(Handler.reg, Handler.gw, cfg)
            self._json({"ok": ok, "message": msg}, 200 if ok else 400)
        elif path.startswith("/api/gateway/") and Handler.gw:
            g = Handler.gw
            if path == "/api/gateway/toggle":
                g.enabled = not g.enabled
                self._json({"ok": True, "enabled": g.enabled})
            elif path == "/api/gateway/key":
                k = g.gen_key(payload.get("name", ""), payload.get("rpm", 0),
                              payload.get("quota_tokens", 0))
                self._json({"ok": True, "key": k})
            elif path == "/api/gateway/key/op":
                ok = g.key_op(payload.get("key", ""), payload.get("op", ""),
                              rpm=payload.get("rpm"), quota_tokens=payload.get("quota_tokens"))
                self._json({"ok": ok})
            elif path == "/api/gateway/channel":
                ok = g.add_channel(payload.get("name", ""), payload.get("url", ""),
                                   payload.get("api_key", ""), payload.get("priority", 1),
                                   payload.get("weight", 10))
                self._json({"ok": ok} if ok else {"ok": False, "error": "名称为空或已存在"})
            elif path == "/api/gateway/channel/op":
                ok = g.channel_op(payload.get("name", ""), payload.get("op", ""))
                self._json({"ok": ok})
            elif path == "/api/gateway/policy":
                ok = g.set_policy(payload.get("policy", ""))
                self._json({"ok": ok} if ok else {"ok": False, "error": "未知策略"})
            else:
                self._json({"error": "not found"}, 404)
        elif path == "/api/targets/remove":
            Handler.reg.remove(payload.get("id") or "")
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)


def main():
    global HTML
    ap = argparse.ArgumentParser(description="AI 推理性能监控大屏（单文件，标准库）")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8124)
    ap.add_argument("--target", action="append", default=[],
                    help="NAME=URL，可多次；URL 为 vLLM 服务根地址（自动拼 /metrics）")
    ap.add_argument("--api-key", default="", help="默认抓取用 Bearer key（页面添加的目标可单独覆盖）")
    ap.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    ap.add_argument("--history", type=int, default=DEFAULT_HISTORY)
    ap.add_argument("--config", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "targets.json"))
    ap.add_argument("--demo", action="store_true", help="内置仿真数据源（无真实服务时演示界面）")
    ap.add_argument("--demo-count", type=int, default=10, help="仿真源数量（默认 10，上限 26）")
    ap.add_argument("--gw", action="store_true", help="启用 API 分发网关（/gw/v1/* OpenAI 兼容端点）")
    ap.add_argument("--gw-port", type=int, default=8125, help="网关监听端口（默认 8125）")
    ap.add_argument("--gw-config", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "gateway.json"), help="网关配置文件（渠道+Key+用量）")
    ap.add_argument("--no-open", action="store_true")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    HTML = _build_html().replace("__APPVER__", VERSION)
    reg = Registry(args.config)
    Handler.reg = reg
    Handler.node = NodeCollector(interval=max(3.0, args.interval + 1.0))
    if not Handler.node.available:
        print("[node] 宿主机资源采集不可用（无 /proc 且无 nvidia-smi），服务器资源卡将隐藏")

    demo_url = None
    if args.demo:
        # demo 指标挂在主服务 /api/demo-metrics[N] 上，直接注册为本地回环目标
        for i in range(max(1, min(args.demo_count, 26))):
            Handler.demos.append(DemoEngine(phase_shift=(i * 37) % 190.0))
            suf = "" if i == 0 else str(i)   # 后缀=索引：/api/demo-metricsN → demos[N]
            reg.add("demo 仿真 %s" % "ABCDEFGHIJKLMNOP"[i],
                    "http://127.0.0.1:%d/api/demo-metrics%s" % (args.port, suf), transient=True)
        print("[demo] 仿真数据源 ×%d 已启用（周期负载：空闲-爬坡-c32-排空）" % len(Handler.demos))

    for spec in args.target:
        if "=" in spec:
            name, url = spec.split("=", 1)
        else:
            name, url = spec.rsplit("/", 1)[-1] or "target", spec
        if not any(t.url.rstrip("/") == url.rstrip("/") for t in reg.targets):
            t = reg.add(name, url, args.api_key)
            wire_local_node(t)
            print("[target] %s = %s" % (name, url))
    reg.save()
    for t in reg.targets:      # 持久化恢复的目标也接线
        wire_local_node(t)

    if args.gw:
        gw = Gateway(args.gw_config)

        def _gw_metrics(base_url):
            # 渠道地址 ↔ 监控目标地址匹配（规范化后精确匹配）→ 引擎最新指标
            want = base_url.rstrip("/").lower()
            for t in reg.targets:
                if t.url.rstrip("/").lower() == want:
                    return t.now_brief()
            return None
        gw.metrics_fn = _gw_metrics
        Handler.gw = gw
        Handler.gw_port = args.gw_port
        GatewayHandler.gw = gw
        ghttpd = ThreadingHTTPServer((args.host, args.gw_port), GatewayHandler)
        threading.Thread(target=ghttpd.serve_forever, daemon=True).start()
        print("  网关:   http://%s:%d/gw/v1 （OpenAI 兼容；⚙ 菜单里管理 Key/渠道）"
              % ("127.0.0.1" if args.host == "0.0.0.0" else args.host, args.gw_port))
        print("         渠道 %d 个 / Key %d 把（%s）" % (len(gw.data["channels"]),
              len(gw.data["keys"]), args.gw_config))
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print("=" * 62)
    print("  AI 推理性能监控大屏已启动  (v%s)" % VERSION)
    print("  界面:   http://%s:%d/" % ("127.0.0.1" if args.host == "0.0.0.0" else args.host, args.port))
    print("  目标数: %d | 抓取间隔 %.0fs | 历史 %d 点/目标" % (len(reg.targets), args.interval, args.history))
    print("  目标配置持久化: %s" % args.config)
    print("=" * 62)
    if not args.no_open:
        try:
            import webbrowser
            threading.Timer(0.6, lambda: webbrowser.open("http://127.0.0.1:%d/" % args.port)).start()
        except Exception:
            pass
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


# ============================== 前端（内嵌 HTML） ==============================

def _build_html():
    return r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI 推理性能监控大屏</title>
<style>
html{scrollbar-gutter:stable}
:root{
  --bg:#070b16; --panel:rgba(20,28,48,.55); --panel-border:rgba(120,160,255,.14);
  --txt:#dbe6ff; --dim:#7f8db0; --accent:#22d3ee; --blue:#60a5fa; --violet:#a78bfa;
  --amber:#fbbf24; --rose:#fb7185; --green:#34d399;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{
  font-family:-apple-system,"Segoe UI","Microsoft YaHei","PingFang SC",sans-serif;
  background:
    radial-gradient(1200px 500px at 15% -10%, rgba(34,211,238,.10), transparent 60%),
    radial-gradient(1000px 600px at 85% -20%, rgba(167,139,250,.10), transparent 60%),
    var(--bg);
  color:var(--txt); overflow-x:hidden;
  font-variant-numeric:tabular-nums;
}
body::before{content:"";position:fixed;inset:0;pointer-events:none;opacity:.35;
  background-image:linear-gradient(rgba(130,160,255,.05) 1px,transparent 1px),
    linear-gradient(90deg,rgba(130,160,255,.05) 1px,transparent 1px);
  background-size:44px 44px;}
.wrap{max-width:none;margin:0;padding:12px 14px 26px;position:relative}

/* ---- header（标题独占居中行，其余下移工具栏） ---- */
.brand{display:flex;justify-content:center;padding:2px 0 10px}
.brand h1{font-size:25px;font-weight:700;letter-spacing:3px;text-align:center;
  background:linear-gradient(92deg,#67e8f9,#818cf8 55%,#c084fc);
  -webkit-background-clip:text;background-clip:text;color:transparent}
.toolbar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:0 2px 12px}
.toolbar .spacer{flex:1}
.gearwrap{position:relative}
#gearBtn{width:34px;height:30px;border-radius:8px;border:1px solid rgba(120,160,255,.35);
  background:transparent;color:#9fb4e8;cursor:pointer;font-size:15px;line-height:1}
#gearBtn:hover,#gearBtn.open{color:var(--accent);border-color:rgba(34,211,238,.5)}
.menu{position:absolute;right:0;top:116%;background:#0d1428;border:1px solid rgba(130,160,255,.3);
  border-radius:11px;padding:6px;display:none;min-width:186px;z-index:60;
  box-shadow:0 10px 30px rgba(0,0,0,.5)}
.menu .mi{padding:9px 12px;border-radius:8px;font-size:13px;color:var(--txt);cursor:pointer;white-space:nowrap}
.menu .mi:hover{background:rgba(34,211,238,.10);color:#aef3ff}
.menu .mver{padding:7px 12px 3px;font-size:11px;color:var(--dim);border-top:1px solid rgba(130,160,255,.18);
  margin-top:5px;cursor:default;user-select:text}
.tabs{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.tab{padding:7px 14px;border-radius:9px;cursor:pointer;border:1px solid var(--panel-border);
  background:var(--panel);font-size:13px;display:flex;gap:8px;align-items:center;user-select:none}
.tab.active{border-color:rgba(34,211,238,.55);background:rgba(34,211,238,.10);color:#aef3ff}
.tab .x{color:var(--dim);font-size:11px;padding:0 2px;border-radius:4px}
.tsel{background:var(--panel);color:var(--txt);border:1px solid var(--panel-border);border-radius:9px;
  padding:6px 10px;font-size:13px;cursor:pointer;max-width:220px}
.tbtn{padding:6px 10px;border-radius:9px;border:1px solid var(--panel-border);background:var(--panel);
  color:var(--dim);font-size:12.5px;cursor:pointer;user-select:none}
.tbtn:hover{color:#aef3ff;border-color:rgba(34,211,238,.45)}
.tbtn.danger:hover{color:#fda4af;border-color:rgba(251,113,133,.5)}
.tab .x:hover{color:var(--rose)}
.tab .led{width:8px;height:8px;border-radius:50%;background:#555}
.tab .led.up{background:var(--green);box-shadow:0 0 8px var(--green)}
.tab .led.down{background:var(--rose);box-shadow:0 0 8px var(--rose)}
#ovAlarm{background:rgba(251,113,133,.10);border:1px solid rgba(251,113,133,.45);color:#fda4af;
  padding:9px 14px;border-radius:10px;margin-bottom:10px;font-size:13.5px}
.ovtbl{width:100%;border-collapse:collapse;table-layout:fixed;font-size:12px}
.ovtbl th:nth-child(1){width:34px}
.ovtbl th:nth-child(2){width:96px}
.ovtbl th:nth-child(2),.ovtbl td:nth-child(2){text-align:center}
.ovtbl th:nth-child(11){width:205px}
.ovtbl th{color:var(--dim);font-weight:400;text-align:left;padding:3px 5px 2px;border-bottom:1px solid var(--panel-border);white-space:nowrap;font-size:11px;overflow:hidden;text-overflow:ellipsis}
.ovtbl td{padding:2px 5px;border-bottom:1px solid rgba(120,140,200,.06);white-space:nowrap;line-height:1.35;overflow:hidden;text-overflow:ellipsis}
#ovmachCard{padding:8px 10px 6px}
.ovtbl tr.ovrow{cursor:pointer}
.ovtbl tr.ovrow:hover{background:rgba(34,211,238,.06)}
.ovtbl td.nm{color:#cfe3ff;font-weight:600}
.spacer{flex:1}
.clock{font-size:14px;color:#9fb4e8;letter-spacing:.5px}
.clock small{color:var(--dim);margin-left:8px}
.ranges{display:flex;gap:6px}
.ranges button{padding:5px 10px;font-size:12px;border-radius:7px;border:1px solid var(--panel-border);
  background:transparent;color:var(--dim);cursor:pointer}
.ranges button.active{color:var(--accent);border-color:rgba(34,211,238,.5);background:rgba(34,211,238,.08)}

/* ---- banner ---- */
#banner{display:none;margin:4px 0 10px;padding:10px 14px;border-radius:10px;
  border:1px solid rgba(251,113,133,.4);background:rgba(251,113,133,.08);color:#ffd3da;font-size:13px}

/* ---- KPI ---- */
/* ---- KPI + 服务器资源两栏区（左：模型卡+vLLM 卡两行 / 右：服务器卡） ---- */
.zone{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:12px;align-items:stretch}
.zleft{display:flex;flex-direction:column;gap:10px;flex:2 1 640px;min-width:0}
.modelcard{padding:9px 14px 7px}
.modelcard h4{margin-bottom:4px;font-size:12px}
.modelcard td{padding:2.5px 4px;font-size:12px}
.kpis{display:grid;grid-template-columns:repeat(6,minmax(130px,158px));grid-auto-rows:1fr;
  gap:10px;align-content:start;justify-content:start}
.nodes{display:grid;grid-template-columns:minmax(240px,1fr);gap:12px;
  flex:1 1 340px;min-width:300px;align-content:start}
.ovlay{position:relative;display:flex;align-items:flex-start}
#lg-ovcum{display:none}   # 机器多时图例占空间，隐藏（悬停仍显示每线名称）
#mainOv .charts{grid-template-columns:repeat(2,minmax(0,1fr))}
#mainSingle .charts{grid-template-columns:repeat(3,minmax(0,1fr))}
#mainSingle .kpis{grid-template-columns:repeat(auto-fit,minmax(148px,1fr));justify-content:stretch}
#ovKpis{grid-template-columns:repeat(auto-fit,minmax(118px,1fr));justify-content:stretch}
.ovL{flex:1;min-width:0;margin-right:350px}
.ovR{position:absolute;top:0;right:0;bottom:0;width:338px;display:flex}
.ovR .card{flex:1;display:flex;flex-direction:column;min-height:0;overflow:hidden}
.ovR .card h4{flex:none}
#ovNodes{flex:1;overflow-y:auto;min-height:0;padding-right:4px}
.ovnode{padding:8px 10px;border:1px solid var(--panel-border);border-radius:10px;margin-bottom:8px;
  background:rgba(15,23,42,.35);cursor:pointer}
.ovnode:hover{border-color:rgba(34,211,238,.4)}
.ovnode .ovhead{display:flex;align-items:center;gap:7px;font-size:13.5px;margin-bottom:4px}
.ovnode .ovhead b{color:#cfe3ff}
.ovnode .dim{color:var(--dim);font-size:11.5px}
.ovnode .ln{font-size:12.5px;color:#aebfe6;margin:2px 0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
@media (max-width:1280px){.kpis{grid-template-columns:repeat(auto-fit,minmax(150px,1fr));flex:1 1 100%}}
.kpi{background:var(--panel);border:1px solid var(--panel-border);border-radius:13px;
  padding:9px 12px 7px;backdrop-filter:blur(8px);position:relative;overflow:hidden;transition:border-color .3s}
.kpi:hover{border-color:rgba(34,211,238,.4)}
.kpi .lbl{font-size:11.5px;color:var(--dim);display:flex;justify-content:space-between;align-items:center}
.kpi .val{font-size:22px;font-weight:700;margin:4px 0 2px;letter-spacing:.5px;line-height:1.1}
.kpi .unit{font-size:12px;color:var(--dim);font-weight:400;margin-left:3px}
.kpi .sub{font-size:11px;color:var(--dim);min-height:13px}
.kpi canvas.spark{position:absolute;right:7px;bottom:7px;width:66px;height:27px;opacity:.9}
.kpi.hidden{display:none}
.c-cyan{color:#67e8f9;text-shadow:0 0 18px rgba(34,211,238,.35)}
.c-blue{color:#93c5fd;text-shadow:0 0 18px rgba(96,165,250,.35)}
.c-violet{color:#c4b5fd;text-shadow:0 0 18px rgba(167,139,250,.35)}
.c-amber{color:#fcd34d;text-shadow:0 0 18px rgba(251,191,36,.3)}
.c-green{color:#6ee7b7;text-shadow:0 0 18px rgba(52,211,153,.3)}
.c-rose{color:#fda4af;text-shadow:0 0 18px rgba(251,113,133,.3)}
.bar{height:6px;border-radius:3px;background:rgba(130,160,255,.12);margin:7px 0 4px;overflow:hidden}
.bar i{display:block;height:100%;border-radius:3px;background:linear-gradient(90deg,#34d399,#22d3ee);
  transition:width .5s ease}
.bar i.warn{background:linear-gradient(90deg,#fbbf24,#f97316)}
.bar i.crit{background:linear-gradient(90deg,#fb7185,#ef4444)}

/* ---- charts ---- */
.charts{display:grid;grid-template-columns:repeat(auto-fit,minmax(430px,1fr));gap:12px}
.panel{background:var(--panel);border:1px solid var(--panel-border);border-radius:14px;
  padding:12px 14px 8px;backdrop-filter:blur(8px)}
.panel h3{font-size:13.5px;font-weight:600;color:#aebfe6;margin-bottom:6px;display:flex;justify-content:space-between}
.panel h3 .legend{display:flex;gap:12px;font-size:11.5px;color:var(--dim);font-weight:400}
.panel h3 .legend i{display:inline-block;width:9px;height:9px;border-radius:2px;margin-right:5px;vertical-align:-1px}
.panel .cwrap{position:relative;height:190px}
canvas.chart{width:100%;height:100%;display:block}
.tip{position:fixed;z-index:50;pointer-events:none;display:none;background:rgba(10,16,32,.94);
  border:1px solid rgba(130,160,255,.25);border-radius:9px;padding:8px 11px;font-size:12px;
  color:var(--txt);box-shadow:0 8px 28px rgba(0,0,0,.45);white-space:nowrap}
.tip .tt{color:var(--dim);margin-bottom:4px}

/* ---- footer info ---- */
.info{margin-top:14px;display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.card{background:var(--panel);border:1px solid var(--panel-border);border-radius:14px;padding:12px 14px;font-size:12.5px}
.card h4{font-size:12.5px;color:#aebfe6;margin-bottom:8px;font-weight:600}
.card table{width:100%;border-collapse:collapse;table-layout:fixed}
.card td{padding:3.5px 4px;color:var(--dim);vertical-align:middle}
.card td:first-child{width:34%;white-space:nowrap}
.card td:last-child{text-align:left;color:var(--txt);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#tb-ovsum td:first-child{width:86px}
#tb-model td:first-child,#tb-req td:first-child,#tb-info td:first-child,#tb-total td:first-child{width:112px}
.card .mono{font-family:Consolas,Menlo,monospace;font-size:11.5px;color:#9fb4e8}

/* ---- modal ---- */
.modal-bg{position:fixed;inset:0;background:rgba(4,7,15,.7);display:none;z-index:100;
  align-items:center;justify-content:center;backdrop-filter:blur(3px)}
.modal{position:relative;width:420px;background:#0d1428;border:1px solid rgba(130,160,255,.25);border-radius:16px;padding:22px}
.mclose{position:absolute;right:12px;top:10px;cursor:pointer;color:var(--dim);font-size:17px;line-height:1;padding:4px 7px;border-radius:8px;user-select:none}
.mclose:hover{color:#fda4af;background:rgba(251,113,133,.12)}
.modal h3{font-size:16px;margin-bottom:14px;color:#cfe0ff}
.modal label{display:block;font-size:12px;color:var(--dim);margin:12px 0 5px}
.modal input{width:100%;padding:9px 12px;border-radius:9px;border:1px solid rgba(130,160,255,.25);
  background:rgba(20,28,48,.8);color:var(--txt);font-size:13px;outline:none}
.modal input:focus{border-color:var(--accent)}
.modal .btns{display:flex;gap:10px;margin-top:20px;justify-content:flex-end}
.modal.wide{width:920px;max-width:96vw;max-height:88vh;overflow:auto}
.gwtop{display:flex;justify-content:space-between;align-items:center;font-size:13px;
  padding:10px 12px;border:1px solid var(--panel-border);border-radius:10px;margin-bottom:10px}
.gwbtns button{padding:6px 14px;border-radius:8px;border:1px solid rgba(130,160,255,.3);
  background:transparent;color:var(--txt);cursor:pointer;font-size:12.5px}
.gwbtns button:hover{border-color:var(--accent);color:var(--accent)}
.gwstats{display:flex;gap:22px;font-size:12.5px;color:var(--dim);margin-bottom:14px}
.gwstats b{color:var(--txt);font-size:15px;margin-right:4px}
.gwh{font-size:13px;color:#aebfe6;margin:14px 0 6px;font-weight:600}
.gwh .dim{font-weight:400;font-size:11px;color:var(--dim)}
.gwtable{width:100%;border-collapse:collapse;font-size:12px}
.gwtable th{color:var(--dim);font-weight:400;text-align:left;padding:4px 6px;
  border-bottom:1px solid var(--panel-border);white-space:nowrap}
.gwtable td{padding:5px 6px;border-bottom:1px solid rgba(130,160,255,.07);white-space:nowrap}
.gwtable td.key{cursor:pointer;color:#9fb4e8}
.gwtable td.key:hover{color:var(--accent)}
.gwtable .op{color:#67e8f9;cursor:pointer;margin-right:2px}
.gwtable .op:hover{text-decoration:underline}
.gwform{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap;align-items:center}
.gwadd{margin-top:8px;color:#67e8f9;font-size:12.5px;cursor:pointer;user-select:none;display:inline-block;padding:3px 8px;border-radius:7px}
.gwadd:hover{background:rgba(34,211,238,.10)}
.slide{overflow:hidden;max-height:0;transition:max-height .22s ease}
.slide.open{max-height:200px}
.gwform input{padding:7px 10px;border-radius:8px;border:1px solid rgba(130,160,255,.25);
  background:rgba(20,28,48,.8);color:var(--txt);font-size:12.5px;outline:none}
.gwform button{padding:8px 16px;border-radius:8px;border:none;cursor:pointer;font-size:12.5px;
  background:linear-gradient(92deg,#0891b2,#2563eb);color:#fff}
.modal button{padding:8px 18px;border-radius:9px;border:none;cursor:pointer;font-size:13px}
#mOk{background:linear-gradient(92deg,#0891b2,#2563eb);color:#fff}
#mCancel{background:transparent;border:1px solid rgba(130,160,255,.3);color:var(--dim)}
.err{color:var(--rose);font-size:12px;margin-top:8px;min-height:15px}
@media (max-width:900px){.kpi .val{font-size:22px}.brand h1{font-size:19px}}
</style>
</head>
<body>
<div class="wrap">
  <header>
  <div class="brand"><h1>AI 推理性能监控大屏</h1></div>
  <div class="toolbar">
    <div class="tabs" id="tabs"></div>
    <div class="spacer"></div>
    <div class="ranges" id="ranges">
      <button data-min="5">5 分钟</button><button data-min="15" class="active">15 分钟</button>
      <button data-min="60">1 小时</button><button data-min="0">全部</button>
    </div>
    <div class="gearwrap">
      <button id="gearBtn" title="设置">⚙</button>
      <div class="menu" id="gearMenu">
        <div class="mi" id="miAdd">＋ 添加监控目标</div>
        <div class="mi" id="miExport">⬇ 导出日志（CSV）</div>
        <div class="mi" id="miGw">🔌 API 分发（网关）</div>
        <div class="mi" id="miCfgExport">⬇ 导出配置（当前）</div>
        <div class="mi" id="miCfgExample">⬇ 导出示例配置</div>
        <div class="mi" id="miCfgImport">⬆ 导入配置</div>
        <div class="mi" id="miAgentMd">⬇ 下载 agent.md</div>
        <input type="file" id="cfgFile" accept=".json,application/json" style="display:none">
        <div class="mver" title="当前运行的版本">v__APPVER__</div>
      </div>
    </div>
  </div>
</header>
  <div id="banner"></div>

  <div id="mainOv" style="display:none">
    <div id="ovAlarm" style="display:none"></div>
    <div class="ovlay">
    <div class="ovL">
    <div class="zone">
      <div class="zleft">
        <div class="card modelcard"><h4>全场总览</h4><table id="tb-ovsum"></table></div>
        <div class="kpis" id="ovKpis"></div>
      </div>
    </div>
    <div class="charts" style="grid-template-columns:repeat(2,minmax(0,1fr))">
      <div class="panel"><h3>Decode 吞吐（合计）<span class="legend" id="lg-ovdec"></span></h3><div class="cwrap"><canvas class="chart" id="ch-ovdec"></canvas></div></div>
      <div class="panel"><h3>Prefill 吞吐（合计）<span class="legend" id="lg-ovpre"></span></h3><div class="cwrap"><canvas class="chart" id="ch-ovpre"></canvas></div></div>
      <div class="panel"><h3>并发请求（合计）<span class="legend" id="lg-ovcc"></span></h3><div class="cwrap"><canvas class="chart" id="ch-ovcc"></canvas></div></div>
      <div class="panel"><h3>累计 Decode（各机对比）<span class="legend" id="lg-ovcum"></span></h3><div class="cwrap"><canvas class="chart" id="ch-ovcum"></canvas></div></div>
    </div>
    </div>
    <div class="ovR">
      <div class="card"><h4>服务器列表</h4><div id="ovNodes"></div></div>
    </div>
    </div>
    <div class="info">
      <div class="card" id="ovmachCard"><h4>AI服务详情（点击名称切换到该服务大屏）</h4>
        <table class="ovtbl" id="tb-ovmach"><thead><tr>
          <th></th><th>名称</th><th>Decode</th><th>Prefill</th><th>运行/排队</th><th>KV</th><th>TPOT p95</th><th>缓存命中</th><th>接受率</th><th>累计生成</th><th>模型</th><th>上下文</th><th>版本</th>
        </tr></thead><tbody></tbody></table>
      </div>
    </div>
  </div>

  <div id="mainSingle">
  <div class="zone">
    <div class="zleft">
      <div class="card modelcard"><h4>模型 / 接入信息</h4><table id="tb-model"></table></div>
      <div class="kpis" id="kpis"></div>
    </div>
    <div class="nodes" id="nodeStrip" style="display:none">
      <div class="card"><h4>服务器信息</h4><table id="tb-node"></table></div>
      <div class="kpi" id="nd-mem"><div class="lbl"><span>内存</span></div><div class="val c-blue">—</div><div class="bar"><i></i></div><div class="sub"></div></div>
    </div>
  </div>

  <div class="charts">
    <div class="panel"><h3>Decode 吞吐<span class="legend" id="lg-dec"></span></h3><div class="cwrap"><canvas class="chart" id="ch-dec"></canvas></div></div>
    <div class="panel"><h3>Prefill 吞吐<span class="legend" id="lg-pre"></span></h3><div class="cwrap"><canvas class="chart" id="ch-pre"></canvas></div></div>
    <div class="panel"><h3>KV Cache 使用率<span class="legend" id="lg-kv"></span></h3><div class="cwrap"><canvas class="chart" id="ch-kv"></canvas></div></div>
    <div class="panel"><h3>并发请求<span class="legend" id="lg-cc"></span></h3><div class="cwrap"><canvas class="chart" id="ch-cc"></canvas></div></div>
    <div class="panel"><h3>延迟分位（窗口 p95）<span class="legend" id="lg-lat"></span></h3><div class="cwrap"><canvas class="chart" id="ch-lat"></canvas></div></div>
  </div>

  <div class="info">
    <div class="card"><h4>请求统计</h4><table id="tb-req"></table></div>
    <div class="card"><h4>目标信息</h4><table id="tb-info"></table></div>
    <div class="card"><h4>累计量</h4><table id="tb-total"></table></div>
  </div>
  </div>
</div>

<div class="modal-bg" id="modalBg">
  <div class="modal">
    <h3>添加监控目标</h3>
    <label>名称</label><input id="mName" placeholder="例如 prod-1">
    <label>vLLM 服务地址（自动拼 /metrics）</label><input id="mUrl" placeholder="http://127.0.0.1:8123">
    <label>API Key（--api-key 设置过 /metrics 可匿名时留空）</label><input id="mKey" placeholder="可选">
    <label>模型权重路径（可选，模型卡展示）</label><input id="mPath" placeholder="/data/models/your-model">
    <label>最大上下文（可选）</label><input id="mCtx" placeholder="1048576">
    <div class="err" id="mErr"></div>
    <div class="btns"><button id="mCancel">取消</button><button id="mOk">添加</button></div>
  </div>
</div>
<div class="modal-bg" id="gwModalBg">
  <div class="modal wide">
    <div class="mclose" id="gwCloseX" title="关闭">✕</div>
    <h3>API 分发（网关）</h3>
    <div id="gwOff" style="display:none;color:#fda4af;font-size:13px;margin-bottom:10px">
      网关未启动：用 <span class="mono">--gw</span> 参数启动大屏后可用（如 <span class="mono">python vllm_dashboard.py --gw</span>）
    </div>
    <div id="gwBody">
      <div class="gwtop">
        <div>状态：<b id="gwState">—</b>　端口：<span class="mono" id="gwPort">—</span>　接入地址：<span class="mono" id="gwBase">—</span>
          <span style="margin-left:10px">策略：
            <select id="gwPolicy" style="background:#0b1526;color:#cfe3ff;border:1px solid rgba(34,211,238,.35);border-radius:6px;padding:2px 6px;font-size:12px">
              <option value="weight">加权随机</option>
              <option value="round_robin">轮询</option>
              <option value="p2c">P2C 二选一</option>
              <option value="least_requests">最少负载</option>
              <option value="session">会话亲和（前缀缓存）</option>
            </select>
          </span>
        </div>
        <div class="gwbtns">
          <button id="gwToggleBtn">暂停转发</button>
        </div>
      </div>
      <div class="gwstats" id="gwStats"></div>

      <h4 class="gwh">负载均衡总表 <span class="dim">（上游渠道 × 引擎实时状态；同优先级层内按策略分流，低优先级层做故障转移）</span></h4>
      <table class="gwtable" id="gwChanT"><thead><tr>
        <th>名称</th><th>地址</th><th>优先/权重</th><th>状态</th><th>探活</th><th>在途</th>
        <th>请求/失败</th><th>QPS(1m)</th><th>token</th><th>均延迟</th>
        <th>引擎: 运行/排队 · KV · Decode</th><th>操作</th>
      </tr></thead><tbody></tbody><tfoot></tfoot></table>
      <div class="gwadd" id="gcToggle">＋ 添加渠道</div>
      <div class="slide" id="gcSlide"><div class="gwform" id="gwChanForm">
        <input id="gcName" placeholder="名称（如 prod-1）" style="width:120px">
        <input id="gcUrl" placeholder="http://127.0.0.1:8123" style="width:200px">
        <input id="gcKey" placeholder="上游Key(可空)" style="width:110px">
        <input id="gcPri" placeholder="优先级1" style="width:80px" value="1">
        <input id="gcWt" placeholder="权重10" style="width:80px" value="10">
        <button id="gcAdd">确 定</button>
      </div></div>

      <h4 class="gwh">接入 Key <span class="dim">（生成后发给同事：接入地址 + Key + 模型名）</span></h4>
      <table class="gwtable" id="gwKeyT"><thead><tr>
        <th>名称</th><th>Key（点击复制）</th><th>RPM 限</th><th>配额/已用 token</th><th>请求</th><th>状态</th><th>操作</th>
      </tr></thead><tbody></tbody></table>
      <div class="gwadd" id="gkToggle">＋ 生成 Key</div>
      <div class="slide" id="gkSlide"><div class="gwform" id="gwKeyForm">
        <input id="gkName" placeholder="Key 名称（如 张三）" style="width:150px">
        <input id="gkRpm" placeholder="RPM 限额（0=不限）" style="width:150px">
        <input id="gkQuota" placeholder="token 配额（0=不限）" style="width:160px">
        <button id="gkGen">确 定</button>
      </div></div>
    </div>
  </div>
</div>

<div class="tip" id="tip"></div>

<script>
"use strict";
/* ---------------- 全局状态 ---------------- */
const OV="__ov__";   // 固定主屏页签（不可移除，始终第一位）
let targets=[], activeId=OV, rangeMin=15, data=null, timer=null, ovData=null, ovSeries=null;

const $=id=>document.getElementById(id);
const fmtInt=v=>v==null?"—":Math.round(v).toLocaleString("en-US");
const fmt1=v=>v==null?"—":v.toFixed(1);
function fmtRate(v){if(v==null)return"—";if(v>=1000)return fmtInt(v);if(v>=100)return v.toFixed(0);return v.toFixed(1);}
function fmtMs(s){if(s==null)return"—";const ms=s*1000;if(ms>=10000)return (ms/1000).toFixed(1)+"s";if(ms>=100)return ms.toFixed(0);return ms.toFixed(1);}
function fmtPct(v,d=1){return v==null?"—":(v*100).toFixed(d)+"%";}
function fmtTok(n){if(n==null)return"—";if(n>=1e9)return (n/1e9).toFixed(2)+"B";if(n>=1e6)return (n/1e6).toFixed(2)+"M";if(n>=1e3)return (n/1e3).toFixed(1)+"K";return ""+Math.round(n);}
function hhmmss(t){const d=new Date(t*1000);return [d.getHours(),d.getMinutes(),d.getSeconds()].map(x=>String(x).padStart(2,"0")).join(":");}
function hhmm(t){const d=new Date(t*1000);return String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0");}

/* ---------------- KPI 卡片定义 ---------------- */
const KPIS=[
 {id:"gen",  lbl:"Decode 吞吐",  cls:"c-cyan",  unit:"tok/s", key:"gen_rate",  fmt:fmtRate, sub:d=>"累计 "+fmtTok(d.now.gen_tokens_total)},
 {id:"pf",   lbl:"Prefill 吞吐", cls:"c-violet",unit:"tok/s", key:"prompt_rate",fmt:fmtRate, sub:d=>"累计 "+fmtTok(d.now.prompt_tokens_total)},
 {id:"kv",   lbl:"KV Cache 剩余", cls:"c-green", unit:"",    key:null,       fmt:null,
   render:(el,d)=>{const p=d.now.kv_perc;const rem=p==null?null:1-p;
     el.querySelector(".val").innerHTML=(rem==null?"—":fmtPct(rem,1))+`<span class="unit">剩余</span>`;
     const bar=el.querySelector(".bar i");const used=p==null?0:p;
     bar.style.width=(used*100).toFixed(1)+"%";
     bar.className=used>=0.9?"crit":used>=0.7?"warn":"";
     el.querySelector(".sub").textContent=d.kv_max_tokens?("池 "+fmtTok(d.kv_max_tokens)+" · 已用 "+fmtTok(d.now.kv_used_tokens)):"";},
   html:`<div class="bar"><i></i></div>`},
 {id:"run",  lbl:"运行中请求", cls:"c-blue",  unit:"路",    key:"running",   fmt:fmtInt, sub:d=>"排队 "+fmtInt(d.now.waiting)+" 路"},
 {id:"ttft", lbl:"TTFT p95",   cls:"c-violet",unit:"ms",    key:"ttft_p95",  fmt:fmtMs,  sub:d=>"p50 "+fmtMs(d.now.ttft_p50)},
 {id:"tpot", lbl:"TPOT p95",   cls:"c-amber", unit:"ms",    key:"tpot_p95",  fmt:fmtMs,  sub:d=>"p50 "+fmtMs(d.now.tpot_p50)},
 {id:"e2e",  lbl:"端到端 p95", cls:"c-blue",  unit:"",      key:"e2e_p95",   fmt:fmtMs,  sub:d=>"排队 p95 "+fmtMs(d.now.queue_p95)},
 {id:"pc",   lbl:"前缀缓存命中", cls:"c-cyan", unit:"",     key:"prefix_hit",fmt:fmtPct, sub:()=>""},
 {id:"spec", lbl:"投机接受率", cls:"c-violet",unit:"",      key:"spec_accept",fmt:fmtPct, sub:()=>""},
 {id:"pre",  lbl:"KV 抢占",    cls:"c-rose",  unit:"次",    key:null,        fmt:null,
   render:(el,d)=>{el.querySelector(".val").innerHTML=fmtInt(d.now.preemptions)+`<span class="unit">次</span>`;},
   html:``},
 {id:"fin",  lbl:"已完成请求", cls:"c-green", unit:"个",    key:null,        fmt:null,
   render:(el,d)=>{el.querySelector(".val").innerHTML=fmtInt(d.now._fin_total)+`<span class="unit">个</span>`;
     el.querySelector(".sub").textContent=d.now._fin_len!=null?("length "+fmtInt(d.now._fin_len)+" · stop "+fmtInt(d.now._fin_stop)):"";},
   html:``},
];
function buildKpis(){
  const box=$("kpis");box.innerHTML="";
  for(const k of KPIS){
    const el=document.createElement("div");el.className="kpi";el.id="kpi-"+k.id;
    el.innerHTML=`<div class="lbl"><span>${k.lbl}</span></div><div class="val">—</div>${k.html||""}<div class="sub"></div>`+
      (k.key?`<canvas class="spark" id="sp-${k.id}"></canvas>`:"");
    box.appendChild(el);
  }
}

/* ---------------- Canvas 图表 ---------------- */
class Chart{
  constructor(id,cfg){
    this.cv=$(id);this.ctx=this.cv.getContext("2d");this.cfg=cfg;
    this.t=[];this.data={};this.hover=-1;
    this.cv.addEventListener("mousemove",e=>this.onMove(e));
    this.cv.addEventListener("mouseleave",()=>{this.hover=-1;$("tip").style.display="none";this.draw();});
    new ResizeObserver(()=>this.draw()).observe(this.cv.parentElement);
  }
  setData(t,map){this.t=t;this.data=map;this.draw();}
  visible(){
    const t=this.t;if(!t.length)return[[],{}];
    let i0=0;
    if(rangeMin>0){const lo=t[t.length-1]-rangeMin*60;while(i0<t.length&&t[i0]<lo)i0++;}
    const map={};for(const s of this.cfg.series)map[s.key]=(this.data[s.key]||[]).slice(i0);
    return[t.slice(i0),map];
  }
  onMove(e){
    const[t,map]=this.visible();if(t.length<2)return;
    const r=this.cv.getBoundingClientRect();
    const x=(e.clientX-r.left);
    const pl=10,pr=54;
    const wCss=this.cv.width/(devicePixelRatio||1), hCss=this.cv.height/(devicePixelRatio||1);
    const pw=wCss-pl-pr;
    let i=Math.round((x-pl)/pw*(t.length-1));i=Math.max(0,Math.min(t.length-1,i));
    this.hover=i;this.draw();
    const tip=$("tip");
    let html=`<div class="tt">${hhmmss(t[i])}</div>`;
    for(const s of this.cfg.series){const v=(map[s.key]||[])[i];
      html+=`<div><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${s.color};margin-right:6px"></i>${s.label}: <b>${this.cfg.fmt?this.cfg.fmt(v):fmtInt(v)}</b></div>`;}
    tip.innerHTML=html;tip.style.display="block";
    tip.style.left=Math.min(window.innerWidth-tip.offsetWidth-8,e.clientX+14)+"px";
    tip.style.top=Math.min(window.innerHeight-tip.offsetHeight-8,e.clientY+14)+"px";
  }
  draw(){
    const dpr=devicePixelRatio||1,w=this.cv.parentElement.clientWidth,h=this.cv.parentElement.clientHeight;
    if(this.cv.width!==w*dpr||this.cv.height!==h*dpr){this.cv.width=w*dpr;this.cv.height=h*dpr;}
    const c=this.ctx;c.setTransform(dpr,0,0,dpr,0,0);c.clearRect(0,0,w,h);
    const[t,map]=this.visible();this.vt=t;this.vmap=map;
    if(t.length<2){c.fillStyle="#7f8db0";c.font="12px sans-serif";c.fillText("等待数据…",w/2-30,h/2);return;}
    const pl=10,pr=54,pt=8,pb=20,pw=w-pl-pr,ph=h-pt-pb;
    let ymax=0;
    for(const s of this.cfg.series)for(const v of map[s.key]||[])if(v!=null&&v>ymax)ymax=v;
    if(!ymax)ymax=1;
    ymax*=1.12;
    const logY=this.cfg.logY;
    let Y,vmin=0,vmaxL=0,vminL=0,step=1,top=1;
    if(logY){
      let lo=Infinity;
      for(const s of this.cfg.series)for(const v of map[s.key]||[])if(v!=null&&v>0){if(v<lo)lo=v;if(v>ymax)ymax=v;}
      if(!isFinite(lo)){lo=0.001;}
      vmaxL=Math.log10(ymax);vminL=Math.log10(lo/1.5);
      if(vmaxL-vminL<0.8)vmaxL=vminL+0.8;
      Y=v=>pt+ph-(Math.log10(Math.max(v,Math.pow(10,vminL)))-vminL)/(vmaxL-vminL)*ph;
    }else{
      step=niceStep(ymax/4);
      top=Math.ceil(ymax/step)*step;
      Y=v=>pt+ph-(v/top)*ph;
    }
    const X=i=>pl+i/(t.length-1)*pw;
    // 网格
    c.strokeStyle="rgba(130,160,255,.09)";c.fillStyle="#66799c";c.font="10px sans-serif";c.lineWidth=1;
    const ticks=[];
    if(logY){
      for(let e=Math.ceil(vminL);e<=Math.floor(vmaxL);e++)
        for(const m of [1,2,5]){const v=m*Math.pow(10,e);
          if(Math.log10(v)>=vminL-1e-9&&Math.log10(v)<=vmaxL+1e-9)ticks.push(v);}
    }else{
      for(let v=0;v<=top+1e-9;v+=step)ticks.push(v);
    }
    for(const v of ticks){const y=Y(v);c.beginPath();c.moveTo(pl,y);c.lineTo(pl+pw,y);c.stroke();
      c.fillText(this.cfg.axisFmt?this.cfg.axisFmt(v):fmtInt(v),pl+pw+6,y+3);}
    // x 轴标签
    const nx=Math.max(2,Math.min(6,Math.floor(pw/110)));
    for(let k=0;k<=nx;k++){const i=Math.round(k/nx*(t.length-1));
      c.fillText(hhmm(t[i]),X(i)-14,h-6);}
    // 阈值线
    if(this.cfg.thresh!=null){const y=Y(this.cfg.thresh);
      c.strokeStyle="rgba(251,113,133,.5)";c.setLineDash([4,4]);c.beginPath();c.moveTo(pl,y);c.lineTo(pl+pw,y);c.stroke();c.setLineDash([]);}
    // 序列
    for(const s of this.cfg.series){
      const arr=map[s.key]||[];
      c.strokeStyle=s.color;c.lineWidth=1.8;c.lineJoin="round";
      if(s.fill){const g=c.createLinearGradient(0,pt,0,pt+ph);
        g.addColorStop(0,s.color+"44");g.addColorStop(1,s.color+"05");
        c.fillStyle=g;}
      c.beginPath();let started=false,firstX=0,firstY=0,lastX=0,lastY=0;
      for(let i=0;i<arr.length;i++){const v=arr[i];if(v==null){continue;}
        const x=X(i),y=Y(Math.min(v,ymax));
        if(!started){c.moveTo(x,y);started=true;firstX=x;firstY=y;}else c.lineTo(x,y);
        lastX=x;lastY=y;}
      if(started){
        if(s.fill){c.save();c.lineTo(lastX,pt+ph);c.lineTo(firstX,pt+ph);c.closePath();c.fill();c.restore();
          c.beginPath();let st2=false;
          for(let i=0;i<arr.length;i++){const v=arr[i];if(v==null)continue;
            const x=X(i),y=Y(Math.min(v,ymax));if(!st2){c.moveTo(x,y);st2=true;}else c.lineTo(x,y);}}
        c.stroke();}
    }
    // hover
    if(this.hover>=0&&this.hover<t.length){
      const x=X(this.hover);
      c.strokeStyle="rgba(160,190,255,.35)";c.beginPath();c.moveTo(x,pt);c.lineTo(x,pt+ph);c.stroke();
      for(const s of this.cfg.series){const v=map[s.key][this.hover];
        if(v==null)continue;c.fillStyle=s.color;c.beginPath();c.arc(x,Y(Math.min(v,ymax)),3,0,7);c.fill();}
    }
  }
}
function niceStep(x){const p=Math.pow(10,Math.floor(Math.log10(x||1)));const m=x/p;
  return (m>=5?10:m>=2?5:m>=1?2:1)*p;}

const CH={};
function buildCharts(){
  const mk=(id,legend,series,extra={})=>{CH[id]=new Chart(id,Object.assign({series},extra));
    $(legend).innerHTML=series.map(s=>`<span><i style="background:${s.color}"></i>${s.label}</span>`).join("");};
  mk("ch-dec","lg-dec",[
    {key:"gen_rate",label:"Decode tok/s",color:"#22d3ee",fill:true}]);
  mk("ch-pre","lg-pre",[
    {key:"prompt_rate",label:"Prefill tok/s",color:"#a78bfa",fill:true}]);
  mk("ch-kv","lg-kv",[
    {key:"kv_perc",label:"已用比例",color:"#34d399",fill:true}],
    {thresh:0.9,axisFmt:v=>{const p=v*100;return (p>=10?p.toFixed(0):p>=1?p.toFixed(1):p.toFixed(2))+"%";},fmt:v=>fmtPct(v)});
  mk("ch-cc","lg-cc",[
    {key:"running",label:"运行中",color:"#60a5fa",fill:true},
    {key:"waiting",label:"排队",color:"#fbbf24",fill:true}]);
  mk("ch-lat","lg-lat",[
    {key:"ttft_p95",label:"TTFT p95",color:"#a78bfa"},
    {key:"tpot_p95",label:"TPOT p95",color:"#fbbf24"},
    {key:"e2e_p95",label:"端到端 p95",color:"#fb7185"},
    {key:"queue_p95",label:"排队 p95",color:"#38bdf8"}],
    {fmt:fmtMs,axisFmt:fmtMs,logY:true});
  CH["ch-lat"].cfg.fmt=fmtMs;
  mk("ch-ovdec","lg-ovdec",[{key:"gen_rate",label:"Decode 合计 tok/s",color:"#22d3ee",fill:true}]);
  mk("ch-ovpre","lg-ovpre",[{key:"prompt_rate",label:"Prefill 合计 tok/s",color:"#a78bfa",fill:true}]);
  mk("ch-ovcc","lg-ovcc",[
    {key:"running",label:"运行中",color:"#60a5fa",fill:true},
    {key:"waiting",label:"排队",color:"#fbbf24",fill:true}]);
  mk("ch-ovcum","lg-ovcum",[]);
}
function drawSpark(id,key){
  const el=$(id);if(!el||!data)return;const col=data.series[key]||[];
  const w=66,h=27,dpr=devicePixelRatio||1;
  if(el.width!==w*dpr){el.width=w*dpr;el.height=h*dpr;}
  const c=el.getContext("2d");c.setTransform(dpr,0,0,dpr,0,0);c.clearRect(0,0,w,h);
  const pts=col.slice(-90).map(v=>v==null?null:v);
  let mx=0;for(const v of pts)if(v!=null&&v>mx)mx=v;if(!mx)return;
  c.strokeStyle="#67e8f9";c.lineWidth=1.2;c.beginPath();let st=false;
  pts.forEach((v,i)=>{if(v==null)return;const x=i/(pts.length-1)*w,y=h-2-(v/mx)*(h-5);
    if(!st){c.moveTo(x,y);st=true;}else c.lineTo(x,y);});
  c.stroke();
}

/* ---------------- 数据轮询 ---------------- */
async function pollTargets(){
  try{
    const r=await fetch("/api/targets");const j=await r.json();
    targets=j.targets||[];
    if(activeId!==OV&&!targets.some(t=>t.id===activeId))activeId=OV;
    renderTabs();
  }catch(e){}
}
async function pollData(){
  if(document.hidden)return;
  if(activeId===OV){pollOverview();return;}
  try{
    const r=await fetch("/api/data?id="+encodeURIComponent(activeId)+"&maxpoints=1500");
    data=await r.json();
    if(data.error){data=null;return;}
    render();
  }catch(e){}
}
function switchView(){
  const ov=activeId===OV;
  $("mainOv").style.display=ov?"block":"none";
  $("mainSingle").style.display=ov?"none":"block";
  if(ov)$("banner").style.display="none";
}
function renderTabs(){
  const box=$("tabs");box.innerHTML="";
  const ov=document.createElement("div");
  const allUp=targets.length>0&&targets.every(t=>t.status==="up");
  ov.className="tab"+(activeId===OV?" active":"");
  ov.innerHTML=`<span class="led ${targets.length?(allUp?"up":"down"):""}"></span><span class="nm">主屏</span>`;
  ov.title="主屏（全场合计，固定）";
  ov.onclick=()=>{activeId=OV;renderTabs();pollData();};
  box.appendChild(ov);
  // 机器多了改下拉选择（主屏固定不动）
  if(targets.length){
    const sel=document.createElement("select");
    sel.className="tsel";
    sel.innerHTML=(activeId===OV?`<option value="" selected disabled>—— 选择小屏 ——</option>`:"")+
      targets.map(t=>`<option value="${t.id}" ${t.id===activeId?"selected":""}>${t.status==="up"?"●":"○"} ${esc(t.name)}</option>`).join("");
    sel.onchange=()=>{if(!sel.value)return;activeId=sel.value;renderTabs();pollData();};
    box.appendChild(sel);
    const del=document.createElement("span");
    del.className="tbtn danger";del.textContent="✕ 移除";del.title="移除当前选中的服务";
    del.style.display=activeId===OV?"none":"inline-block";
    del.onclick=()=>{
      const t=targets.find(x=>x.id===activeId);if(!t)return;
      if(confirm("移除目标 "+t.name+"？")){fetch("/api/targets/remove",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id:t.id})}).then(()=>{activeId=OV;pollTargets();});}
    };
    box.appendChild(del);
  }
  switchView();
}
/* ---------------- 全场总览（固定第一页签） ---------------- */
function sumSeries(list,key){
  if(!list.length)return {t:[],v:[]};
  let n=Infinity,bestT=[];
  for(const s of list){
    n=Math.min(n,(s[key]||[]).length);
    if((s.t||[]).length>bestT.length)bestT=s.t;
  }
  if(!isFinite(n)||n<2)return {t:[],v:[]};
  const out=[];
  for(let i=0;i<n;i++){
    let sum=0,got=false;
    for(const s of list){const val=(s[key]||[])[i];if(val!=null){sum+=val;got=true;}}
    out.push(got?sum:null);
  }
  return {t:bestT.slice(bestT.length-n),v:out};
}
async function pollOverview(){
  let ov=null;
  try{const r=await fetch("/api/overview");ov=await r.json();}catch(e){}
  if(!ov)return;
  ovData=ov;
  // 并行拉各机 series，做合计/对比曲线
  const res=await Promise.all(targets.map(t =>
    fetch("/api/data?id="+encodeURIComponent(t.id)+"&maxpoints=900").then(r=>r.json()).catch(()=>null)));
  const sers=res.filter(d=>d&&d.series&&d.series.t);
  ovSeries=sers;
  if(activeId===OV){
    const ser=sers.map(d=>d.series);   // 合计求和在 series 层做（fix：此前误传整个 data 载荷导致合计图恒空）
    const dec=sumSeries(ser,"gen_rate"),pre=sumSeries(ser,"prompt_rate"),
          run=sumSeries(ser,"running"),wait=sumSeries(ser,"waiting");
    CH["ch-ovdec"].setData(dec.t,{gen_rate:dec.v});
    CH["ch-ovpre"].setData(pre.t,{prompt_rate:pre.v});
    CH["ch-ovcc"].setData(run.t,{running:run.v,waiting:wait.v});
    // 各机累计 Decode 对比（每机一条线，key=目标 id）
    const cumCfg=[],cumMap={};
    for(const d of sers){
      const col=["#22d3ee","#a78bfa","#34d399","#fbbf24","#fb7185","#38bdf8","#f472b6","#facc15"];
      cumCfg.push({key:d.id,label:d.name,color:col[cumCfg.length%col.length]});
      cumMap[d.id]=d.series.gen_tokens_total||[];
    }
    const cumT=(sers[0]&&sers[0].series.t)||[];
    let cumN=Infinity;for(const k in cumMap)cumN=Math.min(cumN,cumMap[k].length);
    if(!isFinite(cumN))cumN=0;
    const mm={};for(const c of cumCfg)mm[c.key]=(cumMap[c.key]||[]).slice(cumMap[c.key].length-cumN);
    const ct=(sers[0].series.t||[]).slice(cumT.length-cumN);
    if(JSON.stringify(CH["ch-ovcum"].cfg.series.map(s=>s.key))!==JSON.stringify(cumCfg.map(s=>s.key))){
      CH["ch-ovcum"].cfg.series=cumCfg;
      $("lg-ovcum").innerHTML=cumCfg.map(s=>`<span><i style="background:${s.color}"></i>${esc(s.label)}</span>`).join("");
    }
    CH["ch-ovcum"].setData(ct,mm);
    renderOverview(ov);
  }
}
function renderOverview(ov){
  const t=ov.totals;
  const downs=ov.targets.filter(x=>x.status!=="up").map(x=>x.name);
  const kpis=[
    {lbl:"引擎机器",val:ov.n_up+" / "+ov.n_total,unit:"台在线",sub:downs.length?("离线: "+downs.join(", ")):"全部在线",
     cls:downs.length?"c-rose":"c-green"},
    {lbl:"Decode 吞吐 合计",val:fmtRate(t.gen_rate),unit:"tok/s",sub:"全场 "+ov.n_up+" 台求和",cls:"c-cyan"},
    {lbl:"Prefill 吞吐 合计",val:fmtRate(t.prompt_rate),unit:"tok/s",cls:"c-violet"},
    {lbl:"运行中请求 合计",val:fmtInt(t.running),unit:"路",sub:"排队 "+fmtInt(t.waiting)+" 路",cls:"c-blue"},
    {lbl:"KV Cache 最高",val:ov.kv_max==null?"—":fmtPct(ov.kv_max),unit:"",
     sub:ov.kv_max_name?("最高: "+ov.kv_max_name):"",cls:(ov.kv_max!=null&&ov.kv_max>=0.9)?"c-rose":"c-green"},
    {lbl:"TPOT p95 均值",val:t.tpot_p95==null?"—":fmtMs(t.tpot_p95),unit:"",sub:"各机 p95 平均",cls:"c-amber"},
    {lbl:"前缀缓存命中",val:t.prefix_hit==null?"—":fmtPct(t.prefix_hit),unit:"",sub:"各机平均",cls:"c-green"},
    {lbl:"投机接受率",val:t.spec_accept==null?"—":fmtPct(t.spec_accept),unit:"",sub:"各机平均",cls:"c-violet"},
    {lbl:"全场累计生成",val:fmtTok(t.gen_tokens_total),unit:"tok",
     sub:"完成 "+fmtInt(t.finished)+" · 预填 "+fmtTok(t.prompt_tokens_total)+" · 抢占 "+fmtInt(t.preemptions),cls:"c-cyan"}];
  $("ovKpis").innerHTML=kpis.map(k=>`
    <div class="kpi ${k.cls||""}"><div class="lbl"><span>${k.lbl}</span></div>
    <div class="val">${k.val}${k.unit?`<span class="unit">${k.unit}</span>`:""}</div>
    <div class="sub">${esc(k.sub||"")}</div></div>`).join("");
  const al=$("ovAlarm");
  if(downs.length){al.style.display="block";
    al.innerHTML=`⚠ <b>掉线报警：${downs.length} 台离线</b> —— ${esc(downs.join("、"))}（其余 ${ov.n_up} 台正常）`;
  }else al.style.display="none";
  $("tb-ovsum").innerHTML=`
   <tr><td>监控目标</td><td>${ov.n_total} 个（${ov.n_up} 在线）</td></tr>
   <tr><td>在线机器</td><td>${ov.n_up} 台在线</td></tr>
   ${ov.gw?`<tr><td>API 网关</td><td>${ov.gw.enabled?"<span style='color:#6ee7b7'>运行中 ✓</span>":"已暂停"} · 端口 ${ov.gw.port} · 策略 ${{weight:"加权随机",round_robin:"轮询",p2c:"P2C",least_requests:"最少负载",session:"会话亲和"}[ov.gw.policy]||ov.gw.policy}</td></tr>`
          :`<tr><td>API 网关</td><td>未启用（--gw）</td></tr>`}
   <tr><td>汇总口径</td><td>吞吐/并发为各机求和；TPOT 为各机 p95 平均；KV 取最高</td></tr>`;
  $("ovNodes").innerHTML=ov.targets.map(r=>{
    const host=((r.url||"").replace(/^https?:\/\//,"")
      .replace(/^(127\.0\.0\.1|localhost)(:|\/|$)/,location.hostname+"$2"));
    const kv=r.kv_perc==null?"—":fmtPct(r.kv_perc);
    const n=r.node, nw=(n&&n.now)||{};
    const mem=nw.mem_total?`${nw.mem_used.toFixed(0)} / ${nw.mem_total.toFixed(0)} GB（${nw.mem_perc!=null?Math.round(nw.mem_perc):"—"}%）`:"—";
    const cn=(n.cpu_name||"").replace(/^(AMD|Intel\(R\)|Intel)\s+/,"").replace(/\s*\S*[Cc]ore\s*[Pp]rocessor$/,"").replace(/\s+Processor$/,"");
    const gn=(n.gpu_name||"").replace(/^NVIDIA\s+/,"");
    const osn=(n.os_pretty||"").replace(/\s+LTS$/,"");
    return `<div class="ovnode" data-id="${r.id}">
      <div class="ovhead"><span class="led ${r.status}"></span><b>${esc(r.name)}</b>
        <span class="dim mono">${esc(host)}</span></div>
      ${n?`
      <div class="ln">CPU ${nw.cpu!=null?Math.round(nw.cpu):"—"}% · ${esc(cn||"?")} · ${n.cpu_cores||"?"} 核</div>
      <div class="ln">GPU ${nw.gpu_util!=null?Math.round(nw.gpu_util):"—"}% · ${esc(gn||"—")}${n.per_gpu&&n.per_gpu.length?" × "+n.per_gpu.length:""}</div>
      <div class="ln">内存 ${mem}${n.power_w!=null?" · 功率 "+Math.round(n.power_w)+"W":""}</div>
      <div class="ln dim">${esc(osn||"—")} · 运行 ${fmtUp(n.uptime_s)}</div>`
      :`<div class="ln dim">宿主机信息未接入（在该机 :8124 跑一个同款大屏实例即自动接入 CPU/GPU/内存）</div>`}
    </div>`;}).join("")||`<div class="dim" style="padding:10px">暂无监控目标（⚙ → ＋ 添加监控目标）</div>`;
  $("ovNodes").onclick=e=>{
    const nd=e.target.closest(".ovnode");if(!nd)return;
    activeId=nd.dataset.id;renderTabs();pollData();};
  const tb=$("tb-ovmach").querySelector("tbody");
  tb.innerHTML=ov.targets.map(r=>{
    const kv=r.kv_perc==null?"—":fmtPct(r.kv_perc);
    return `<tr class="ovrow" data-id="${r.id}">
      <td><span class="led ${r.status}"></span></td>
      <td class="nm">${esc(r.name)}</td>
      <td>${fmtRate(r.gen_rate)}</td><td>${fmtRate(r.prompt_rate)}</td>
      <td>${fmtInt(r.running)} / ${fmtInt(r.waiting)}</td>
      <td>${kv}</td><td>${r.tpot_p95==null?"—":fmtMs(r.tpot_p95)}</td>
      <td>${r.has_pc&&r.prefix_hit!=null?fmtPct(r.prefix_hit):"—"}</td>
      <td>${r.has_spec&&r.spec_accept!=null?fmtPct(r.spec_accept):"—"}</td>
      <td>${fmtTok(r.gen_tokens_total)}</td>
      <td class="mono" style="color:#cfe3ff">${esc(r.model||"—")}</td>
      <td>${r.context_len?fmtCtx(r.context_len):"—"}</td>
      <td class="mono" style="color:var(--dim)">${r.version?esc(r.version):"—"}</td>
    </tr>`;}).join("");
  tb.onclick=e=>{
    const tr=e.target.closest("tr.ovrow");if(!tr)return;
    activeId=tr.dataset.id;renderTabs();pollData();};
}
function render(){
  const d=data;if(!d)return;
  const b=$("banner");
  if(d.status==="down"){b.style.display="block";b.innerHTML=`⚠ 目标 <b>${esc(d.name)}</b> 抓取失败：${esc(d.last_error||"")}（URL: ${esc(d.url)}/metrics）`;}
  else b.style.display="none";
  // KPI
  for(const k of KPIS){
    const el=$("kpi-"+k.id);
    const show=(k.id!=="pc"||d.has_prefix_cache)&&(k.id!=="spec"||d.has_spec);
    el.classList.toggle("hidden",!show);if(!show)continue;
    if(k.render)k.render(el,d);
    else{el.querySelector(".val").innerHTML=k.fmt(d.now[k.key])+`<span class="unit">${k.unit}</span>`;
      const sub=el.querySelector(".sub");if(sub)sub.textContent=k.sub?k.sub(d):"";
      drawSpark("sp-"+k.id,k.key);}
  }
  // charts
  CH["ch-dec"].setData(d.series.t,d.series);
  CH["ch-pre"].setData(d.series.t,d.series);
  CH["ch-kv"].setData(d.series.t,d.series);
  CH["ch-cc"].setData(d.series.t,d.series);
  CH["ch-lat"].setData(d.series.t,d.series);
  // tables
  $("tb-req").innerHTML=`
   <tr><td>运行 / 排队 / 换出</td><td>${fmtInt(d.now.running)} / ${fmtInt(d.now.waiting)} / ${fmtInt(d.now.swapped)}</td></tr>
   <tr><td>Decode / Prefill 吞吐</td><td>${fmtRate(d.now.gen_rate)} / ${fmtRate(d.now.prompt_rate)} tok/s</td></tr>
   <tr><td>TTFT p50 / p95</td><td>${fmtMs(d.now.ttft_p50)} / ${fmtMs(d.now.ttft_p95)}</td></tr>
   <tr><td>TPOT p50 / p95</td><td>${fmtMs(d.now.tpot_p50)} / ${fmtMs(d.now.tpot_p95)}</td></tr>
   <tr><td>端到端 / 排队 p95</td><td>${fmtMs(d.now.e2e_p95)} / ${fmtMs(d.now.queue_p95)}</td></tr>
   ${d.has_spec?`<tr><td>投机接受率</td><td>${fmtPct(d.now.spec_accept)}</td></tr>`:""}
   ${d.has_prefix_cache?`<tr><td>前缀缓存命中</td><td>${fmtPct(d.now.prefix_hit)}</td></tr>`:""}`;
  $("tb-info").innerHTML=`
   <tr><td>名称 / 状态</td><td>${esc(d.name)} · ${d.status==="up"?"<span style='color:#6ee7b7'>正常</span>":"<span style='color:#fda4af'>失联</span>"}</td></tr>
   <tr><td>metrics 地址</td><td class="mono">${esc(d.url)}/metrics</td></tr>
   <tr><td>抓取间隔 / 保留点数</td><td>${d.interval}s / ${d.meta.points}</td></tr>
   <tr><td>最近抓取</td><td>${d.last_ts?hhmmss(d.last_ts):"—"}</td></tr>
   <tr><td>KV 池容量</td><td>${d.kv_max_tokens?fmtTok(d.kv_max_tokens)+" token":"—"}</td></tr>`;
  const mi=d.meta_info||{};
  const mid=(mi.model_ids&&mi.model_ids.length)?mi.model_ids.join(", "):
            (d.model_path?d.model_path.split("/").filter(Boolean).pop():"—");
  let ver=mi.version||"";
  try{const j=JSON.parse(ver);ver=(j.version||ver)+(j.rust_frontend_version?" (rust fe "+j.rust_frontend_version+")":"");}catch(e){}
  const apiBase=d.url.replace(/^(https?:\/\/)(127\.0\.0\.1|localhost)(:|\/)/,"$1"+location.hostname+"$3");
  $("tb-model").innerHTML=`
   <tr><td>模型 ID</td><td class="mono">${esc(mid)}</td></tr>
   <tr><td>上下文长度</td><td>${d.context_len?esc(d.context_len):"—"}</td></tr>
   <tr><td>权重路径</td><td class="mono">${d.model_path?esc(d.model_path):"—"}</td></tr>
   <tr><td>接入地址</td><td class="mono">${esc(apiBase)}/v1</td></tr>
   <tr><td>API Key</td><td class="mono">${d.api_key?esc(d.api_key):"（无鉴权）"}</td></tr>
   <tr><td>vLLM 版本</td><td>${ver?esc(ver):"—"}</td></tr>`;
  $("tb-total").innerHTML=`
   <tr><td>累计生成 token</td><td>${fmtTok(d.now.gen_tokens_total)}</td></tr>
   <tr><td>累计预填充 token</td><td>${fmtTok(d.now.prompt_tokens_total)}</td></tr>
   <tr><td>KV 抢占次数</td><td>${fmtInt(d.now.preemptions)}</td></tr>
   <tr><td>监控已运行</td><td>${uptimeStr(d.monitor_since)}</td></tr>`;
}
function fmtUp(s){if(s==null)return"—";const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);
  return (d?d+" 天 ":"")+(d||h?h+" 小时 ":"")+m+" 分";}
function fmtCtx(s){const v=parseInt(s);if(!v)return"";return v%1048576===0?(v/1048576)+"M":v%1024===0?(v/1024)+"K":String(v);}
function uptimeStr(ts){const s=Math.max(0,Date.now()/1000-ts);const m=Math.floor(s/60);
  return m>=60?Math.floor(m/60)+" 小时 "+(m%60)+" 分":m+" 分 "+Math.floor(s%60)+" 秒";}
function esc(s){return String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));}

/* ---------------- 交互 ---------------- */
/* ---------------- 交互：齿轮菜单 ---------------- */
const gearBtn=$("gearBtn"),gearMenu=$("gearMenu");
gearBtn.onclick=e=>{e.stopPropagation();
  const open=gearMenu.style.display==="block";
  gearMenu.style.display=open?"none":"block";
  gearBtn.classList.toggle("open",!open);};
document.addEventListener("click",e=>{
  if(!e.target.closest(".gearwrap")){gearMenu.style.display="none";gearBtn.classList.remove("open");}});
$("miAdd").onclick=()=>{gearMenu.style.display="none";$("mErr").textContent="";
  $("modalBg").style.display="flex";$("mName").focus();};
$("miExport").onclick=()=>{gearMenu.style.display="none";doExport();};
$("miGw").onclick=()=>{gearMenu.style.display="none";gwOpen();};
function dlText(name,text,mime){
  const blob=new Blob([text],{type:mime||"application/json;charset=utf-8"});
  const a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=name;a.click();
  setTimeout(()=>URL.revokeObjectURL(a.href),2000);
}
async function dlApi(path,name,mime){
  try{const r=await fetch(path);const t=await r.text();dlText(name,t,mime);toast(name+" 已下载");}
  catch(e){toast("下载失败:"+e);}
}
$("miCfgExport").onclick=()=>{gearMenu.style.display="none";dlApi("/api/config/export","ai-dashboard-config.json");};
$("miCfgExample").onclick=()=>{gearMenu.style.display="none";dlApi("/api/config/example","ai-dashboard-config-example.json");};
$("miAgentMd").onclick=()=>{gearMenu.style.display="none";dlApi("/agent.md","agent.md","text/markdown;charset=utf-8");};
$("miCfgImport").onclick=()=>{gearMenu.style.display="none";$("cfgFile").value="";$("cfgFile").click();};
$("cfgFile").onchange=async e=>{
  const f=e.target.files[0];if(!f)return;
  try{
    const cfg=JSON.parse(await f.text());
    if(!confirm("导入将整体替换现有监控目标与网关渠道（客户端 Key 保留），确认？"))return;
    const r=await fetch("/api/config/import",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({config:cfg})});
    const j=await r.json();
    toast(j.message||("导入"+(j.ok?"成功":"失败")));
    if(j.ok){pollTargets();setTimeout(pollData,400);}
  }catch(err){toast("导入失败：JSON 解析错误 "+err.message.slice(0,60));}
};
$("gwCloseX").onclick=gwClose;
$("gwModalBg").onclick=e=>{if(e.target===$("gwModalBg"))gwClose();};
$("gwToggleBtn").onclick=async()=>{const j=await gwPost("/api/gateway/toggle");if(j.ok)gwPoll();};
$("gwPolicy").onchange=async()=>{
  const j=await gwPost("/api/gateway/policy",{policy:$("gwPolicy").value});
  if(j.ok){toast("负载均衡策略已切换");}else{toast("策略设置失败");$("gwPolicy").value=gwData?gwData.policy:"weight";}
};
$("gcToggle").onclick=()=>{const el=$("gcSlide");el.classList.toggle("open");$("gcToggle").textContent=el.classList.contains("open")?"－ 收起":"＋ 添加渠道";};
$("gkToggle").onclick=()=>{const el=$("gkSlide");el.classList.toggle("open");$("gkToggle").textContent=el.classList.contains("open")?"－ 收起":"＋ 生成 Key";};
$("gcAdd").onclick=async()=>{
  const j=await gwPost("/api/gateway/channel",{name:$("gcName").value.trim(),
    url:$("gcUrl").value.trim(),api_key:$("gcKey").value.trim(),
    priority:parseInt($("gcPri").value||"1"),weight:parseInt($("gcWt").value||"10")});
  if(j.ok){$("gcName").value=$("gcUrl").value=$("gcKey").value="";toast("渠道已添加");$("gcSlide").classList.remove("open");$("gcToggle").textContent="＋ 添加渠道";gwPoll();}
  else toast(j.error||"添加失败");};
$("gkGen").onclick=async()=>{
  const j=await gwPost("/api/gateway/key",{name:$("gkName").value.trim(),
    rpm:parseInt($("gkRpm").value||"0")||0,quota_tokens:parseInt($("gkQuota").value||"0")||0});
  if(j.ok){$("gkName").value=$("gkRpm").value=$("gkQuota").value="";$("gkSlide").classList.remove("open");$("gkToggle").textContent="＋ 生成 Key";
    gwPoll();setTimeout(()=>{
      copyText(gwAccessSnippet(j.key),"Key 已生成，接入信息已复制，直接发给同事即可");
    },400);}
  else toast("生成失败");};
$("gwChanT").onclick=async e=>{
  const a=e.target.closest("a.op");if(!a)return;
  const j=await gwPost("/api/gateway/channel/op",{name:a.dataset.n,op:a.dataset.op});
  if(j.ok)gwPoll();};
$("gwKeyT").onclick=async e=>{
  const td=e.target.closest("td.key");
  if(td){copyText(td.textContent.trim(),"Key 已复制");return;}
  const a=e.target.closest("a.kop");if(!a)return;
  if(a.dataset.op==="access"){copyText(gwAccessSnippet(a.dataset.k),"接入信息已复制");return;}
  if(a.dataset.op==="delete"&&!confirm("删除该 Key？"))return;
  const j=await gwPost("/api/gateway/key/op",{key:a.dataset.k,op:a.dataset.op});
  if(j.ok)gwPoll();};

/* ---------------- API 分发网关面板 ---------------- */
let gwData=null, gwTimer=null;

function gwOpen(){
  $("gwModalBg").style.display="flex";
  gwPoll();
  if(gwTimer)clearInterval(gwTimer);
  gwTimer=setInterval(()=>{if($("gwModalBg").style.display==="flex")gwPoll();},4000);
}
function gwClose(){
  $("gwModalBg").style.display="none";
  if(gwTimer){clearInterval(gwTimer);gwTimer=null;}
}
async function gwPoll(){
  try{const r=await fetch("/api/gateway");gwData=await r.json();gwRender();}catch(e){}
}
function gwModelName(){
  const mi=(data&&data.meta_info)||{};
  return (mi.model_ids&&mi.model_ids[0])||"deepseek-v4.1-flash";
}
function gwAccessSnippet(key){
  const base="http://"+location.hostname+":"+gwData.port+"/gw/v1";
  const m=gwModelName();
  return "接入地址: "+base+"\nAPI Key:  "+key+"\n模型名:   "+m+"\n\n"
    +'curl '+base+'/chat/completions \\\n'
    +'  -H "Authorization: Bearer '+key+'" -H "Content-Type: application/json" \\\n'
    +"  -d '{\"model\":\""+m+"\",\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}]}'\n\n"
    +"# Python (openai SDK)\n"
    +"from openai import OpenAI\n"
    +'client = OpenAI(base_url="'+base+'", api_key="'+key+'")\n'
    +'print(client.chat.completions.create(model="'+m+'",\n'
    +'    messages=[{"role":"user","content":"你好"}]).choices[0].message.content)';
}
function copyText(t,tip){
  tip=tip||"已复制";
  const done=()=>toast(tip);
  if(navigator.clipboard&&navigator.clipboard.writeText){
    navigator.clipboard.writeText(t).then(done).catch(()=>fallbackCopy(t,done));return;}
  fallbackCopy(t,done);
}
function fallbackCopy(t,done){
  const ta=document.createElement("textarea");ta.value=t;
  ta.style.position="fixed";ta.style.opacity="0";
  document.body.appendChild(ta);ta.select();
  try{document.execCommand("copy");done();}catch(e){toast("复制失败，请手动复制");}
  document.body.removeChild(ta);
}
let toastTimer=null;
function toast(msg){
  let el=$("toastTip");
  if(!el){el=document.createElement("div");el.id="toastTip";
    el.style.cssText="position:fixed;bottom:28px;left:50%;transform:translateX(-50%);"+
      "background:rgba(13,20,40,.95);border:1px solid rgba(34,211,238,.4);color:#aef3ff;"+
      "padding:9px 18px;border-radius:10px;font-size:13px;z-index:200;pointer-events:none";
    document.body.appendChild(el);}
  el.textContent=msg;el.style.display="block";
  if(toastTimer)clearTimeout(toastTimer);
  toastTimer=setTimeout(()=>{el.style.display="none";},1800);
}
function gwRender(){
  const g=gwData;if(!g)return;
  if(g.off){$("gwOff").style.display="block";$("gwBody").style.display="none";return;}
  $("gwOff").style.display="none";$("gwBody").style.display="block";
  const st=$("gwState");
  st.textContent=g.enabled?"转发中 ✓":"已暂停";
  st.style.color=g.enabled?"#6ee7b7":"#fda4af";
  $("gwPort").textContent=g.port;
  $("gwBase").textContent="http://"+location.hostname+":"+g.port+"/gw/v1";
  $("gwToggleBtn").textContent=g.enabled?"⏸ 暂停转发":"▶ 开启转发";
  $("gwPolicy").value=g.policy||"weight";
  const t=g.totals;
  $("gwStats").innerHTML=
    '<span class="gs"><b>'+t.requests+'</b> 总请求</span>'
    +'<span class="gs"><b>'+t.ok+'</b> 成功</span>'
    +'<span class="gs"><b>'+t.fail+'</b> 失败</span>'
    +'<span class="gs"><b>'+fmtTok(t.tokens)+'</b> token</span>'
    +'<span class="gs"><b>'+(g.qps_1m||0)+'</b> QPS(1m)</span>'
    +(g.eng_running!=null?'<span class="gs"><b>'+fmtInt(g.eng_running)+'</b> 引擎运行</span>'
      +' <span class="gs"><b>'+fmtInt(g.eng_gen_rate)+'</b> Decode tok/s 合计</span>':'');
  // 负载均衡总表
  const tb=$("gwChanT").querySelector("tbody");tb.innerHTML="";
  for(const c of g.channels){
    const stTxt={ok:"<span style='color:#6ee7b7'>正常</span>",
                 paused:"<span style='color:#fcd34d'>已停用</span>",
                 broken:"<span style='color:#fda4af'>熔断 "+c.broken_until+"s</span>"}[c.status];
    const pb=c.probe_ok==null?"<span style='color:#94a3b8'>探测中</span>":
      (c.probe_ok?"<span style='color:#6ee7b7'>"+c.probe_ms+"ms</span>":
                  "<span style='color:#fda4af'>不通</span>");
    let eng="—<span class='dim'>（未监控）</span>";
    if(c.engine&&c.engine.running!=null){
      const kv=c.engine.kv_perc!=null?Math.round(c.engine.kv_perc*100)+"%":"—";
      eng=fmtInt(c.engine.running)+" / "+fmtInt(c.engine.waiting||0)+" · "+kv+" · "
         +fmtInt(c.engine.gen_rate||0)+" tok/s";
    }
    const tr=document.createElement("tr");
    tr.innerHTML='<td>'+esc(c.name)+'</td><td class="mono">'+esc(c.base_url)+'</td>'
      +'<td>'+c.priority+' / '+c.weight+'</td><td>'+stTxt+'</td><td>'+pb+'</td>'
      +'<td>'+c.inflight+'</td>'
      +'<td>'+c.requests+' / <span style="color:#fda4af">'+(c.fail||0)+'</span></td>'
      +'<td>'+c.qps_1m+'</td><td>'+fmtTok(c.tokens)+'</td>'
      +'<td>'+(c.avg_latency_ms||0)+'ms</td><td>'+eng+'</td>'
      +'<td><a class="op" data-op="toggle" data-n="'+esc(c.name)+'">'+(c.status==="paused"?"启用":"停用")+'</a> '
      +'<a class="op" data-op="delete" data-n="'+esc(c.name)+'">删除</a></td>';
    tb.appendChild(tr);
  }
  const tf=$("gwChanT").querySelector("tfoot");
  if(tf&&g.channels.length){
    tf.innerHTML='<tr><td colspan="7" style="color:var(--dim)">合计（'+g.channels.length+' 渠道）</td>'
      +'<td>'+fmtInt(g.qps_1m||0)+'</td>'
      +'<td>'+fmtTok(t.tokens)+'</td><td>—</td>'
      +'<td>'+(g.eng_running!=null?fmtInt(g.eng_running)+' 路 · '+fmtInt(g.eng_gen_rate)+' tok/s':'—')+'</td><td>—</td></tr>';
  }
  // Key 表
  const kb=$("gwKeyT").querySelector("tbody");kb.innerHTML="";
  for(const k of g.keys){
    const quota=k.quota_tokens?fmtTok(k.quota_tokens):"不限";
    const used=k.quota_tokens&&k.used_tokens>=k.quota_tokens?"（已超）":"";
    const tr=document.createElement("tr");
    tr.innerHTML='<td>'+esc(k.name)+'</td>'
      +'<td class="mono key" title="点击复制 Key">'+esc(k.key)+'</td>'
      +'<td>'+(k.rpm||"不限")+'</td>'
      +'<td>'+quota+' / '+fmtTok(k.used_tokens)+used+'</td>'
      +'<td>'+k.used_requests+'</td>'
      +'<td>'+(k.enabled?"<span style='color:#6ee7b7'>启用</span>":"<span style='color:#fda4af'>停用</span>")+'</td>'
      +'<td><a class="op kop" data-op="access" data-k="'+esc(k.key)+'">复制接入</a> '
      +'<a class="op kop" data-op="toggle" data-k="'+esc(k.key)+'">'+(k.enabled?"停用":"启用")+'</a> '
      +'<a class="op kop" data-op="delete" data-k="'+esc(k.key)+'">删除</a></td>';
    kb.appendChild(tr);
  }
}
async function gwPost(path,body){
  try{const r=await fetch(path,{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify(body||{})});
    return await r.json();}catch(e){return {error:String(e)};}
}


function doExport(){
  if(!data||!data.series||!data.series.t||!data.series.t.length){alert("当前无数据可导出");return;}
  const keys=["gen_rate","prompt_rate","running","waiting","kv_perc",
    "ttft_p50","ttft_p95","tpot_p50","tpot_p95","e2e_p95","queue_p95","spec_accept","prefix_hit"];
  const names={gen_rate:"decode_tok_s",prompt_rate:"prefill_tok_s",running:"running",waiting:"waiting",
    kv_perc:"kv_used",ttft_p50:"ttft_p50_s",ttft_p95:"ttft_p95_s",tpot_p50:"tpot_p50_s",tpot_p95:"tpot_p95_s",
    e2e_p95:"e2e_p95_s",queue_p95:"queue_p95_s",spec_accept:"spec_accept",prefix_hit:"prefix_hit"};
  const t=data.series.t;
  const L=[];
  L.push("# AI 推理性能监控大屏 - 监控日志导出");
  L.push("# target: "+data.name+"    metrics: "+data.url+"/metrics");
  L.push("# exported_at: "+new Date().toLocaleString("zh-CN"));
  L.push("# points: "+t.length+"    interval_s: "+data.interval);
  L.push("# 注: 吞吐=tok/s(12s窗口差分); 延迟=秒(30s窗口直方图分位); kv_used=已用比例");
  L.push("time,"+keys.map(k=>names[k]).join(","));
  for(let i=0;i<t.length;i++){
    const row=[hhmmss(t[i])];
    for(const k of keys){const v=data.series[k][i];
      row.push(v==null?"":String(Math.round(v*10000)/10000));}
    L.push(row.join(","));
  }
  const blob=new Blob(["\ufeff"+L.join("\n")],{type:"text/csv;charset=utf-8"});
  const a=document.createElement("a");
  a.href=URL.createObjectURL(blob);
  a.download=(data.name||"vllm").replace(/[^\w\u4e00-\u9fa5.-]+/g,"_")+"_metrics_"+
    new Date().toISOString().replace(/[:.]/g,"-").slice(0,19)+".csv";
  a.click();setTimeout(()=>URL.revokeObjectURL(a.href),2000);
}
$("mCancel").onclick=()=>$("modalBg").style.display="none";
$("modalBg").onclick=e=>{if(e.target===$("modalBg"))$("modalBg").style.display="none";};
$("mOk").onclick=async()=>{
  try{
    const r=await fetch("/api/targets",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name:$("mName").value.trim()||"target",url:$("mUrl").value.trim(),
        api_key:$("mKey").value.trim(),model_path:$("mPath").value.trim(),context_len:$("mCtx").value.trim()})});
    const j=await r.json();
    if(j.error){$("mErr").textContent=j.error;return;}
    $("modalBg").style.display="none";
    $("mName").value=$("mUrl").value=$("mKey").value=$("mPath").value=$("mCtx").value="";
    activeId=j.id;await pollTargets();pollData();
  }catch(e){$("mErr").textContent="添加失败: "+e;}
};

/* ---------------- 服务器资源（/api/node） ---------------- */
let nodeData=null;
async function pollNode(){
  try{const r=await fetch("/api/node");nodeData=await r.json();renderNode();}
  catch(e){}
}
function setBar(el,pct,warn=70,crit=90){
  const i=el.querySelector(".bar i");if(!i)return;
  i.style.width=Math.max(0,Math.min(100,pct))+"%";
  i.className=pct>=crit?"crit":pct>=warn?"warn":"";
}
function renderNode(){
  const n=nodeData;if(!n)return;
  const strip=$("nodeStrip");
  if(!n.available){strip.style.display="none";return;}
  strip.style.display="grid";
  const now=n.now||{};
  const ndMem=$("nd-mem");
  if(n.has_proc&&now.mem_total){
    ndMem.querySelector(".val").innerHTML=now.mem_used.toFixed(0)+'<span class="unit">/ '+now.mem_total.toFixed(0)+' GB</span>';
    ndMem.querySelector(".sub").textContent=(now.mem_perc!=null?now.mem_perc.toFixed(1)+"% 已用":"");
  }else{ndMem.querySelector(".val").textContent="—";ndMem.querySelector(".sub").textContent="";}
  setBar(ndMem,now.mem_perc||0,80,92);
  const nG=(n.per_gpu||[]).length;
  function ibar(p,warn,crit){const w=Math.max(0,Math.min(100,p||0));
    const col=(p||0)>=crit?"#fb7185":(p||0)>=warn?"#fbbf24":"#34d399";
    return `<span style="display:inline-block;width:66px;height:6px;border-radius:3px;background:rgba(130,160,255,.12);vertical-align:middle;margin-left:9px"><span style="display:inline-block;width:${w}%;height:100%;border-radius:3px;background:${col}"></span></span>`;}
  $("tb-node").innerHTML=`
   <tr><td>IP / 主机名</td><td class="mono">${esc(n.ip)} / ${esc(n.hostname)}</td></tr>
   <tr><td>服务器时间</td><td>${n.time?new Date(n.time*1000).toLocaleString("zh-CN",{hour12:false}):"—"}</td></tr>
   <tr><td>运行时间</td><td>${fmtUp(n.uptime_s)}</td></tr>
   <tr><td>CPU 型号</td><td class="mono">${esc(n.cpu_name||"—")}${n.cpu_cores?" / "+n.cpu_cores+" 核":""}</td></tr>
   <tr><td>CPU 使用率</td><td>${n.has_proc&&now.cpu!=null?now.cpu.toFixed(1)+"%"+ibar(now.cpu,70,90):"—"}</td></tr>
   <tr><td>GPU 利用率</td><td>${n.has_gpu&&now.gpu_util!=null?now.gpu_util.toFixed(1)+"%"+ibar(now.gpu_util,70,95):"—"}</td></tr>
   <tr><td>整机功率</td><td>${n.power_w!=null?Math.round(n.power_w)+" W":"—"}</td></tr>
   <tr><td>GPU 型号 / 数量</td><td class="mono">${esc(n.gpu_name||"—")}${nG?" / "+nG+" 卡":""}</td></tr>
   <tr><td>系统</td><td class="mono">${esc(n.os_pretty||String(n.platform||"").slice(0,52)||"—")}</td></tr>
   <tr><td>内核</td><td class="mono">${esc(n.kernel||"—")}</td></tr>`;
}

$("ranges").onclick=e=>{
  if(e.target.tagName!=="BUTTON")return;
  for(const b of $("ranges").children)b.classList.toggle("active",b===e.target);
  rangeMin=parseInt(e.target.dataset.min);
  for(const k in CH)CH[k].draw();
};
document.addEventListener("visibilitychange",()=>{if(!document.hidden)pollData();});

/* ---------------- 启动 ---------------- */
buildKpis();buildCharts();
pollTargets();pollData();pollNode();
setInterval(pollTargets,10000);
setInterval(pollData,2000);
setInterval(pollNode,3000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
