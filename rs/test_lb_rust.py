#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Rust 网关 e2e 断言（复用 test_lb_v3 语义）"""
import json
import time
import urllib.request
import urllib.error

DP, GP = 18304, 18305
FAILED = []


def api(path, obj=None, port=DP):
    url = "http://127.0.0.1:%d%s" % (port, path)
    if obj is None:
        r = urllib.request.urlopen(url, timeout=15)
        return json.loads(r.read().decode("utf-8"))
    req = urllib.request.Request(url, data=json.dumps(obj).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    r = urllib.request.urlopen(req, timeout=15)
    return json.loads(r.read().decode("utf-8"))


def chat(key, n=1, first="ping lb"):
    ok = 0
    for i in range(n):
        req = urllib.request.Request(
            "http://127.0.0.1:%d/gw/v1/chat/completions" % GP,
            data=json.dumps({"model": "mock-model",
                             "messages": [{"role": "system", "content": first},
                                          {"role": "user", "content": "turn %d" % i}]}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + key})
        r = urllib.request.urlopen(req, timeout=60)
        j = json.loads(r.read().decode("utf-8"))
        if j.get("choices"):
            ok += 1
    return ok


def chat_stream(key):
    req = urllib.request.Request(
        "http://127.0.0.1:%d/gw/v1/chat/completions" % GP,
        data=json.dumps({"model": "mock-model", "stream": True,
                         "messages": [{"role": "user", "content": "stream test"}]}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + key})
    r = urllib.request.urlopen(req, timeout=60)
    events, usage_seen = 0, False
    for ln in r:
        ln = ln.decode("utf-8").strip()
        if ln.startswith("data:"):
            events += 1
            if '"usage"' in ln:
                usage_seen = True
    return events, usage_seen


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    if not cond:
        FAILED.append(name)
    print("  [%s] %s %s" % (tag, name, detail))


def chans():
    return {c["name"]: c for c in api("/api/gateway")["channels"]}


def main():
    key = api("/api/gateway/key", {"name": "rs-lb"})["key"]
    check("add A", api("/api/gateway/channel", {"name": "A", "url": "http://127.0.0.1:18301", "priority": 1, "weight": 1})["ok"])
    check("add B", api("/api/gateway/channel", {"name": "B", "url": "http://127.0.0.1:18302", "priority": 1, "weight": 1})["ok"])

    print("== round_robin：8 请求应 4/4 ==")
    api("/api/gateway/policy", {"policy": "round_robin"})
    n = chat(key, 8)
    c = chans()
    check("8 请求全成功", n == 8)
    check("4/4 均分", c["A"]["requests"] == 4 and c["B"]["requests"] == 4,
          "A=%d B=%d" % (c["A"]["requests"], c["B"]["requests"]))

    print("== weight：40 请求两渠道都有份 ==")
    api("/api/gateway/policy", {"policy": "weight"})
    chat(key, 40)
    c = chans()
    check("weight 分流", c["A"]["requests"] > 5 and c["B"]["requests"] > 5,
          "A=%d B=%d" % (c["A"]["requests"], c["B"]["requests"]))

    print("== p2c：20 请求 ==")
    api("/api/gateway/policy", {"policy": "p2c"})
    chat(key, 20)
    c = chans()
    check("p2c 分流", c["A"]["requests"] > 2 and c["B"]["requests"] > 2)

    print("== session 亲和：同前缀粘同机 ==")
    api("/api/gateway/policy", {"policy": "session"})
    c0 = chans()
    b0 = (c0["A"]["requests"], c0["B"]["requests"])
    n = chat(key, 8, first="sticky-X")
    c1 = chans()
    dA = c1["A"]["requests"] - b0[0]
    dB = c1["B"]["requests"] - b0[1]
    check("同会话 8 轮全成功", n == 8)
    check("同会话粘同一台", (dA == 8 and dB == 0) or (dB == 8 and dA == 0), "A+%d B+%d" % (dA, dB))
    for k in range(16):
        chat(key, 1, first="uniq-%d" % k)
    c2 = chans()
    dA2 = c2["A"]["requests"] - c1["A"]["requests"]
    dB2 = c2["B"]["requests"] - c1["B"]["requests"]
    check("16 新会话均衡（防挤一台）", dA2 >= 5 and dB2 >= 5, "A+%d B+%d" % (dA2, dB2))

    print("== 流式 SSE 透传（含 usage 注入） ==")
    ev, usage = chat_stream(key)
    check("流式事件透传", ev >= 5, "events=%d" % ev)
    check("usage 注入生效", usage)

    print("== 鉴权与限流 ==")
    try:
        req = urllib.request.Request("http://127.0.0.1:%d/gw/v1/chat/completions" % GP,
                                     data=b'{"model":"m"}', headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        check("无 Key 401", False)
    except urllib.error.HTTPError as e:
        check("无 Key 401", e.code == 401, str(e.code))
    k2 = api("/api/gateway/key", {"name": "rpm2", "rpm": 2})["key"]
    codes = []
    for _ in range(4):
        try:
            chat(k2, 1)
            codes.append(200)
        except urllib.error.HTTPError as e:
            codes.append(e.code)
    check("RPM=2 第 3 次 429", codes.count(429) >= 1 and codes[:2] == [200, 200], str(codes))

    print("== 记账与 Key 用量 ==")
    snap = api("/api/gateway")
    check("总 token 记账>0", snap["totals"]["tokens"] > 0, str(snap["totals"]))
    kd = [k for k in snap["keys"] if k["key"] == key][0]
    check("Key 用量记账", kd["used_requests"] > 0 and kd["used_tokens"] > 0,
          "req=%d tok=%d" % (kd["used_requests"], kd["used_tokens"]))

    print("== 引擎指标联动（总表 engine 列） ==")
    time.sleep(4)
    c = chans()
    eb = c["B"].get("engine") or {}
    check("B 引擎 running 指标在", eb.get("running") is not None, str(eb.get("running")))
    check("QPS(1m) 窗口", snap["qps_1m"] >= 70, str(snap["qps_1m"]))

    print("== 故障转移：停 mock A，请求应全部经 B 成功 ==")
    import subprocess
    subprocess.run(["pkill", "-f", "mock[1].py"])
    n = chat(key, 5)
    check("A 死后 5 请求全成功", n == 5)
    print("  ... 等 12s 探活 ...")
    time.sleep(12)
    c = chans()
    check("A 探活判死", c["A"]["probe_ok"] is False)
    check("B 探活在线", c["B"]["probe_ok"] is True)

    print("== 熔断：B 连败 3 次进熔断 ==")
    subprocess.run(["pkill", "-f", "mock[2].py"])
    for _ in range(3):
        try:
            chat(key, 1)
        except Exception:
            pass
    c = chans()
    check("B 连败计数", c["B"]["fail"] >= 3, "B.fail=%d" % c["B"]["fail"])
    check("B 进熔断", c["B"]["status"] == "broken", c["B"]["status"])

    print("\n结果: %s" % ("ALL PASS" if not FAILED else "FAILED: %s" % FAILED))
    return 0 if not FAILED else 2


if __name__ == "__main__":
    raise SystemExit(main())
