#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""v3 网关负载均衡本地测试：双 mock 上游 → 策略分流 / 故障转移 / 探活 / 总表。
用法: python test_lb_v3.py   （自动起 2 个 mock + 1 个大屏+网关，结束自动清理）
"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
TMP = os.path.join(os.environ.get("TEMP", "/tmp"), "vdlb")
MP1, MP2 = 18301, 18302      # mock 上游
DP, GP = 18304, 18305        # 大屏 / 网关
PROCS = []
FAILED = []


def sh(*args, log=None):
    f = open(log, "w") if log else subprocess.DEVNULL
    p = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT)
    PROCS.append(p)
    return p


def api(path, obj=None, port=DP):
    url = "http://127.0.0.1:%d%s" % (port, path)
    if obj is None:
        r = urllib.request.urlopen(url, timeout=15)
        return json.loads(r.read().decode("utf-8"))
    req = urllib.request.Request(url, data=json.dumps(obj).encode("utf-8"),
                                 headers={"Content-Type": "application/json"}, method="POST")
    r = urllib.request.urlopen(req, timeout=15)
    return json.loads(r.read().decode("utf-8"))


def chat(key, n=1):
    ok = 0
    for _ in range(n):
        req = urllib.request.Request(
            "http://127.0.0.1:%d/gw/v1/chat/completions" % GP,
            data=json.dumps({"model": "mock-model",
                             "messages": [{"role": "user", "content": "ping lb"}]}).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + key})
        r = urllib.request.urlopen(req, timeout=60)
        j = json.loads(r.read().decode("utf-8"))
        if j.get("choices"):
            ok += 1
    return ok


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    if not cond:
        FAILED.append(name)
    print("  [%s] %s %s" % (tag, name, detail))


def chans():
    return {c["name"]: c for c in api("/api/gateway")["channels"]}


def main():
    shutil.rmtree(TMP, ignore_errors=True)
    os.makedirs(TMP, exist_ok=True)
    py = sys.executable
    sh(py, os.path.join(BASE, "test_mock_openai.py"), str(MP1), log=os.path.join(TMP, "mock1.log"))
    sh(py, os.path.join(BASE, "test_mock_openai.py"), str(MP2), log=os.path.join(TMP, "mock2.log"))
    sh(py, os.path.join(BASE, "vllm_dashboard.py"),
       "--port", str(DP), "--gw", "--gw-port", str(GP), "--no-open",
       "--config", os.path.join(TMP, "targets.json"),
       "--gw-config", os.path.join(TMP, "gateway.json"),
       "--target", "mockA=http://127.0.0.1:%d" % MP1,
       "--target", "mockB=http://127.0.0.1:%d" % MP2,
       log=os.path.join(TMP, "dash.log"))

    for _ in range(60):
        try:
            api("/api/targets")
            break
        except Exception:
            time.sleep(0.5)
    else:
        print("大屏启动失败，见 %s" % os.path.join(TMP, "dash.log"))
        return 1

    print("== 准备：渠道 ×2 + Key ==")
    check("add channel A", api("/api/gateway/channel",
          {"name": "A", "url": "http://127.0.0.1:%d" % MP1, "priority": 1, "weight": 1})["ok"])
    check("add channel B", api("/api/gateway/channel",
          {"name": "B", "url": "http://127.0.0.1:%d" % MP2, "priority": 1, "weight": 1})["ok"])
    key = api("/api/gateway/key", {"name": "lbtest"})["key"]

    print("== 策略 round_robin：8 请求应 4/4 交替 ==")
    check("set round_robin", api("/api/gateway/policy", {"policy": "round_robin"})["ok"])
    n = chat(key, 8)
    c = chans()
    check("8 请求全成功", n == 8)
    check("4/4 均分", c["A"]["requests"] == 4 and c["B"]["requests"] == 4,
          "A=%d B=%d" % (c["A"]["requests"], c["B"]["requests"]))

    print("== 策略 weight：40 请求两渠道都有份 ==")
    api("/api/gateway/policy", {"policy": "weight"})
    chat(key, 40)
    c = chans()
    check("两渠道均分到流量", c["A"]["requests"] > 5 and c["B"]["requests"] > 5,
          "A=%d B=%d" % (c["A"]["requests"], c["B"]["requests"]))

    print("== 策略 p2c：20 请求两渠道都有份 ==")
    api("/api/gateway/policy", {"policy": "p2c"})
    chat(key, 20)
    c = chans()
    check("p2c 分流", c["A"]["requests"] > 2 and c["B"]["requests"] > 2,
          "A=%d B=%d" % (c["A"]["requests"], c["B"]["requests"]))

    print("== 策略 least_requests：6 请求全成功（指标感知，引擎指标可或缺席） ==")
    api("/api/gateway/policy", {"policy": "least_requests"})
    n = chat(key, 6)
    check("least_requests 全成功", n == 6)

    print("== 总表：engine 指标接入（监控目标 ↔ 渠道地址匹配） ==")
    time.sleep(4)   # 等 /metrics 抓一轮
    c = chans()
    snap = api("/api/gateway")
    eng_b = (c["B"].get("engine") or {})
    check("B 引擎 running 指标在", eng_b.get("running") is not None, str(eng_b.get("running")))
    check("B 引擎 kv/gen 字段在", eng_b.get("kv_perc") is not None and "gen_rate" in eng_b,
          "kv=%s" % eng_b.get("kv_perc"))
    check("QPS(1m) 窗口计数", snap.get("qps_1m", 0) >= 70, "qps_1m=%s" % snap.get("qps_1m"))
    check("策略字段持久在快照", snap.get("policy") == "least_requests", snap.get("policy"))

    print("== 故障转移：杀掉 A，请求应全部经 B 成功 ==")
    PROCS[0].kill()
    n = chat(key, 5)
    c = chans()
    check("A 死后 5 请求全成功", n == 5)
    check("失败计在 A 上", c["A"]["fail"] >= 1, "A.fail=%d" % c["A"]["fail"])
    print("  ... 等 12s 让探活线程跑一轮 ...")
    time.sleep(12)
    c = chans()
    check("A 探活判死", c["A"]["probe_ok"] is False)
    check("B 探活在线", c["B"]["probe_ok"] is True, "B.probe=%sms" % c["B"]["probe_ms"])

    print("== 收尾：gateway.json 持久化检查 ==")
    time.sleep(0.5)
    with open(os.path.join(TMP, "gateway.json"), encoding="utf-8") as f:
        gj = json.load(f)
    check("policy 已持久化", gj.get("policy") == "least_requests", gj.get("policy"))

    print("\n结果: %s（%d 项失败）" % ("ALL PASS" if not FAILED else "FAILED: %s" % FAILED,
                                      len(FAILED)))
    return 0 if not FAILED else 2


if __name__ == "__main__":
    rc = 1
    try:
        rc = main()
    finally:
        for p in PROCS:
            try:
                p.kill()
            except Exception:
                pass
    sys.exit(rc)
