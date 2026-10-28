// API 分发网关：渠道/Key/五策略/探活/亲和/SSE 透传/记账（对齐 Python 版语义）
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use serde_json::Value as V;

pub const BREAK_NFAIL: u32 = 3;
pub const BREAK_SECONDS: f64 = 60.0;
pub const PROBE_INTERVAL: f64 = 10.0;
pub const AFF_TTL: f64 = 1800.0;
pub const AFF_MAX: usize = 20000;
pub const REQ_TIMEOUT: Duration = Duration::from_secs(600);
pub const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
pub const POLICIES: &[&str] = &["weight", "round_robin", "p2c", "least_requests", "session"];

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Channel {
    pub name: String,
    pub base_url: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "d1")]
    pub priority: i64,
    #[serde(default = "d10")]
    pub weight: i64,
    #[serde(default = "d_true")]
    pub enabled: bool,
}
fn d1() -> i64 { 1 }
fn d10() -> i64 { 10 }
fn d_true() -> bool { true }

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct GwKey {
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub rpm: u64,
    #[serde(default)]
    pub quota_tokens: u64,
    #[serde(default)]
    pub used_tokens: u64,
    #[serde(default)]
    pub used_requests: u64,
    #[serde(default = "d_true")]
    pub enabled: bool,
    pub created: f64,
}

#[derive(Default, Clone)]
pub struct ChanStats {
    pub requests: u64,
    pub ok: u64,
    pub fail: u64,
    pub tokens: u64,
    pub lat_ms: f64,
    pub consec: u32,
    pub broken_until: f64,
    pub inflight: i64,
    pub probe_ok: Option<bool>,
    pub probe_ms: f64,
    pub qps: VecDeque<f64>,
}

#[derive(Default, Clone)]
pub struct Totals {
    pub requests: u64,
    pub ok: u64,
    pub fail: u64,
    pub tokens: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Default, Clone)]
pub struct GwData {
    #[serde(default)]
    pub policy: String,
    #[serde(default)]
    pub channels: Vec<Channel>,
    #[serde(default)]
    pub keys: Vec<GwKey>,
}

pub type MetricsFn = Arc<dyn Fn(&str) -> Option<Value> + Send + Sync>;

pub struct Gateway {
    pub path: String,
    pub enabled: AtomicBool,
    pub data: Mutex<GwData>,
    pub chan: Mutex<HashMap<String, ChanStats>>,
    pub totals: Mutex<Totals>,
    pub rpm_win: Mutex<HashMap<String, VecDeque<f64>>>,
    pub aff: Mutex<HashMap<String, (String, f64)>>,
    pub rr: AtomicUsize,
    pub metrics_fn: Mutex<Option<MetricsFn>>,
    pub started: f64,
}

pub fn now() -> f64 { crate::target::now() }

impl Gateway {
    pub fn new(path: &str) -> Arc<Gateway> {
        let mut data = GwData::default();
        if let Ok(txt) = std::fs::read_to_string(path) {
            if let Ok(d) = serde_json::from_str::<GwData>(&txt) {
                data = d;
            }
        }
        if !POLICIES.contains(&data.policy.as_str()) {
            data.policy = "weight".into();
        }
        let gw = Arc::new(Gateway {
            path: path.to_string(),
            enabled: AtomicBool::new(true),
            data: Mutex::new(data),
            chan: Mutex::new(HashMap::new()),
            totals: Mutex::new(Totals::default()),
            rpm_win: Mutex::new(HashMap::new()),
            aff: Mutex::new(HashMap::new()),
            rr: AtomicUsize::new(0),
            metrics_fn: Mutex::new(None),
            started: now(),
        });
        gw.start_probe_loop();
        gw.start_flush_loop();
        gw
    }

    fn start_flush_loop(self: &Arc<Self>) {
        let g = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                let _ = g.save();
            }
        });
    }

    fn start_probe_loop(self: &Arc<Self>) {
        let g = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs_f64(PROBE_INTERVAL)).await;
                let list: Vec<Channel> = g.data.lock().unwrap().channels.clone();
                for c in list {
                    if !c.enabled { continue; }
                    let t0 = now();
                    let client = reqwest::Client::new();
                    let ok = match client.get(format!("{}/health", c.base_url))
                        .timeout(Duration::from_secs(3)).send().await {
                        Ok(_) => true,           // 有 HTTP 应答即算进程活着
                        Err(_) => false,
                    };
                    let mut cs = g.chan.lock().unwrap();
                    let st = cs.entry(c.name.clone()).or_default();
                    st.probe_ok = Some(ok);
                    st.probe_ms = ((now() - t0) * 1000.0 * 10.0).round() / 10.0;
                }
            }
        });
    }

    pub fn save(&self) -> std::io::Result<()> {
        let d: GwData = self.data.lock().unwrap().clone();
        std::fs::write(&self.path, serde_json::to_string_pretty(&d)?)
    }

    // ---------- Key ----------
    pub fn gen_key(&self, name: &str, rpm: u64, quota: u64) -> String {
        use rand::RngCore;
        let mut b = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut b);
        let key = format!("sk-{}", b.iter().map(|x| format!("{:02x}", x)).collect::<String>());
        self.data.lock().unwrap().keys.push(GwKey {
            key: key.clone(), name: if name.trim().is_empty() { "unnamed".into() } else { name.trim().into() },
            rpm, quota_tokens: quota, used_tokens: 0, used_requests: 0,
            enabled: true, created: now(),
        });
        let _ = self.save();
        key
    }

    pub fn key_op(&self, key: &str, op: &str, rpm: Option<u64>, quota: Option<u64>) -> bool {
        let mut d = self.data.lock().unwrap();
        match d.keys.iter().position(|k| k.key == key) {
            Some(i) => {
                match op {
                    "toggle" => d.keys[i].enabled = !d.keys[i].enabled,
                    "delete" => { d.keys.remove(i); }
                    "quota" => {
                        if let Some(r) = rpm { d.keys[i].rpm = r; }
                        if let Some(q) = quota { d.keys[i].quota_tokens = q; }
                    }
                    _ => {}
                }
                drop(d);
                let _ = self.save();
                true
            }
            None => false,
        }
    }

    /// 鉴权+限流：Err(code, msg)
    pub fn check_key(&self, key: &str) -> Result<(), (u16, String)> {
        let d = self.data.lock().unwrap();
        let k = match d.keys.iter().find(|k| k.key == key) {
            Some(k) => k.clone(),
            None => return Err((401, "无效的 API Key".into())),
        };
        drop(d);
        if !k.enabled {
            return Err((403, "该 Key 已被停用，请联系管理员".into()));
        }
        if k.quota_tokens > 0 && k.used_tokens >= k.quota_tokens {
            return Err((429, format!("token 配额已用尽（配额 {}）", k.quota_tokens)));
        }
        if k.rpm > 0 {
            let mut w = self.rpm_win.lock().unwrap();
            let q = w.entry(key.to_string()).or_default();
            let t = now();
            while let Some(&f) = q.front() { if f < t - 60.0 { q.pop_front(); } else { break; } }
            if q.len() as u64 >= k.rpm {
                return Err((429, format!("超过 RPM 限额（{} 次/分钟）", k.rpm)));
            }
            q.push_back(t);
        }
        Ok(())
    }

    pub fn account_key(&self, key: &str, tokens: u64) {
        let mut d = self.data.lock().unwrap();
        if let Some(k) = d.keys.iter_mut().find(|k| k.key == key) {
            k.used_requests += 1;
            k.used_tokens += tokens;
        }
    }

    // ---------- 渠道 ----------
    pub fn add_channel(&self, name: &str, url: &str, api_key: &str, pri: i64, wt: i64) -> bool {
        let name = name.trim().to_string();
        let url = url.trim().trim_end_matches('/').to_string();
        if name.is_empty() || url.is_empty() { return false; }
        let mut d = self.data.lock().unwrap();
        if d.channels.iter().any(|c| c.name == name) { return false; }
        d.channels.push(Channel { name, base_url: url, api_key: api_key.into(),
                                  priority: pri.max(1), weight: wt.max(1), enabled: true });
        drop(d);
        let _ = self.save();
        true
    }

    pub fn channel_op(&self, name: &str, op: &str) -> bool {
        let mut d = self.data.lock().unwrap();
        match d.channels.iter().position(|c| c.name == name) {
            Some(i) => {
                match op {
                    "toggle" => d.channels[i].enabled = !d.channels[i].enabled,
                    "delete" => { d.channels.remove(i); }
                    _ => {}
                }
                drop(d);
                let _ = self.save();
                true
            }
            None => false,
        }
    }

    pub fn set_policy(&self, p: &str) -> bool {
        if !POLICIES.contains(&p) { return false; }
        self.data.lock().unwrap().policy = p.to_string();
        let _ = self.save();
        true
    }

    fn inflight_of(&self, name: &str) -> i64 {
        self.chan.lock().unwrap().get(name).map(|s| s.inflight).unwrap_or(0)
    }

    /// 策略选渠道（对齐 Python pick_channel 语义，含探活过滤/亲和/最少负载）
    pub fn pick_channel(&self, exclude: &[String], hint: Option<&str>) -> Option<Channel> {
        let t = now();
        let (cands_all, policy) = {
            let d = self.data.lock().unwrap();
            (d.channels.clone(), d.policy.clone())
        };
        let mut cands: Vec<Channel> = cands_all.into_iter()
            .filter(|c| c.enabled && !exclude.contains(&c.name))
            .filter(|c| self.chan.lock().unwrap().get(&c.name).map(|s| s.broken_until <= t).unwrap_or(true))
            .collect();
        if cands.is_empty() { return None; }
        let alive: Vec<Channel> = cands.iter().filter(|c|
            self.chan.lock().unwrap().get(&c.name).map(|s| s.probe_ok.unwrap_or(true)).unwrap_or(true)).cloned().collect();
        if !alive.is_empty() { cands = alive; }
        cands.sort_by_key(|c| c.priority);
        let p0 = cands[0].priority;
        let same: Vec<Channel> = cands.into_iter().filter(|c| c.priority == p0).collect();
        if same.len() == 1 { return Some(same[0].clone()); }

        if policy == "session" {
            if let Some(h) = hint {
                let hs = md5_hex(h);
                let mut aff = self.aff.lock().unwrap();
                if let Some((ch, ts)) = aff.get(&hs).cloned() {
                    if same.iter().any(|c| c.name == ch) {
                        aff.insert(hs, (ch.clone(), t));
                        return same.into_iter().find(|c| c.name == ch);
                    }
                }
                // 新会话 → 最少粘性数分配（并列按权重抽签）
                if aff.len() > AFF_MAX {
                    aff.retain(|_, v| t - v.1 <= AFF_TTL);
                }
                let mut cnt: HashMap<String, u32> = same.iter().map(|c| (c.name.clone(), 0)).collect();
                for (_, (ch, ts)) in aff.iter() {
                    if t - ts <= AFF_TTL {
                        if let Some(x) = cnt.get_mut(ch) { *x += 1; }
                    }
                }
                let m0 = cnt.values().cloned().min().unwrap_or(0);
                let pool: Vec<&Channel> = same.iter().filter(|c| cnt[&c.name] == m0).collect();
                let tw: f64 = pool.iter().map(|c| c.weight as f64).sum();
                let mut r = rand::random::<f64>() * tw;
                let mut pick = pool[pool.len() - 1].clone();
                for c in &pool {
                    r -= c.weight as f64;
                    if r <= 0.0 { pick = (*c).clone(); break; }
                }
                aff.insert(hs, (pick.name.clone(), t));
                return Some(pick);
            }
        } else if policy == "round_robin" {
            let n = self.rr.fetch_add(1, Ordering::SeqCst) % same.len();
            return Some(same[n].clone());
        } else if policy == "p2c" || policy == "least_requests" {
            let load = |c: &Channel| -> (f64, i64) {
                if policy == "least_requests" {
                    let f = self.metrics_fn.lock().unwrap().clone();
                    if let Some(f) = f {
                        if let Some(m) = f(&c.base_url) {
                            if let Some(run) = m["running"].as_f64() {
                                return (run, self.inflight_of(&c.name));
                            }
                        }
                    }
                    return (100000.0 + self.inflight_of(&c.name) as f64, 0);
                }
                (self.inflight_of(&c.name) as f64, 0)
            };
            if policy == "p2c" && same.len() >= 2 {
                let i1 = rand::random::<usize>() % same.len();
                let mut i2 = rand::random::<usize>() % same.len();
                if i2 == i1 { i2 = (i2 + 1) % same.len(); }
                let (a, b) = (&same[i1], &same[i2]);
                return Some(if load(a) <= load(b) { a.clone() } else { b.clone() });
            }
            return same.into_iter().min_by(|a, b| load(a).partial_cmp(&load(b)).unwrap_or(std::cmp::Ordering::Equal));
        }
        // weight（默认/兜底）：同层加权随机
        let tw: f64 = same.iter().map(|c| c.weight as f64).sum();
        let mut r = rand::random::<f64>() * tw;
        for c in &same {
            r -= c.weight as f64;
            if r <= 0.0 { return Some(c.clone()); }
        }
        Some(same.last().unwrap().clone())
    }

    pub fn chan_begin(&self, name: &str) {
        self.chan.lock().unwrap().entry(name.to_string()).or_default().inflight += 1;
    }
    pub fn chan_end(&self, name: &str) {
        let mut cs = self.chan.lock().unwrap();
        if let Some(st) = cs.get_mut(name) { st.inflight = (st.inflight - 1).max(0); }
    }

    pub fn chan_hit(&self, name: &str, ok: bool, lat_ms: f64, tokens: u64, fail_pre: bool) {
        let t = now();
        let mut tot = self.totals.lock().unwrap();
        tot.requests += 1;
        drop(tot);
        let mut cs = self.chan.lock().unwrap();
        let st = cs.entry(name.to_string()).or_default();
        st.requests += 1;
        st.qps.push_back(t);
        while let Some(&f) = st.qps.front() { if f < t - 60.0 { st.qps.pop_front(); } else { break; } }
        if ok {
            let mut tot = self.totals.lock().unwrap();
            tot.ok += 1;
            if tokens > 0 { tot.tokens += tokens; }
            st.ok += 1;
            st.lat_ms += lat_ms;
            if tokens > 0 { st.tokens += tokens; }
            st.consec = 0;
            st.broken_until = 0.0;
        } else {
            self.totals.lock().unwrap().fail += 1;
            st.fail += 1;
            if fail_pre {
                st.consec += 1;
                if st.consec >= BREAK_NFAIL {
                    st.broken_until = t + BREAK_SECONDS;
                }
            }
        }
    }

    pub fn snapshot(&self, port: u16) -> Value {
        let tt = self.totals.lock().unwrap().clone();
        let t = now();
        let d = self.data.lock().unwrap();
        let cs_all = self.chan.lock().unwrap();
        let mut chans = Vec::new();
        let mut tot_qps = 0u64;
        let mut tot_run = 0.0;
        let mut tot_dec = 0.0;
        let mut n_eng = 0;
        let mf = self.metrics_fn.lock().unwrap().clone();
        for c in &d.channels {
            let st = cs_all.get(&c.name).cloned().unwrap_or_default();
            let q = st.qps.len() as u64;
            tot_qps += q;
            let mut eng = V::Null;
            if let Some(f) = &mf {
                if let Some(m) = f(&c.base_url) {
                    if let Some(r) = m["running"].as_f64() {
                        tot_run += r; tot_dec += m["gen_rate"].as_f64().unwrap_or(0.0); n_eng += 1;
                    }
                    eng = m;
                }
            }
            let status = if !c.enabled { "paused" } else if st.broken_until > t { "broken" } else { "ok" };
            chans.push(json!({
                "name": c.name, "base_url": c.base_url, "priority": c.priority, "weight": c.weight,
                "status": status, "broken_until": if st.broken_until > t { (st.broken_until - t).round() as i64 } else { 0 },
                "requests": st.requests, "ok": st.ok, "fail": st.fail, "tokens": st.tokens,
                "avg_latency_ms": if st.ok > 0 { (st.lat_ms / st.ok as f64 * 10.0).round() / 10.0 } else { 0.0 },
                "inflight": st.inflight,
                "probe_ok": st.probe_ok, "probe_ms": st.probe_ms,
                "qps_1m": q, "engine": eng,
            }));
        }
        json!({
            "enabled": self.enabled.load(Ordering::Relaxed), "port": port, "since": self.started,
            "policy": d.policy,
            "totals": {"requests": tt.requests, "ok": tt.ok, "fail": tt.fail, "tokens": tt.tokens},
            "channels": chans, "qps_1m": tot_qps,
            "eng_running": if n_eng > 0 { json!(tot_run) } else { V::Null },
            "eng_gen_rate": if n_eng > 0 { json!((tot_dec * 10.0).round() / 10.0) } else { V::Null },
            "keys": d.keys,
        })
    }
}

// ---------- 工具 ----------
pub fn md5_hex(s: &str) -> String {
    // 简易 md5：无外部 crate，用 FNV 不行（需与稳定哈希一致即可——亲和只在本进程内一致即可）
    // 说明：Python 版用 md5；Rust 版自用 FNV-1a 64 位，只要进程内一致即可保持粘性语义。
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}

pub fn usage_from_json(body: &[u8]) -> u64 {
    if let Ok(v) = serde_json::from_slice::<Value>(body) {
        if let Some(u) = v["usage"].as_object() {
            return u.get("prompt_tokens").and_then(|x| x.as_u64()).unwrap_or(0)
                + u.get("completion_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
        }
    }
    0
}

pub fn usage_from_sse_tail(tail: &[u8]) -> u64 {
    for ln in String::from_utf8_lossy(tail).lines().rev() {
        let ln = ln.trim();
        if ln.starts_with("data:") && !ln.contains("[DONE]") && ln.contains("\"usage\"") {
            if let Ok(v) = serde_json::from_str::<Value>(&ln[5..].trim()) {
                if let Some(u) = v["usage"].as_object() {
                    return u.get("prompt_tokens").and_then(|x| x.as_u64()).unwrap_or(0)
                        + u.get("completion_tokens").and_then(|x| x.as_u64()).unwrap_or(0);
                }
            }
        }
    }
    0
}

// ---------- 反代 ----------
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

pub async fn relay(
    gw: &Arc<Gateway>,
    client: &reqwest::Client,
    method: &str,
    path_after_gw: &str,
    headers: &HeaderMap,
    body: Vec<u8>,
    is_stream: bool,
    key: String,
    hint: Option<String>,
) -> Response {
    let nch = gw.data.lock().unwrap().channels.len().max(1);
    let mut tried = 0;
    let mut tried_names: Vec<String> = Vec::new();
    let mut last_err = String::from("无可用渠道");
    while tried < nch {
        tried += 1;
        let ch = match gw.pick_channel(&tried_names, hint.as_deref()) {
            Some(c) => c,
            None => break,
        };
        tried_names.push(ch.name.clone());
        gw.chan_begin(&ch.name);
        let t0 = now();
        let url = format!("{}{}", ch.base_url, path_after_gw);
        let mut req = client.request(method.parse().unwrap_or(reqwest::Method::POST), &url)
            .timeout(if is_stream { STREAM_IDLE_TIMEOUT } else { REQ_TIMEOUT });
        for (k, v) in headers.iter() {
            if k.as_str() == "host" || k.as_str() == "authorization" || k.as_str() == "content-length" { continue; }
            if let Ok(hv) = v.to_str() { req = req.header(k.as_str(), hv); }
        }
        if !ch.api_key.is_empty() { req = req.bearer_auth(&ch.api_key); }
        if !body.is_empty() { req = req.body(body.clone()); }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                gw.chan_hit(&ch.name, false, 0.0, 0, true);
                last_err = format!("{}: {}", e.status().map(|s| s.to_string()).unwrap_or_else(|| "error".into()), e);
                gw.chan_end(&ch.name);
                continue;
            }
        };
        let status = resp.status();
        let rheaders = resp.headers().clone();
        if status.as_u16() >= 502 && tried < nch {
            gw.chan_hit(&ch.name, false, 0.0, 0, true);
            last_err = format!("上游 HTTP {}", status.as_u16());
            gw.chan_end(&ch.name);
            continue;
        }
        // 透传
        if is_stream {
            let tail = Arc::new(Mutex::new(Vec::<u8>::new()));
            let tail2 = tail.clone();
            let g = gw.clone();
            let key2 = key.clone();
            let chn = ch.name.clone();
            let t02 = t0;
            let st = status.as_u16();
            let ctype = rheaders.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("text/event-stream").to_string();
            let stream = resp.bytes_stream()
                .map(move |c| {
                    if let Ok(ref b) = c {
                        let mut t = tail2.lock().unwrap();
                        t.extend_from_slice(b);
                        let n = t.len();
                        if n > 32768 { t.drain(..n - 32768); }
                    }
                    c
                })
                .chain(futures::stream::once(async move {
                    let tail = tail.lock().unwrap().clone();
                    let tokens = usage_from_sse_tail(&tail);
                    g.chan_hit(&chn, true, (now() - t02) * 1000.0, tokens, false);
                    g.account_key(&key2, tokens);
                    g.chan_end(&chn);
                    Ok::<bytes::Bytes, reqwest::Error>(bytes::Bytes::new())
                }));
            let mut resp_b = Response::new(Body::from_stream(stream));
            *resp_b.status_mut() = StatusCode::from_u16(st).unwrap();
            resp_b.headers_mut().insert("content-type", ctype.parse().unwrap());
            resp_b.headers_mut().insert("cache-control", "no-cache".parse().unwrap());
            return resp_b;
        } else {
            let body_out = match resp.bytes().await {
                Ok(b) => b.to_vec(),
                Err(e) => {
                    gw.chan_hit(&ch.name, false, 0.0, 0, false);
                    last_err = e.to_string();
                    gw.chan_end(&ch.name);
                    continue;
                }
            };
            let tokens = usage_from_json(&body_out);
            gw.chan_hit(&ch.name, true, (now() - t0) * 1000.0, tokens, false);
            gw.account_key(&key, tokens);
            gw.chan_end(&ch.name);
            let mut resp_b = Response::new(Body::from(body_out));
            *resp_b.status_mut() = StatusCode::from_u16(status.as_u16()).unwrap();
            let ct = rheaders.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or("application/json").to_string();
            resp_b.headers_mut().insert("content-type", ct.parse().unwrap());
            return resp_b;
        }
    }
    api_err(502, &format!("所有渠道不可用：{}", last_err))
}

pub fn api_err(code: u16, msg: &str) -> Response {
    let etype = if code == 429 { "insufficient_quota" } else if code >= 500 { "server_error" } else { "invalid_request_error" };
    let body = json!({"error": {"message": msg, "type": etype, "code": code}});
    let mut r = Response::new(Body::from(serde_json::to_string(&body).unwrap()));
    *r.status_mut() = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY);
    r.headers_mut().insert("content-type", "application/json".parse().unwrap());
    r
}
