// 监控目标：抓取循环 / 派生序列 / snapshot 输出（口径对齐 Python 版）
use crate::metrics::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

pub const W_RATE: f64 = 12.0;
pub const W_PCT: f64 = 30.0;

static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn now() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64()
}

pub type Snap = HashMap<String, Option<f64>>;
pub type Histsnap = HashMap<String, Hist>;

pub struct Target {
    pub id: String,
    pub name: String,
    pub url: String,
    pub api_key: String,
    pub model_path: String,
    pub context_len: String,
    pub interval: f64,
    pub hist_max: usize,
    pub transient: bool,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
    pub st: Mutex<TState>,
    pub meta: RwLock<MetaInfo>,
}

#[derive(Default)]
pub struct MetaInfo {
    pub version: String,
    pub model_ids: Vec<String>,
}

#[derive(Default)]
pub struct TState {
    pub status: String, // init/up/down
    pub last_error: String,
    pub last_ts: f64,
    pub monitor_since: f64,
    pub n_scrapes: u64,
    pub t: Vec<f64>,
    pub raw: Vec<Snap>,
    pub hist: Vec<Histsnap>,
    pub series: HashMap<String, Vec<Option<f64>>>,
    pub finished: HashMap<String, f64>,
    pub has_spec: bool,
    pub has_prefix_cache: bool,
    pub kv_max_tokens: i64,
}

impl Target {
    pub fn new(name: &str, url: &str, api_key: &str, interval: f64, hist_max: usize,
               model_path: &str, context_len: &str, transient: bool) -> Arc<Target> {
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let id = format!("t{}_{}", seq, &rand_suffix());
        let mut series = HashMap::new();
        for k in SERIES_KEYS {
            series.insert(k.to_string(), Vec::new());
        }
        Arc::new(Target {
            id,
            name: name.to_string(),
            url: url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            model_path: model_path.to_string(),
            context_len: context_len.to_string(),
            interval,
            hist_max,
            transient,
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            st: Mutex::new(TState {
                status: "init".into(),
                monitor_since: now(),
                series,
                ..Default::default()
            }),
            meta: RwLock::new(MetaInfo::default()),
        })
    }

    fn metrics_url(&self) -> String {
        let path = self.url.splitn(3, "://").nth(1)
            .map(|rest| rest.contains('/')).unwrap_or(false);
        if !path {
            format!("{}/metrics", self.url)
        } else {
            self.url.clone()
        }
    }

    pub fn ingest(&self, ss: &SampleSet) {
        // finish_reason 计数
        let mut finished: HashMap<String, f64> = HashMap::new();
        for ((name, lk), v) in &ss.labeled {
            if name.ends_with("request_success_total") || name.ends_with("request_failure_total") {
                let key = lk.iter().find(|(k, _)| k == "finish_reason").map(|(_, v)| v.clone())
                    .or_else(|| lk.iter().find(|(k, _)| k == "error_type").map(|(_, v)| v.clone()))
                    .unwrap_or_else(|| "other".into());
                *finished.entry(key).or_insert(0.0) += v;
            }
        }
        // 带标签同名求和
        let mut agg: HashMap<String, f64> = ss.simple.clone();
        for ((name, _), v) in &ss.labeled {
            if name.ends_with("request_success_total") {
                continue;
            }
            *agg.entry(name.clone()).or_insert(0.0) += v;
        }
        let mut snap: Snap = HashMap::new();
        for (key, names) in ALIAS {
            let mut v: Option<f64> = None;
            for n in *names {
                if let Some(x) = agg.get(*n) {
                    v = Some(*x);
                    break;
                }
            }
            if let Some(x) = v {
                if !x.is_finite() {
                    v = None;
                }
            }
            snap.insert(key.to_string(), v);
        }
        let mut hsnap: Histsnap = HashMap::new();
        for (key, names) in ALIAS_HIST {
            for n in *names {
                if let Some(h) = ss.hist.get(*n) {
                    hsnap.insert(key.to_string(), h.clone());
                    break;
                }
            }
        }
        let mut st = self.st.lock().unwrap();
        st.finished = finished;
        if snap.get("spec_accepted").map(|v| v.is_some()).unwrap_or(false) {
            st.has_spec = true;
        }
        if snap.get("pc_queries").map(|v| v.is_some()).unwrap_or(false) {
            st.has_prefix_cache = true;
        }
        if let Some(Some(v)) = snap.get("kv_max_tokens") {
            if *v > 0.0 {
                st.kv_max_tokens = *v as i64;
            }
        }
        let ts = now();
        st.t.push(ts);
        st.raw.push(snap);
        st.hist.push(hsnap);
        st.n_scrapes += 1;
        st.status = "up".into();
        st.last_error.clear();
        st.last_ts = ts;
        self.derive_latest(&mut st);
        // 裁剪
        if st.t.len() > self.hist_max {
            let cut = st.t.len() - self.hist_max;
            st.t.drain(..cut);
            st.raw.drain(..cut);
            st.hist.drain(..cut);
            for v in st.series.values_mut() {
                v.drain(..cut);
            }
        }
    }

    fn idx_before(st: &TState, now: f64, window: f64) -> usize {
        let lo = now - window;
        for (i, t) in st.t.iter().enumerate() {
            if *t >= lo {
                return i;
            }
        }
        0
    }

    fn rate(t0: f64, t1: f64, v0: Option<f64>, v1: Option<f64>) -> Option<f64> {
        let d = t1 - t0;
        if v0.is_none() || v1.is_none() || d <= 0.0 {
            return None;
        }
        let r = (v1.unwrap() - v0.unwrap()) / d;
        if r.is_finite() {
            Some(r)
        } else {
            None
        }
    }

    fn derive_latest(&self, st: &mut TState) {
        let i = st.t.len() - 1;
        let j_rate = Self::idx_before(st, st.t[i], W_RATE);
        let j_pct = Self::idx_before(st, st.t[i], W_PCT);
        let cur = &st.raw[i];
        let gcur = cur.clone();
        let g = |k: &str| gcur.get(k).cloned().flatten();
        let mut push = |st: &mut TState, k: &str, v: Option<f64>| {
            st.series.get_mut(k).unwrap().push(v);
        };
        push(st, "running", g("running"));
        push(st, "waiting", g("waiting"));
        push(st, "kv_perc", g("kv_perc"));
        let t = st.t.clone();
        let raw: Vec<Snap> = st.raw.clone();
        let grow = Self::rate(t[j_rate], t[i], raw[j_rate].get("gen_tokens").cloned().flatten(), g("gen_tokens"));
        push(st, "gen_rate", grow);
        let prow = Self::rate(t[j_rate], t[i], raw[j_rate].get("prompt_tokens").cloned().flatten(), g("prompt_tokens"));
        push(st, "prompt_rate", prow);
        for (key, p50, p95) in [("ttft", "ttft_p50", "ttft_p95"), ("tpot", "tpot_p50", "tpot_p95")] {
            let v = st.hist[i].get(key).map(|h_now| {
                let b_ref = st.hist.get(j_pct).and_then(|h| h.get(key)).map(|h| h.buckets.as_slice());
                let delta = hist_delta(&h_now.buckets, b_ref);
                (hist_quantile(&delta, 0.50), hist_quantile(&delta, 0.95))
            });
            match v {
                Some((a, b)) => { push(st, p50, a); push(st, p95, b); }
                None => { push(st, p50, None); push(st, p95, None); }
            }
        }
        for (key, out) in [("e2e", "e2e_p95"), ("queue", "queue_p95")] {
            let v = st.hist[i].get(key).and_then(|h_now| {
                let b_ref = st.hist.get(j_pct).and_then(|h| h.get(key)).map(|h| h.buckets.as_slice());
                hist_quantile(&hist_delta(&h_now.buckets, b_ref), 0.95)
            });
            push(st, out, v);
        }
        let acc = Self::rate(t[j_pct], t[i], raw[j_pct].get("spec_accepted").cloned().flatten(), g("spec_accepted"));
        let drf = Self::rate(t[j_pct], t[i], raw[j_pct].get("spec_draft").cloned().flatten(), g("spec_draft"));
        let sv = match (acc, drf) { (Some(a), Some(d)) if d > 0.0 => Some((a / d).min(1.0)), _ => None };
        push(st, "spec_accept", sv);
        let hq = Self::rate(t[j_pct], t[i], raw[j_pct].get("pc_hits").cloned().flatten(), g("pc_hits"));
        let qq = Self::rate(t[j_pct], t[i], raw[j_pct].get("pc_queries").cloned().flatten(), g("pc_queries"));
        let pv = match (hq, qq) { (Some(h), Some(q)) if q > 0.0 => Some((h / q).min(1.0)), _ => None };
        push(st, "prefix_hit", pv);
        push(st, "gen_tokens_total", g("gen_tokens"));
        push(st, "prompt_tokens_total", g("prompt_tokens"));
    }

    /// 最新指标简表（主屏/总览用）
    pub fn now_brief(&self) -> Option<Value> {
        let st = self.st.lock().unwrap();
        if st.raw.is_empty() {
            return None;
        }
        let cur = &st.raw[st.raw.len() - 1];
        let last = |k: &str| -> Option<f64> {
            st.series.get(k).and_then(|c| c.iter().rev().find(|v| v.is_some()).cloned().flatten())
        };
        let get = |k: &str| cur.get(k).cloned().flatten();
        Some(json!({
            "status": st.status, "running": get("running"), "waiting": get("waiting"),
            "kv_perc": get("kv_perc"), "gen_rate": last("gen_rate"), "prompt_rate": last("prompt_rate"),
            "prefix_hit": last("prefix_hit"), "spec_accept": last("spec_accept"),
            "ttft_p95": last("ttft_p95"), "tpot_p95": last("tpot_p95"),
            "gen_tokens_total": get("gen_tokens"), "prompt_tokens_total": get("prompt_tokens"),
            "preemptions": get("preemptions"),
            "finished": st.finished.values().sum::<f64>(),
        }))
    }

    /// /api/data 全量 payload（与 Python snapshot 字段一致）
    pub async fn snapshot(&self, maxpoints: usize) -> Value {
        let (meta_version, meta_model_ids) = {
            let m = self.meta.read().await;
            (m.version.clone(), m.model_ids.clone())
        };
        let st = self.st.lock().unwrap();
        let n = st.t.len();
        let stride = (((n as f64) / maxpoints as f64).ceil() as usize).max(1);
        let pick = |c: &Vec<Option<f64>>| -> Vec<Option<f64>> {
            (0..n).step_by(stride).map(|i| c.get(i).cloned().flatten()).collect()
        };
        let mut series = serde_json::Map::new();
        for k in SERIES_KEYS {
            series.insert(k.to_string(), json!(pick(st.series.get(*k).unwrap())));
        }
        series.insert("t".into(), json!((0..n).step_by(stride).map(|i| st.t[i]).collect::<Vec<_>>()));
        let cur = if st.raw.is_empty() { json!({}) } else {
            let c = &st.raw[n - 1];
            let g = |k: &str| c.get(k).cloned().flatten();
            json!({
                "running": g("running"), "waiting": g("waiting"), "swapped": g("swapped"),
                "gen_rate": pick_last(&st, "gen_rate"), "prompt_rate": pick_last(&st, "prompt_rate"),
                "kv_perc": g("kv_perc"), "kv_used_tokens": g("kv_used_tokens"),
                "ttft_p50": pick_last(&st, "ttft_p50"), "ttft_p95": pick_last(&st, "ttft_p95"),
                "tpot_p50": pick_last(&st, "tpot_p50"), "tpot_p95": pick_last(&st, "tpot_p95"),
                "e2e_p95": pick_last(&st, "e2e_p95"), "queue_p95": pick_last(&st, "queue_p95"),
                "spec_accept": pick_last(&st, "spec_accept"), "prefix_hit": pick_last(&st, "prefix_hit"),
                "preemptions": g("preemptions"),
                "gen_tokens_total": g("gen_tokens"), "prompt_tokens_total": g("prompt_tokens"),
                "finished_total": st.finished.values().sum::<f64>(),
            })
        };
        json!({
            "id": self.id, "name": self.name, "url": self.url,
            "status": st.status, "last_error": if st.last_error.is_empty() { Value::Null } else { json!(st.last_error) },
            "last_ts": st.last_ts, "interval": self.interval,
            "monitor_since": st.monitor_since, "n_scrapes": st.n_scrapes,
            "has_spec": st.has_spec, "has_prefix_cache": st.has_prefix_cache,
            "kv_max_tokens": st.kv_max_tokens,
            "model_path": self.model_path, "context_len": self.context_len,
            "api_key": self.api_key,
            "meta_info": {"version": meta_version, "model_ids": meta_model_ids},
            "meta": {"points": n, "stride": stride},
            "series": Value::Object(series),
            "now": cur,
        })
    }

    pub fn start(self: &Arc<Target>, client: reqwest::Client) {
        let t = self.clone();
        tokio::spawn(async move {
            let url = t.metrics_url();
            loop {
                if t.stop.load(Ordering::Relaxed) {
                    break;
                }
                let mut req = client.get(&url);
                if !t.api_key.is_empty() {
                    req = req.bearer_auth(&t.api_key);
                }
                match req.timeout(std::time::Duration::from_secs(8)).send().await {
                    Ok(r) if r.status().is_success() => match r.text().await {
                        Ok(text) => {
                            let ss = parse_prom(&text);
                            t.ingest(&ss);
                        }
                        Err(e) => t.mark_down(&format!("URLError: {}", e)),
                    },
                    Ok(r) => t.mark_down(&format!("HTTPError: {}", r.status().as_u16())),
                    Err(e) => t.mark_down(&crate::node::friendly_error(&e)),
                }
                if t.st.lock().unwrap().n_scrapes % 30 == 1 {
                    let t2 = t.clone();
                    tokio::spawn(async move { t2.fetch_meta().await });
                }
                tokio::time::sleep(std::time::Duration::from_secs_f64(t.interval)).await;
            }
        });
    }

    fn mark_down(&self, e: &str) {
        let mut st = self.st.lock().unwrap();
        st.status = "down".into();
        st.last_error = e.to_string();
    }

    async fn fetch_meta(self: Arc<Target>) {
        let client = reqwest::Client::new();
        // /version
        if let Ok(r) = client.get(format!("{}/version", self.url)).timeout(std::time::Duration::from_secs(5)).send().await {
            if let Ok(text) = r.text().await {
                self.meta.write().await.version = text.trim().to_string();
            }
        }
        // /v1/models
        if let Ok(r) = client.get(format!("{}/v1/models", self.url)).timeout(std::time::Duration::from_secs(5)).send().await {
            if let Ok(text) = r.text().await {
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    if let Some(arr) = v["data"].as_array() {
                        let ids: Vec<String> = arr.iter()
                            .filter_map(|m| m["id"].as_str().map(|s| s.to_string()))
                            .collect();
                        self.meta.write().await.model_ids = ids;
                    }
                }
            }
        }
    }
}

fn pick_last(st: &TState, k: &str) -> Option<f64> {
    st.series.get(k).and_then(|c| c.iter().rev().find(|v| v.is_some()).cloned().flatten())
}

fn rand_suffix() -> String {
    let t = now();
    format!("{:x}", (t * 1e6) as u64 ^ std::process::id() as u64)[..6].to_string()
}
