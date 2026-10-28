mod metrics;
mod node;
mod target;

use axum::{extract::{Path, Query, State}, http::{header, StatusCode}, response::IntoResponse, routing::{get, post}, Json, Router};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use target::Target;

pub const VERSION: &str = "2.2.0";

#[derive(Clone)]
pub struct App {
    pub targets: Arc<Mutex<Vec<Arc<Target>>>>,
    pub config_path: String,
    pub http: reqwest::Client,
}

fn now() -> f64 { target::now() }

// ---------- 注册表 ----------
impl App {
    fn add_target(&self, name: &str, url: &str, api_key: &str, model_path: &str, ctx: &str, persist: bool) -> Arc<Target> {
        let t = Target::new(name, url, api_key, 2.0, 5400, model_path, ctx, !persist);
        t.start(self.http.clone());
        self.targets.lock().unwrap().push(t.clone());
        if persist { let _ = self.save_targets(); }
        t
    }

    fn save_targets(&self) -> std::io::Result<()> {
        let list: Vec<Value> = self.targets.lock().unwrap().iter()
            .filter(|t| !t.transient)
            .map(|t| json!({"name": t.name, "url": t.url, "api_key": t.api_key,
                            "model_path": t.model_path, "context_len": t.context_len}))
            .collect();
        std::fs::write(&self.config_path, serde_json::to_string_pretty(&json!({"targets": list}))?)?;
        Ok(())
    }

    fn load_targets(&self) {
        if let Ok(txt) = std::fs::read_to_string(&self.config_path) {
            if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                if let Some(arr) = v["targets"].as_array() {
                    for it in arr {
                        let g = |k: &str| it[k].as_str().unwrap_or("").to_string();
                        self.add_target(&g("name"), &g("url"), &g("api_key"), &g("model_path"), &g("context_len"), false);
                    }
                }
            }
        }
    }
}

// ---------- handlers ----------
async fn index() -> impl IntoResponse {
    let html = include_str!("frontend.html").replace("__APPVER__", VERSION);
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8"),
      (header::CACHE_CONTROL, "no-store")], html)
}

async fn api_targets(State(app): State<App>) -> Json<Value> {
    let arr: Vec<Value> = app.targets.lock().unwrap().iter().map(|t| {
        let st = t.st.lock().unwrap();
        json!({"id": t.id, "name": t.name, "url": t.url, "status": st.status,
               "last_error": if st.last_error.is_empty() { Value::Null } else { json!(st.last_error) },
               "last_ts": st.last_ts})
    }).collect();
    Json(json!({"targets": arr}))
}

async fn api_data(State(app): State<App>, Query(q): Query<HashMap<String, String>>) -> Result<Json<Value>, StatusCode> {
    let id = q.get("id").cloned().unwrap_or_default();
    let maxp: usize = q.get("maxpoints").and_then(|v| v.parse().ok()).unwrap_or(1200);
    let t = app.targets.lock().unwrap().iter().find(|t| t.id == id).cloned();
    match t {
        Some(t) => Ok(Json(t.snapshot(maxp).await)),
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn api_node() -> Json<Value> {
    Json(node::NodeCollector::snapshot().await)
}

async fn api_overview(State(app): State<App>) -> Json<Value> {
    let mut rows = Vec::new();
    let mut tot_gen = 0.0; let mut tot_pre = 0.0;
    let mut run = 0.0; let mut wait = 0.0;
    let mut gen_total = 0.0; let mut pre_total = 0.0;
    let mut preempt = 0.0; let mut finished = 0.0;
    let mut n_up = 0usize;
    let mut kv_max: Option<f64> = None;
    let mut kv_name = String::new();
    let mut tpots = Vec::new();
    let mut pcs = Vec::new();
    let mut specs = Vec::new();
    let list = app.targets.lock().unwrap().clone();
    for t in list.iter() {
        let (status, has_spec, has_pc, model_path) = {
            let st = t.st.lock().unwrap();
            (st.status.clone(), st.has_spec, st.has_prefix_cache, t.model_path.clone())
        };
        let meta = t.meta.read().await;
        let model = if !model_path.is_empty() {
            model_path.trim_end_matches('/').rsplit('/').next().unwrap_or("").to_string()
        } else {
            meta.model_ids.first().cloned().unwrap_or_default()
        };
        let mut version = meta.version.clone();
        if let Ok(v) = serde_json::from_str::<Value>(&version) {
            version = v["version"].as_str().unwrap_or("").to_string();
        }
        let nb = t.now_brief();
        let mut row = json!({"id": t.id, "name": t.name, "status": status, "url": t.url,
                             "model": model, "version": version,
                             "context_len": t.context_len,
                             "has_spec": has_spec, "has_pc": has_pc});
        if status == "up" {
            n_up += 1;
            if let Some(b) = &nb {
                tot_gen += b["gen_rate"].as_f64().unwrap_or(0.0);
                tot_pre += b["prompt_rate"].as_f64().unwrap_or(0.0);
                run += b["running"].as_f64().unwrap_or(0.0);
                wait += b["waiting"].as_f64().unwrap_or(0.0);
                gen_total += b["gen_tokens_total"].as_f64().unwrap_or(0.0);
                pre_total += b["prompt_tokens_total"].as_f64().unwrap_or(0.0);
                preempt += b["preemptions"].as_f64().unwrap_or(0.0);
                finished += b["finished"].as_f64().unwrap_or(0.0);
                if let Some(k) = b["kv_perc"].as_f64() {
                    if kv_max.map(|m| k > m).unwrap_or(true) { kv_max = Some(k); kv_name = t.name.clone(); }
                }
                if let Some(x) = b["tpot_p95"].as_f64() { tpots.push(x); }
                if let Some(x) = b["prefix_hit"].as_f64() { pcs.push(x); }
                if let Some(x) = b["spec_accept"].as_f64() { specs.push(x); }
            }
        }
        if let Some(b) = nb {
            for k in ["running", "waiting", "kv_perc", "gen_rate", "prompt_rate", "tpot_p95", "ttft_p95",
                      "gen_tokens_total", "prompt_tokens_total", "preemptions", "finished",
                      "prefix_hit", "spec_accept"] {
                row[k] = b[k].clone();
            }
        }
        rows.push(row);
    }
    let avg = |v: &Vec<f64>| if v.is_empty() { Value::Null } else { json!(v.iter().sum::<f64>() / v.len() as f64) };
    Json(json!({
        "targets": rows, "n_up": n_up, "n_total": list.len(),
        "totals": {"gen_rate": tot_gen, "prompt_rate": tot_pre, "running": run, "waiting": wait,
                   "gen_tokens_total": gen_total, "prompt_tokens_total": pre_total,
                   "preemptions": preempt, "finished": finished,
                   "tpot_p95": avg(&tpots), "prefix_hit": avg(&pcs), "spec_accept": avg(&specs)},
        "kv_max": kv_max, "kv_max_name": kv_name,
        "gw": Value::Null,
    }))
}

async fn api_add_target(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    let b = body.0;
    let name = b["name"].as_str().unwrap_or("target").trim().to_string();
    let url = b["url"].as_str().unwrap_or("").trim().to_string();
    if url.is_empty() || !url.starts_with("http") {
        return Json(json!({"error": "url required (http(s)://)"}));
    }
    let g = |k: &str| b[k].as_str().unwrap_or("").trim().to_string();
    let t = app.add_target(&name, &url, &g("api_key"), &g("model_path"), &g("context_len"), true);
    Json(json!({"ok": true, "id": t.id}))
}

async fn api_remove_target(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    let id = body.0["id"].as_str().unwrap_or("").to_string();
    let mut gs = app.targets.lock().unwrap();
    if let Some(pos) = gs.iter().position(|t| t.id == id) {
        let t = gs.remove(pos);
        t.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    drop(gs);
    let _ = app.save_targets();
    Json(json!({"ok": true}))
}

async fn agent_md() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/markdown; charset=utf-8"),
      (header::CONTENT_DISPOSITION, "attachment; filename=\"agent.md\"")],
     include_str!("agent.md"))
}

async fn cfg_example() -> Json<Value> {
    Json(serde_json::from_str::<Value>(include_str!("config_example.json")).unwrap())
}

async fn cfg_export(State(app): State<App>) -> Json<Value> {
    let list: Vec<Value> = app.targets.lock().unwrap().iter()
        .filter(|t| !t.transient)
        .map(|t| json!({"name": t.name, "url": t.url, "api_key": t.api_key,
                        "model_path": t.model_path, "context_len": t.context_len}))
        .collect();
    Json(json!({"targets": list, "gateway": {"policy": "weight", "channels": []}}))
}

async fn cfg_import(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    let cfg = body.0["config"].clone();
    let tl = match cfg["targets"].as_array() {
        Some(a) => a.clone(),
        None => return Json(json!({"error": "config 需要 {targets:[...], gateway:{...}} 结构"})),
    };
    let mut gs = app.targets.lock().unwrap();
    for t in gs.iter() {
        t.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    gs.clear();
    drop(gs);
    for it in &tl {
        let g = |k: &str| it[k].as_str().unwrap_or("").trim().to_string();
        app.add_target(&g("name"), &g("url"), &g("api_key"), &g("model_path"), &g("context_len"), false);
    }
    let _ = app.save_targets();
    Json(json!({"ok": true, "message": format!("导入完成：{} 个监控目标", tl.len())}))
}

// 404 兜底：未实现端点（网关 Round 2）
async fn not_implemented(Path(p): Path<String>) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({"error": format!("not found: /{}", p)})))
}

// ---------- CLI ----------
struct Args {
    port: u16,
    target: Vec<String>,
    api_key: String,
    config: String,
}

fn parse_args() -> Args {
    let mut a = Args { port: 8124, target: Vec::new(), api_key: String::new(),
                       config: "targets.json".into() };
    let mut it = std::env::args().skip(1);
    while let Some(x) = it.next() {
        match x.as_str() {
            "--port" => { if let Some(v) = it.next() { a.port = v.parse().unwrap_or(8124); } }
            "--config" => { if let Some(v) = it.next() { a.config = v; } }
            "--api-key" => { if let Some(v) = it.next() { a.api_key = v; } }
            "--target" => { if let Some(v) = it.next() { a.target.push(v); } }
            _ => {}
        }
    }
    a
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let app = App {
        targets: Arc::new(Mutex::new(Vec::new())),
        config_path: args.config.clone(),
        http: reqwest::Client::new(),
    };
    app.load_targets();
    for spec in &args.target {
        let (name, url) = match spec.split_once('=') {
            Some((n, u)) => (n.to_string(), u.to_string()),
            None => (spec.rsplit('/').next().unwrap_or("target").to_string(), spec.clone()),
        };
        if !app.targets.lock().unwrap().iter().any(|t| t.url == url) {
            app.add_target(&name, &url, &args.api_key, "", "", true);
            println!("[target] {} = {}", name, url);
        }
    }

    let _ = node::NodeCollector::new(); // 启动功率采样线程

    let router = Router::new()
        .route("/", get(index))
        .route("/api/targets", get(api_targets).post(api_add_target))
        .route("/api/targets/remove", post(api_remove_target))
        .route("/api/data", get(api_data))
        .route("/api/node", get(api_node))
        .route("/api/overview", get(api_overview))
        .route("/agent.md", get(agent_md))
        .route("/api/config/example", get(cfg_example))
        .route("/api/config/export", get(cfg_export))
        .route("/api/config/import", post(cfg_import))
        .fallback(not_implemented)
        .with_state(app.clone());

    let addr = format!("0.0.0.0:{}", args.port);
    println!("AI 推理性能监控大屏已启动 (v{})  http://127.0.0.1:{}/", VERSION, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();
}
