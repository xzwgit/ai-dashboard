mod gw;
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
    pub gw: Option<Arc<gw::Gateway>>,
    pub gw_port: u16,
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
        "gw": match &app.gw { Some(g) => json!({"enabled": g.enabled.load(std::sync::atomic::Ordering::Relaxed), "port": app.gw_port, "policy": g.data.lock().unwrap().policy}), None => Value::Null },
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

// ---------- 网关 admin + 代理 ----------
async fn api_gateway(State(app): State<App>) -> Json<Value> {
    match &app.gw {
        Some(g) => Json(g.snapshot(app.gw_port)),
        None => Json(json!({"enabled": false, "off": true})),
    }
}

async fn gw_toggle(State(app): State<App>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let now = !g.enabled.load(std::sync::atomic::Ordering::Relaxed);
        g.enabled.store(now, std::sync::atomic::Ordering::Relaxed);
        Json(json!({"ok": true, "enabled": now}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_key(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let k = g.gen_key(body.0["name"].as_str().unwrap_or(""),
                          body.0["rpm"].as_u64().unwrap_or(0),
                          body.0["quota_tokens"].as_u64().unwrap_or(0));
        Json(json!({"ok": true, "key": k}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_key_op(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let ok = g.key_op(body.0["key"].as_str().unwrap_or(""), body.0["op"].as_str().unwrap_or(""),
                          body.0["rpm"].as_u64(), body.0["quota_tokens"].as_u64());
        Json(json!({"ok": ok}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_channel(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let ok = g.add_channel(body.0["name"].as_str().unwrap_or(""), body.0["url"].as_str().unwrap_or(""),
                               body.0["api_key"].as_str().unwrap_or(""),
                               body.0["priority"].as_i64().unwrap_or(1), body.0["weight"].as_i64().unwrap_or(10));
        Json(json!({"ok": ok}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_channel_op(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let ok = g.channel_op(body.0["name"].as_str().unwrap_or(""), body.0["op"].as_str().unwrap_or(""));
        Json(json!({"ok": ok}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_policy(State(app): State<App>, body: Json<Value>) -> Json<Value> {
    if let Some(g) = &app.gw {
        let ok = g.set_policy(body.0["policy"].as_str().unwrap_or(""));
        Json(json!({"ok": ok}))
    } else { Json(json!({"error": "gw off"})) }
}

async fn gw_any(
    State((g, client)): State<(Arc<gw::Gateway>, reqwest::Client)>,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::http::Method;
    if !g.enabled.load(std::sync::atomic::Ordering::Relaxed) {
        return gw::api_err(503, "网关转发已暂停（管理面板可开启）");
    }
    let auth = req.headers().get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).trim().to_string())
        .or_else(|| req.headers().get("x-api-key").and_then(|v| v.to_str().ok()).map(|s| s.to_string()))
        .unwrap_or_default();
    if auth.is_empty() {
        return gw::api_err(401, "缺少 API Key：请带 Authorization: Bearer sk-...");
    }
    if let Err((code, msg)) = g.check_key(&auth) {
        return gw::api_err(code, &msg);
    }
    let path_after = req.uri().path().to_string();
    let method = match *req.method() {
        Method::GET => "GET",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        _ => "POST",
    }.to_string();
    let hdrs = req.headers().clone();
    let body_v = axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024).await.unwrap_or_default();
    let mut body = body_v.to_vec();
    let mut is_stream = false;
    let mut hint: Option<String> = None;
    if !body.is_empty() && body.first() == Some(&b'{') {
        if let Ok(mut v) = serde_json::from_slice::<Value>(&body) {
            is_stream = v["stream"].as_bool().unwrap_or(false);
            if let Some(msgs) = v["messages"].as_array() {
                if let Some(m0) = msgs.first() {
                    if let Some(c0) = m0["content"].as_str() {
                        hint = Some(c0.chars().take(1000).collect());
                    }
                }
            }
            if hint.is_none() {
                if let Some(p0) = v["prompt"].as_str() {
                    hint = Some(p0.chars().take(1000).collect());
                }
            }
            if is_stream {
                let mut m = v["stream_options"].as_object().cloned().unwrap_or_default();
                m.insert("include_usage".into(), json!(true));
                v["stream_options"] = Value::Object(m);
                body = serde_json::to_vec(&v).unwrap_or(body);
            }
        }
    }
    gw::relay(&g, &client, &method, &path_after, &hdrs, body, is_stream, auth, hint).await
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
    gw: bool,
    gw_port: u16,
    gw_config: String,
}

fn parse_args() -> Args {
    let exe_dir = std::env::current_exe().ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf())).unwrap_or_default();
    let mut a = Args { port: 8124, target: Vec::new(), api_key: String::new(),
                       config: exe_dir.join("targets.json").to_string_lossy().into(),
                       gw: false, gw_port: 8125,
                       gw_config: exe_dir.join("gateway.json").to_string_lossy().into() };
    let mut it = std::env::args().skip(1);
    while let Some(x) = it.next() {
        match x.as_str() {
            "--port" => { if let Some(v) = it.next() { a.port = v.parse().unwrap_or(8124); } }
            "--config" => { if let Some(v) = it.next() { a.config = v; } }
            "--api-key" => { if let Some(v) = it.next() { a.api_key = v; } }
            "--gw" => { a.gw = true; }
            "--gw-port" => { if let Some(v) = it.next() { a.gw_port = v.parse().unwrap_or(8125); } }
            "--gw-config" => { if let Some(v) = it.next() { a.gw_config = v; } }
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
        gw: None,
        gw_port: args.gw_port,
    };
    app.load_targets();
    let mut app = app;
    if args.gw {
        let g = gw::Gateway::new(&args.gw_config);
        let targets = app.targets.clone();
        let f: gw::MetricsFn = Arc::new(move |base_url: &str| {
            let want = base_url.trim_end_matches('/').to_lowercase();
            let t = targets.lock().unwrap().iter()
                .find(|t| t.url.trim_end_matches('/').to_lowercase() == want).cloned();
            t.and_then(|t| t.now_brief())
        });
        *g.metrics_fn.lock().unwrap() = Some(f);
        app.gw = Some(g);
    }
    let app_final = app.clone();
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

    if let Some(g) = app_final.gw.clone() {
        let g2 = g.clone();
        let client = reqwest::Client::new();
        let gw_router = Router::new()
            .route("/gw/v1/*path", get(gw_any).post(gw_any).put(gw_any).delete(gw_any))
            .route("/gw/health", get(move || async move {
                let d = g2.data.lock().unwrap();
                Json(json!({"ok": true, "enabled": g2.enabled.load(std::sync::atomic::Ordering::Relaxed),
                            "channels": d.channels.len(), "keys": d.keys.len()}))
            }))
            .with_state((g, client));
        let gaddr = format!("0.0.0.0:{}", app_final.gw_port);
        println!("  网关:   http://127.0.0.1:{}/gw/v1 （OpenAI 兼容）", app_final.gw_port);
        tokio::spawn(async move {
            let l = tokio::net::TcpListener::bind(&gaddr).await.unwrap();
            axum::serve(l, gw_router).await.unwrap();
        });
    }

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
        .route("/api/gateway", get(api_gateway))
        .route("/api/gateway/toggle", post(gw_toggle))
        .route("/api/gateway/key", post(gw_key))
        .route("/api/gateway/key/op", post(gw_key_op))
        .route("/api/gateway/channel", post(gw_channel))
        .route("/api/gateway/channel/op", post(gw_channel_op))
        .route("/api/gateway/policy", post(gw_policy))
        .fallback(not_implemented)
        .with_state(app.clone());

    let addr = format!("0.0.0.0:{}", args.port);
    println!("AI 推理性能监控大屏已启动 (v{})  http://127.0.0.1:{}/", VERSION, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, router).await.unwrap();
}
