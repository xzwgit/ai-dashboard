// 宿主机采集：/proc + nvidia-smi + ipmitool（整机功率后台采样）
use serde_json::{json, Value};
use std::sync::Mutex;
use tokio::process::Command;

#[derive(Default)]
pub struct NodePower {
    pub power_w: Option<f64>,
}

pub struct NodeCollector {
    pub power: Mutex<NodePower>,
}

impl NodeCollector {
    pub fn new() -> Self {
        let nc = NodeCollector { power: Mutex::new(NodePower::default()) };
        nc.start_power_loop();
        nc
    }

    fn start_power_loop(&self) {
        tokio::spawn(async {
            loop {
                if let Some(w) = sample_power().await {
                    // 全局 OnceCell 式写入：通过 task-local 不可行，改用全局
                    POWER.get_or_init(|| Mutex::new(None)).lock().unwrap().replace(w);
                }
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        });
    }

    pub fn power_w() -> Option<f64> {
        POWER.get_or_init(|| Mutex::new(None)).lock().unwrap().map(|v| v)
    }

    pub async fn snapshot() -> Value {
        let (cpu, cpu_name, cores, mem) = read_proc();
        let (gname, gcount, gutil, gmem_perc, gmem_used, gmem_total, per_gpu) = read_gpu().await;
        let os_pretty = read_os_release();
        let hostname = std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()).unwrap_or_default();
        let ip = outbound_ip().unwrap_or_default();
        let uptime_s = std::fs::read_to_string("/proc/uptime")
            .ok().and_then(|s| s.split_whitespace().next().and_then(|v| v.parse::<f64>().ok())).unwrap_or(0.0);
        json!({
            "available": cfg!(target_os = "linux"),
            "hostname": hostname, "ip": ip,
            "platform": std::env::consts::OS,
            "os_pretty": os_pretty,
            "kernel": format!("{}.{}", uname_release(), std::env::consts::ARCH),
            "cpu_name": cpu_name, "cpu_cores": cores,
            "has_proc": cfg!(target_os = "linux"),
            "has_gpu": gcount > 0,
            "gpu_name": gname, "uptime_s": uptime_s,
            "power_w": Self::power_w(),
            "time": crate::target::now(),
            "now": {
                "cpu": cpu, "mem_perc": mem.2, "mem_used": mem.0, "mem_total": mem.1,
                "gpu_util": gutil, "gpu_mem_perc": gmem_perc,
                "gpu_mem_used": gmem_used, "gpu_mem_total": gmem_total,
            },
            "per_gpu": per_gpu,
        })
    }
}

static POWER: std::sync::OnceLock<Mutex<Option<f64>>> = std::sync::OnceLock::new();

/// 抓取错误翻译成人话（对齐 Python 口径的关键分支）
pub fn friendly_error(e: &reqwest::Error) -> String {
    let raw = format!("{}: {}", error_kind(e), e);
    if raw.contains("refused") || raw.contains("Errno 111") || raw.contains("10061") || raw.contains("积极拒绝") {
        return format!("连接被拒：vLLM 服务未启动或已退出，或端口不对（原始: {}）", &raw[..raw.len().min(110)]);
    }
    raw
}

fn error_kind(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() { "Timeout" }
    else if e.is_connect() { "URLError" }
    else { "Error" }
}

// ---------- /proc ----------
static CPU_PREV: std::sync::OnceLock<Mutex<(u64, u64)>> = std::sync::OnceLock::new();

fn read_proc() -> (Option<f64>, String, usize, (f64, f64, f64)) {
    let cpu_name = std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
        s.lines().find(|l| l.starts_with("model name"))
            .map(|l| l.split(':').nth(1).unwrap_or("").trim()
                .replace("(R)", "").replace("(TM)", "").replace("  ", " ").to_string())
    }).unwrap_or_default();
    let cores = std::fs::read_to_string("/proc/cpuinfo").ok()
        .map(|s| s.lines().filter(|l| l.starts_with("processor")).count()).unwrap_or(0);
    let cpu = read_cpu_pct();
    let mem = read_meminfo();
    (cpu, cpu_name, cores, mem)
}

fn read_cpu_pct() -> Option<f64> {
    let s = std::fs::read_to_string("/proc/stat").ok()?;
    let line = s.lines().next()?;
    let f: Vec<u64> = line.split_whitespace().skip(1).filter_map(|x| x.parse().ok()).collect();
    if f.len() < 5 { return None; }
    let idle_all = f[3] + f.get(4).copied().unwrap_or(0);
    let total: u64 = f.iter().sum();
    let m = CPU_PREV.get_or_init(|| Mutex::new((0, 0)));
    let mut g = m.lock().unwrap();
    let v = if total > g.1 {
        Some((1.0 - (idle_all - g.0) as f64 / (total - g.1) as f64).max(0.0) * 100.0)
    } else { None };
    *g = (idle_all, total);
    v
}

fn read_meminfo() -> (f64, f64, f64) {
    // GB, GB, %
    let s = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let mut total = 0.0; let mut avail = 0.0;
    for l in s.lines() {
        if l.starts_with("MemTotal:") {
            total = l.split_whitespace().nth(1).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1048576.0;
        } else if l.starts_with("MemAvailable:") {
            avail = l.split_whitespace().nth(1).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1048576.0;
        }
    }
    let used = (total - avail).max(0.0);
    let perc = if total > 0.0 { used / total * 100.0 } else { 0.0 };
    (used, total, perc)
}

fn read_os_release() -> String {
    let s = std::fs::read_to_string("/etc/os-release").unwrap_or_default();
    for l in s.lines() {
        if l.starts_with("PRETTY_NAME=") {
            return l.splitn(2, '=').nth(1).unwrap_or("").trim_matches('"').to_string();
        }
    }
    String::new()
}

fn uname_release() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease").map(|s| s.trim().to_string()).unwrap_or_default()
}

fn outbound_ip() -> Option<String> {
    use std::net::UdpSocket;
    let s = UdpSocket::bind("0.0.0.0:0").ok()?;
    s.connect("8.8.8.8:80").ok()?;
    Some(s.local_addr().ok()?.ip().to_string())
}

// ---------- GPU ----------
async fn read_gpu() -> (String, usize, Option<f64>, Option<f64>, Option<f64>, Option<f64>, Value) {
    let out = Command::new("nvidia-smi")
        .args(["--query-gpu=name,utilization.gpu,memory.used,memory.total,memory.used.percent",
               "--format=csv,noheader,nounits"])
        .output().await;
    let mut name = String::new();
    let mut utils = Vec::new();
    let mut mems = Vec::new();
    if let Ok(o) = out {
        for l in String::from_utf8_lossy(&o.stdout).lines() {
            let p: Vec<&str> = l.split(", ").map(|x| x.trim()).collect();
            if p.len() >= 4 {
                if name.is_empty() { name = p[0].to_string(); }
                utils.push(p[1].parse::<f64>().unwrap_or(0.0));
                mems.push((p[2].parse::<f64>().unwrap_or(0.0) / 1024.0,
                           p[3].parse::<f64>().unwrap_or(0.0) / 1024.0));
            }
        }
    }
    let n = utils.len();
    let avg = |v: &Vec<f64>| if v.is_empty() { None } else { Some(v.iter().sum::<f64>() / v.len() as f64) };
    let mem_used: f64 = mems.iter().map(|(u, _)| u).sum();
    let mem_total: f64 = mems.iter().map(|(_, t)| t).sum();
    let mem_perc = if mem_total > 0.0 { Some(mem_used / mem_total * 100.0) } else { None };
    let per_gpu: Vec<Value> = utils.iter().zip(mems.iter())
        .map(|(u, (mu, mt))| json!({"util": u, "mem_used": mu, "mem_total": mt}))
        .collect();
    (name, n, avg(&utils), mem_perc,
     if n > 0 { Some(mem_used) } else { None },
     if n > 0 { Some(mem_total) } else { None },
     json!(per_gpu))
}

// ---------- 整机功率（对齐 Python 读取链） ----------
async fn sample_power() -> Option<f64> {
    // 1) dcmi 直读
    if let Ok(o) = Command::new("ipmitool").args(["dcmi", "power reading"]).output().await {
        let txt = String::from_utf8_lossy(&o.stdout);
        for l in txt.lines() {
            if l.contains("Instantaneous power reading") {
                if let Some(v) = l.split(':').nth(1).and_then(|s| s.trim().split_whitespace().next())
                    .and_then(|s| s.parse::<f64>().ok()) {
                    if v > 0.0 { return Some(v); }
                }
            }
        }
    }
    // 2) sensor list / sdr elist 聚合
    for args in [vec!["sensor", "list"], vec!["sdr", "elist"]] {
        let mut pins = Vec::new();
        let mut pouts = Vec::new();
        let mut others = Vec::new();
        if let Ok(o) = Command::new("ipmitool").args(&args).output().await {
            for l in String::from_utf8_lossy(&o.stdout).lines() {
                let parts: Vec<&str> = l.split('|').map(|x| x.trim()).collect();
                if parts.len() < 3 || !parts[2].to_lowercase().starts_with("watt") { continue; }
                let v = match parts[1].parse::<f64>() { Ok(v) => v, Err(_) => continue };
                let nm = parts[0].to_uppercase();
                if nm.contains("PIN") { pins.push(v); }
                else if nm.contains("POUT") { pouts.push(v); }
                else { others.push(v); }
            }
        }
        if !pins.is_empty() { return Some(pins.iter().sum()); }
        if !pouts.is_empty() { return Some(pouts.iter().sum()); }
        if !others.is_empty() { return Some(others.iter().cloned().fold(0.0, f64::max)); }
    }
    None
}
