// Prometheus exposition 解析 + 直方图分位（与 Python 版逐项同口径）
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Hist {
    pub buckets: Vec<(f64, f64)>, // (le, cum) 已按 le 升序
    pub sum: f64,
    pub count: f64,
}

#[derive(Default, Debug)]
pub struct SampleSet {
    pub simple: HashMap<String, f64>,
    pub labeled: Vec<((String, Vec<(String, String)>), f64)>,
    pub hist: HashMap<String, Hist>,
}

fn parse_value(s: &str) -> Option<f64> {
    match s {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => s.parse::<f64>().ok(),
    }
}

fn unesc(v: &str) -> String {
    v.replace("\\\"", "\"").replace("\\\\", "\\").replace("\\n", "\n")
}

pub fn parse_prom(text: &str) -> SampleSet {
    let mut out = SampleSet::default();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // name{labels} value
        let (name_part, rest) = match line.find(' ') {
            Some(i) => (&line[..i], line[i + 1..].trim()),
            None => continue,
        };
        let val = match parse_value(rest) {
            Some(v) => v,
            None => continue,
        };
        let (name, lbl_src) = if let Some(b) = name_part.find('{') {
            (&name_part[..b], name_part[b + 1..].trim_end_matches('}'))
        } else {
            (name_part, "")
        };
        let mut labels: Vec<(String, String)> = Vec::new();
        if !lbl_src.is_empty() {
            let mut i = 0usize;
            let b = lbl_src.as_bytes();
            while i < b.len() {
                // 解析 k="v" 对
                let eq = match lbl_src[i..].find('=') {
                    Some(x) => i + x,
                    None => break,
                };
                let k = lbl_src[i..eq].trim().to_string();
                if eq + 1 >= b.len() || b[eq + 1] != b'"' {
                    break;
                }
                let mut j = eq + 2;
                let mut val_buf = String::new();
                while j < b.len() {
                    match b[j] {
                        b'\\' if j + 1 < b.len() => {
                            val_buf.push(b[j + 1] as char);
                            j += 2;
                        }
                        b'"' => {
                            j += 1;
                            break;
                        }
                        c => {
                            val_buf.push(c as char);
                            j += 1;
                        }
                    }
                }
                labels.push((k, unesc(&val_buf)));
                // 跳到下一个逗号后
                while j < b.len() && b[j] == b' ' {
                    j += 1;
                }
                if j < b.len() && b[j] == b',' {
                    j += 1;
                }
                i = j;
            }
        }
        let le = labels.iter().find(|(k, _)| k == "le").map(|(_, v)| v.clone());
        if name.ends_with("_bucket") && le.is_some() {
            let le = match parse_value(&le.unwrap()) {
                Some(v) => v,
                None => continue,
            };
            let h = out
                .hist
                .entry(name[..name.len() - 7].to_string())
                .or_default();
            h.buckets.push((le, val));
        } else if name.ends_with("_sum") || name.ends_with("_count") {
            let base = if name.ends_with("_sum") {
                &name[..name.len() - 4]
            } else {
                &name[..name.len() - 6]
            };
            let h = out.hist.entry(base.to_string()).or_default();
            if name.ends_with("_sum") {
                h.sum = val;
            } else {
                h.count = val;
            }
        } else if !labels.is_empty() {
            let mut lk = labels;
            lk.sort();
            out.labeled.push(((name.to_string(), lk), val));
        } else {
            out.simple.insert(name.to_string(), val);
        }
    }
    for h in out.hist.values_mut() {
        h.buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }
    out
}

/// Prometheus histogram_quantile 插值；buckets=[(le,cum)] 升序。
pub fn hist_quantile(buckets: &[(f64, f64)], q: f64) -> Option<f64> {
    if buckets.is_empty() {
        return None;
    }
    let total = buckets[buckets.len() - 1].1;
    if total <= 0.0 {
        return None;
    }
    let rank = q * total;
    let (mut prev_le, mut prev_cum) = (0.0, 0.0);
    for &(le, cum) in buckets {
        if cum >= rank {
            if cum <= prev_cum {
                return Some(le);
            }
            let frac = (rank - prev_cum) / (cum - prev_cum);
            if le.is_infinite() {
                return Some(prev_le);
            }
            let qv = prev_le + (le - prev_le) * frac;
            return if qv.is_finite() { Some(qv) } else { None };
        }
        prev_le = le;
        prev_cum = cum;
    }
    Some(buckets[buckets.len() - 1].0)
}

/// 两份累计 bucket 快照差分 → 窗口直方图。
pub fn hist_delta(b_now: &[(f64, f64)], b_ref: Option<&[(f64, f64)]>) -> Vec<(f64, f64)> {
    if b_now.is_empty() {
        return Vec::new();
    }
    match b_ref {
        None => b_now.to_vec(),
        Some(ref_map) => {
            let m: HashMap<u64, f64> = ref_map
                .iter()
                .map(|(le, c)| (le.to_bits(), *c))
                .collect();
            b_now.iter()
                .map(|&(le, c)| {
                    let d = match m.get(&le.to_bits()) {
                        Some(rc) => (c - rc).max(0.0),
                        None => c,
                    };
                    (le, d)
                })
                .collect()
        }
    }
}

// ---------- 别名表（与 Python 版一致） ----------
pub const ALIAS: &[(&str, &[&str])] = &[
    ("gen_tokens", &["vllm:token_generation_tokens_total", "vllm:generation_tokens_total"]),
    ("prompt_tokens", &["vllm:prompt_tokens_total"]),
    ("running", &["vllm:num_requests_running"]),
    ("waiting", &["vllm:num_requests_waiting"]),
    ("swapped", &["vllm:num_requests_swapped"]),
    ("kv_perc", &["vllm:kv_cache_usage_perc", "vllm:gpu_cache_usage_perc"]),
    ("kv_used_tokens", &["vllm:kv_cache_usage_num_tokens", "vllm:gpu_cache_usage_num_tokens"]),
    ("kv_max_tokens", &["vllm:kv_cache_max_num_tokens", "vllm:gpu_cache_max_num_tokens"]),
    ("preemptions", &["vllm:num_preemptions_total"]),
    ("spec_accepted", &["vllm:spec_decode_num_accepted_tokens_total"]),
    ("spec_draft", &["vllm:spec_decode_num_draft_tokens_total"]),
    ("pc_queries", &["vllm:prefix_cache_queries_total"]),
    ("pc_hits", &["vllm:prefix_cache_hits_total"]),
];

pub const ALIAS_HIST: &[(&str, &[&str])] = &[
    ("ttft", &["vllm:time_to_first_token_seconds"]),
    ("tpot", &["vllm:request_time_per_output_token_seconds", "vllm:time_per_output_token_seconds"]),
    ("e2e", &["vllm:e2e_request_latency_seconds", "vllm:request_latency_seconds"]),
    ("queue", &["vllm:request_queue_time_seconds", "vllm:time_in_queue_requests"]),
];

pub const SERIES_KEYS: &[&str] = &[
    "gen_rate", "prompt_rate", "running", "waiting", "kv_perc",
    "ttft_p50", "ttft_p95", "tpot_p50", "tpot_p95", "e2e_p95", "queue_p95",
    "spec_accept", "prefix_hit", "gen_tokens_total", "prompt_tokens_total",
];
