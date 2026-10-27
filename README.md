# vllm-dashboard — vLLM 推理监控大屏

> 单文件、纯 Python 标准库的 vLLM 应用层监控大屏（Prometheus /metrics 抓取 → 中文现代大屏），2026-10-24 建。
> 定位：替代 Prometheus+Grafana 组合里最常用的那部分 vLLM 指标看板，零依赖、本地或服务器直跑。

## 界面结构（2026-10-24 终版）

- 标题独占居中行（两侧留空）；工具栏在下一行 = 目标页签 | 时间范围 | **⚙ 设置**（收起状态；菜单内含"＋ 添加监控目标"与"⬇ 导出日志（CSV）"）；右上无时钟（时间以服务器卡为准）
- **左上：模型 / 接入信息卡**（版本 JSON 自动解析；接入地址自动把 127.0.0.1 换成页面实际访问主机），下方 vLLM KPI 卡两行左对齐（紧凑小卡）：Decode 吞吐 / Prefill 吞吐（独立两卡，带英文与累计值）/ KV Cache 剩余（进度条+90% 告警）/ 运行中请求 / TTFT p95 / TPOT p95 / 端到端 p95 / 前缀缓存命中 / 投机接受率 / KV 抢占 / 已完成请求
- **右区服务器信息**（大屏所在宿主机；部署在引擎机上=引擎宿主机）：内存卡（GB+进度条）+ 服务器信息表（IP/主机名/服务器时间/**运行时间**/CPU 使用率(带小条)/GPU 利用率(带小条)/GPU 型号 × 数量/系统）；CPU、GPU 不单开卡
- 图表 5 张：Decode 吞吐 / Prefill 吞吐（各自独立图）/ KV Cache 使用率 / 并发请求（运行+排队）/ 延迟分位（TTFT/TPOT/E2E/排队 p95，对数轴）；悬停出十字线+数值
- 底部卡片：请求统计、目标信息、累计量（模型/接入信息已移至左上）
- **⬇ 导出日志**：当前目标全部监控序列一键导出 CSV（带 BOM，Excel 直接开；含 target/时间/口径注释头）
- 时间范围 5m/15m/1h/全部；页面隐藏时暂停轮询；多目标页签切换；目标持久化 targets.json

## 功能总表

| 模块 | 功能 | 说明 |
|---|---|---|
| **监控接入** | 多目标监控 | 页签切换、页面随时增删，配置持久化 targets.json；vLLM 新旧指标名自动兼容（0.32.0 实测对齐） |
| | **全场总览（v3.1）** | **固定第一页签**：机器在线数、Decode/Prefill 合计、运行+排队合计、KV 最高、TPOT p95 均值 + 合计曲线 ×3 + 各机累计 Decode 对比 + 机器明细表（点击行切到该机） |
| | 指标抓取 | /metrics 2 秒轮询，内存滚动保留约 3 小时；计数器重启自动识别不回负 |
| | 鉴权 | 目标可单独配 Bearer Key；/metrics 匿名时留空即可 |
| | 失败诊断 | 抓取失败按原因报人话：服务未启动/端口不对、鉴权失败(401/403)、路径不对(404)、超时、域名错；附原始错误摘要 |
| **vLLM 指标卡** | Decode 吞吐 | tok/s（12s 窗口差分）+ 累计值 + 迷你趋势线 |
| | Prefill 吞吐 | 同上，独立卡片独立图（多模态图片 token 也计入这里） |
| | KV Cache 剩余 | 剩余比例 + 进度条 + 已用量；≥70% 橙色 ≥90% 红色，图上 90% 告警线 |
| | 并发请求 | 运行中 / 排队 / 换出 |
| | TTFT / TPOT | p50+p95，30s 窗口直方图分位（只统计新完成请求） |
| | 端到端 / 排队延迟 | 窗口 p95 |
| | 前缀缓存命中率 | Δhits/Δqueries（Agent 多轮场景可见） |
| | 投机解码接受率 | Δaccepted/Δdraft（DSpark 等） |
| | 稳定性 | KV 抢占次数、已完成请求数（stop/length 细分）、累计 token 量 |
| **图表** | 5 张曲线图 | Decode、Prefill、KV 使用率、并发（运行+排队）、延迟分位（对数轴）；悬停出十字线+数值 |
| | 时间窗 | 5 分钟 / 15 分钟 / 1 小时 / 全部 |
| **服务器信息** | 内存 | 已用/总量 GB + 进度条 |
| | 服务器信息表 | IP/主机名、服务器时间、运行时间、CPU 型号+核数、CPU 使用率、GPU 利用率、GPU 型号×数量、系统发行版、内核（部署在引擎机上=引擎宿主机资源） |
| **模型/接入** | 模型卡 | 模型 ID（自动抓 /v1/models）、vLLM 版本（自动抓 /version）、上下文长度、权重路径、接入地址（127.0.0.1 自动替换为实际访问 IP）、API Key——客户端接入信息一眼可抄 |
| **工具** | ⚙ 设置菜单 | 添加监控目标 / 导出日志 |
| | 导出日志 | 当前目标全部序列一键 CSV（带 BOM，Excel 直开；含目标信息与口径注释头） |
| **API 分发网关（v2+）** | OpenAI 兼容反代 | `--gw` 启用，`/gw/v1/*` 端点；多渠道优先级+权重路由、连接类失败自动转移、SSE 流式逐 chunk 透传（自动注入 include_usage 拿准确用量） |
| | Key 管理（UI） | ⚙ → API 分发：一键生成 sk- Key / 命名 / RPM+token 配额 / 启停 / 删除；「复制接入」出 curl+Python SDK 现成接入片段 |
| | 限流与记账 | Key 级 401/403/429；token 记账与引擎 usage 精确对账；渠道级 3 连败熔断 60s；网关统计（请求/成功/失败/token/均延迟） |
| **负载均衡（v3）** | 策略分流 | 四种策略同优先级层内分流：加权随机 / 轮询 / P2C 二选一（比在途请求数）/ 最少负载（按引擎运行请求数，监控指标感知）；跨优先级层自动故障转移 |
| | 主动探活 | 渠道每 10s GET /health；探活失败的渠道自动摘除（不再被路由命中），全挂时不雪崩 |
| | 负载均衡总表 | 渠道 × 引擎实时一张表：状态 / 探活延迟 / 在途 / 请求失败 / QPS(1m) / token / 均延迟 + 引擎侧运行/排队/KV/Decode tok/s（渠道地址与监控目标一致时自动联动）+ 合计行 |
| **部署特性** | 单文件零依赖 | 纯 Python 标准库，本地/服务器一条命令起；`--demo` 内置仿真数据源可无服务预览；页面隐藏自动暂停轮询 |

## 安装（同一仓库选版本，v1/v2 并存）

在目标机器（Linux，需 curl 或 wget）上选版本安装：

```bash
# 一键式：装 v3（监控 + 网关负载均衡 + 负载均衡总表）
curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v3
# 装 v2（监控 + API 分发网关）
curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v2
# 装 v1（纯监控大屏）
curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v1
# 或先落脚本再装（第二参数=安装目录）
curl -fsSL -o install.sh https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh
./install.sh v2 /opt/dash
./install.sh list      # 列出仓库已发布的版本 tag
```

- **版本来源**：`v1` / `v2` 优先取 git tag（`v1.0.0` / `v2.0.0`，历史版本固定不变）；tag 不可达时自动回退 master（v1 = 冻结副本 `vllm_dashboard_v1.py`，v2 = 主文件）；`latest` = master 开发版；也支持 `./install.sh v2.0.1` 直装指定 tag。
- **raw 被墙自动兜底**：raw.githubusercontent.com 不可达的网络（国内常见）会自动改走 api.github.com 取文件（base64 解码，无需配置；需目标机有 python3）。
- 下载后校验 `VERSION` 常量（防代理/网关返回错误页）；覆盖旧文件前自动备份 `.bak.<旧版本>.<时间戳>`；装完按版本打印下一步启动命令。
- GitHub 直连不通的机器：`VD_PROXY=https://mirror.ghproxy.com ./install.sh v2`（raw 地址加镜像前缀）。
- 版本 tag 随仓库发布：`v1.0.0` 指向 v1 内容的历史提交（该提交上 `vllm_dashboard.py` 即 v1），raw 地址取 tag 即固定取到该版本。

## 快速开始

```bash
# 监控 + API 分发网关（本地转发 → 生成 Key → 局域网分发）
python vllm_dashboard.py --gw --target my-vllm=http://127.0.0.1:8123
#   网关端点 http://<本机IP>:8125/gw/v1（OpenAI 兼容）；页面 ⚙ → API 分发 里生成 Key 发同事
# 监控一个 vLLM 服务（服务地址 = OpenAI 端口，/metrics 自动拼接）
python vllm_dashboard.py --target my-vllm=http://127.0.0.1:8123
# 多目标（页签切换，页面里也可随时增删，存 targets.json）
python vllm_dashboard.py --port 8124 --target a=http://10.0.0.1:8123 --target b=http://10.0.0.2:8123 --api-key sk-xxxx
# 没有服务时看界面效果（内置仿真数据源，周期负载：空闲→爬坡→c32→排空）
python vllm_dashboard.py --demo
```

浏览器打开 `http://127.0.0.1:8124/`。`--api-key` 为抓取默认 Bearer（vLLM 的 /metrics 通常匿名可访问，带上无害）。

## 指标口径（KPI 与图表）

| 指标 | 口径 |
|---|---|
| Decode / Prefill 吞吐 | `generation_tokens_total` / `prompt_tokens_total` 计数器差分（trailing ~12s 窗口），卡上另给累计值 |
| KV Cache 剩余 | `1 - kv_cache_usage_perc`（0.32.0 名）+ 90% 告警线；池 token 数在指标存在时显示 |
| TTFT / TPOT / E2E / 排队延迟 | 直方图 bucket 差分（trailing ~30s 窗口）求 p50/p95，只统计窗口内新完成请求；延迟图对数轴 |
| 投机解码接受率 | Δ`spec_decode_num_accepted_tokens_total` / Δ`spec_decode_num_draft_tokens_total`（窗口） |
| 前缀缓存命中 | Δ`prefix_cache_hits_total` / Δ`prefix_cache_queries_total` |
| 并发 / 抢占 / 完成数 | `num_requests_running` / `waiting`、`num_preemptions_total`、`request_success_total{finish_reason}` |

新旧指标名都兼容（`generation_tokens_total`/`token_generation_tokens_total`、`kv_cache_usage_perc`/`gpu_cache_usage_perc`、`request_time_per_output_token_seconds`/`time_per_output_token_seconds`、`e2e_request_latency_seconds`/`request_latency_seconds`、`request_queue_time_seconds`/`time_in_queue_requests`），vLLM 0.32.0 实测对齐（含投机解码场景）。

## 文件

- `vllm_dashboard.py` — 工具本体（后端抓取/聚合 + 内嵌前端，~1600 行）
- `vllm_dashboard_v1.py` — v1.0.0 冻结副本（install.sh 的 v1 回退来源）
- `install.sh` — 版本安装脚本（v1/v2/latest/指定 tag/list；tag 优先、回退 master 冻结副本、VD_PROXY 镜像前缀）
- `test_mock_openai.py` — mock OpenAI 上游（非流式+流式+models+/health+/metrics），手边没有 vLLM 时联调网关用
- `test_lb_v3.py` — 负载均衡本地测试套件（双 mock：策略分流/故障转移/探活/总表，17 项断言）

## 实测背景

在 8×RTX PRO 6000 96G（SM120）+ vLLM 0.32.0 + FlashInfer 0.7.2 的多卡 MoE 推理服务（TP8/EP8 + 投机解码）上完成实测：/metrics 指标口径逐项对齐、数十并发压测全程监控曲线连续无断点、网关在真实引擎上完成流式/非流式/记账对账（token 记账与引擎 usage 精确一致）。

## 状态

- 已发布：本仓库 master = v2（监控 + 网关）；tag `v1.0.0` / `v2.0.0` 并存，`./install.sh list` 可见
- v1（纯监控）与 v2（监控+网关）同仓按版本安装，互不覆盖

## 版本记录

| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0.0 | 2026-10-26 | 定版：监控大屏基础功能（多目标抓取/11 张 vLLM 指标卡/5 图/服务器信息/模型接入卡/CSV 导出/失败诊断）。冻结副本 `vllm_dashboard_v1.py` |
| v2.0.0 | 2026-10-26 | **内置轻量网关交付**（M1+M2+M3 一次完成）：--gw 启用；/gw/v1/* OpenAI 兼容反代+渠道路由故障转移+SSE 透传；Key 生成/配额/RPM/记账（UI）；熔断；⚙ 面板。mock 全链路 10/10 + 真机 vLLM（流式/记账对账 token 精确一致）+ v1 监控回归全过 |
| v3.1.0 | 2026-10-27 | **固定总览页签**（第一个屏=全场大屏：几台机+全场合计；固定不可删，其余机器页签可增删）：合计卡×6 + 合计曲线×3 + 各机累计对比 + 机器明细表（点击跳转）+ 网关状态行 |
| v3.0.0 | 2026-10-27 | **网关负载均衡 + 负载均衡总表**：四种策略（加权随机/轮询/P2C/最少负载-引擎指标感知，vllm-router 同款策略族）；主动探活（10s /health，失败自动摘除）；在途请求计数；QPS 窗口；总表=渠道×引擎实时一张表（探活/在途/QPS/token/引擎运行/KV/Decode+合计行）。本地双 mock 17/17 + 双机真机验证（轮询精确分流、探活 10s 摘除、故障转移 6/6） |

## 已知事项

- vLLM 0.32.0 无 KV 池 token 数 gauge（只有 perc）→ "池容量"显示 —，属上游指标缺失
- 计数器重启（引擎重启）自动识别，速率不回负
- 抓取失败按原因分类报人话：连接被拒=vLLM 未启动/端口不对（Linux Errno 111 / Windows 10061 都认）；HTTP 401/403=鉴权失败提示填 Key；404=路径不对；超时/域名解析错误各自提示；均附原始错误摘要
- 页面隐藏时暂停轮询，回来即恢复；时间范围 5m/15m/1h/全部
