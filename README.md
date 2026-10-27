# vllm-dashboard — vLLM 推理监控大屏


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
| | **主屏（固定第一页签）** | **固定第一页签不可移除**：机器数（N 台在线/离线名单）、Decode/Prefill 合计吞吐、运行+排队合计、KV 最高（标机器）、TPOT p95 均值 6 卡 + 合计曲线 3 张（Decode/Prefill/并发）+ 各机累计 Decode 对比 1 张 + 机器明细表（点击行切换到该机大屏）+ 全场累计卡 |
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
| **API 分发网关** | OpenAI 兼容反代 | `--gw` 启用，`/gw/v1/*` 端点；多渠道优先级+权重路由、连接类失败自动转移、SSE 流式逐 chunk 透传（自动注入 include_usage 拿准确用量） |
| | Key 管理（UI） | ⚙ → API 分发：一键生成 sk- Key / 命名 / RPM+token 配额 / 启停 / 删除；「复制接入」出 curl+Python SDK 现成接入片段 |
| | 限流与记账 | Key 级 401/403/429；token 记账与引擎 usage 精确对账；渠道级 3 连败熔断 60s；网关统计（请求/成功/失败/token/均延迟） |
| **负载均衡** | 策略分流 | 五种策略同优先级层内分流：加权随机 / 轮询 / P2C 二选一（比在途请求数）/ 最少负载（按引擎运行请求数，监控指标感知）/ **会话亲和**（同对话粘同机保前缀缓存；新会话分给粘性会话数最少的机器防挤一台，空闲 30min 过期）；跨优先级层自动故障转移 |
| | 主动探活 | 渠道每 10s GET /health；探活失败自动摘除（不再被路由命中），全挂不雪崩 |
| | 负载均衡总表 | 渠道 × 引擎实时一张表：状态/探活延迟/在途/请求失败/QPS(1m)/token/均延迟 + 引擎侧运行/排队/KV/Decode tok/s（渠道地址与监控目标一致时自动联动）+ 合计行 |
| **部署特性** | 单文件零依赖 | 纯 Python 标准库，本地/服务器一条命令起；`--demo` 内置仿真数据源可无服务预览；页面隐藏自动暂停轮询 |

## 安装（同一仓库选版本，v1/v2 并存）

仓库已发布：**https://github.com/xzwgit/vllm-dashboard**（public）。在目标机器（Linux，需 curl 或 wget）上选版本安装：

```bash
# 一键式：装 v2（监控 + API 网关 + 负载均衡 + 主屏） / v1（纯监控大屏）
curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v2
curl -fsSL https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh | bash -s v1
# 或先落脚本再装（第二参数=安装目录）
curl -fsSL -o install.sh https://raw.githubusercontent.com/xzwgit/vllm-dashboard/master/install.sh
./install.sh v2 /opt/dash
./install.sh list      # 列出仓库已发布的版本 tag
```

- **版本来源**：`v1`/`v2` 优先取 git tag（历史版本固定不变）；tag 不可达时回退 master（v1=冻结副本）；`latest`=master 开发版；支持 `./install.sh vX.Y.Z` 直装指定 tag。
- **raw 被墙自动兜底**：raw.githubusercontent.com 不可达的网络（本机即如此）自动改走 api.github.com（base64 解码，无需配置）——2026-10-27 本机实测全走兜底装成。
- 下载后校验 `VERSION` 常量；覆盖前备份 `.bak.<旧版本>.<时间戳>`；装完按版本打印启动命令。
- **推送通道**：本机 github.com/raw 时通时断，**推送走跳板机 14.1.99.114（tar 上去用它家 classic token 推）**，raw 验证也在跳板机上 curl。

## 快速开始

```bash
# 监控 + API 分发网关（本地转发 → 生成 Key → 局域网分发）
python vllm_dashboard.py --gw --target 157-1017=http://10.10.3.157:8123
#   网关端点 http://<本机IP>:8125/gw/v1（OpenAI 兼容）；页面 ⚙ → API 分发 里生成 Key 发同事
# 监控一个 vLLM 服务（服务地址 = OpenAI 端口，/metrics 自动拼接）
python vllm_dashboard.py --target 1017@154=http://10.10.3.154:8123
# 多目标（页签切换，页面里也可随时增删，存 targets.json）
python vllm_dashboard.py --port 8124 --target a=http://10.10.3.154:8123 --target b=http://10.10.50.180:8123 --api-key abc.12345
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

新旧指标名都兼容（`generation_tokens_total`/`token_generation_tokens_total`、`kv_cache_usage_perc`/`gpu_cache_usage_perc`、`request_time_per_output_token_seconds`/`time_per_output_token_seconds`、`e2e_request_latency_seconds`/`request_latency_seconds`、`request_queue_time_seconds`/`time_in_queue_requests`），vLLM 0.32.0 实测对齐（2026-10-24，.154 上 1017+DSpark k4 实测清单）。

## 文件

- `vllm_dashboard.py` — 工具本体（后端抓取/聚合 + 内嵌前端，~1600 行）
- `vllm_dashboard_v1.py` — v1.0.0 冻结副本（install.sh 的 v1 回退来源）
- `install.sh` — 版本安装脚本（v1/v2/latest/指定 tag/list；tag 优先、回退 master 冻结副本、VD_PROXY 镜像前缀；2026-10-27 本地 mock 源全分支测试 + GitHub live list 通过）
- `rrun.py` — paramiko 远程执行/上传小工具（154/158/162/180/55，凭据见《服务器台账.md》；`.154` 的 sftp 子系统对 paramiko 报 ENOENT，用 `--bput` base64 直写绕过；Git Bash 下带绝对路径参数须加 `MSYS_NO_PATHCONV=1`）
- `test_mock_openai.py` — mock OpenAI 上游（非流式+流式+models+/health+/metrics），手边没有 vLLM 时联调网关用
- `test_lb_v3.py` — 负载均衡本地测试套件（双 mock：策略分流/故障转移/探活/总表，17 项断言，2026-10-27 ALL PASS）
- `boot_lb_158.sh` / `boot_lb_162.sh` / `lb_wait_*.sh` / `lb_burst_158.py` — 2026-10-27 双机负载均衡真机测试的操作脚本存档（.158/.162 各 boot 一个 1017 副本 → 大屏+网关 → burst → 故障转移）
- `pull_env_weights_154.sh` / `boot_1017_154.sh` / `bench_154.sh` / `wait_ready_154.sh` / `mm_*_158.*` / `agent_loop_158.sh` / `boot_157_gwtest.sh` — 2026-10-24~26 各轮实测的操作脚本存档

## 2026-10-24 .154 实测记录

- 机器：10.10.3.154（server04，8×RTX PRO 6000 96G）
- 环境：从 .158 rsync `vllm-fi02` venv（0.32.0+FI0.7.2+双补丁树，8.3G）+ FI/编译缓存；venv python 重链本机 uv cpython-3.12.13
- 权重：.158:/data/models/DeepSeek-V4.1-Flash-1017 → .154:/data/models（476G，同网段 ~1GB/s）
- boot：部署方案 v2.0 §4.1 形态（TP8/EP8、fp8 KV、FLASHINFER_MLA_SPARSE_DSV41、FULL 图、DSpark k4、util 0.90、capture 640、1M ctx、:8123/abc.12345）
- 验证：探针 17*19→323 ✓、boot 日志 FULL_DECODE_ONLY ✓、/metrics 匿名可访问 ✓
- bench：c32 1K×1K = **959 tok/s**（聚合，96G 参考带）；c32 2K×1K rate8 = 882 tok/s、TPOT 23.7ms、DSpark 接受率 23.6-27.8%（random 数据无前缀结构，偏低属预期）
- 大屏：.154 侧 `python3 /root/dash_ops/vllm_dashboard.py --port 8124 --target '1017@154'=http://127.0.0.1:8123`（http://10.10.3.154:8124/）；本地侧同脚本指 http://10.10.3.154:8123 亦可（本地直连可达）

## 状态（2026-10-27）

- **已发布 GitHub**：https://github.com/xzwgit/vllm-dashboard（public；**版本重排后 tag = v1.0.0 / v2.0.0 两个**；install.sh 选版本安装，raw 被墙自动 API 兜底）
- 公开仓只放：vllm_dashboard.py / vllm_dashboard_v1.py / install.sh / test_mock_openai.py / test_lb_v3.py / 脱敏 README——**rrun.py（含凭据）、各轮 boot/bench 脚本（含内网 IP）不上仓**，只留工作区
- .158/.162 真机测试环境 2026-10-27 已清理（引擎/大屏全停，双机 GPU 0，dash_ops 留脚本+日志）

## 版本记录
| 版本 | 日期 | 内容 |
|---|---|---|
| v1.0.0 | 2026-10-26 | 纯监控大屏：多目标抓取 / 11 张 vLLM 指标卡 / 5 图 / 服务器信息 / 模型接入卡 / CSV 导出 / 失败诊断 |
| v2.1.0 | 2026-10-27 | **v2 大版打磨**（29 项合并）：负载均衡第五策略**会话亲和**（同对话粘同机保前缀缓存、最少粘性数分配防挤一台）；主屏定版（KPI 9 卡铺满含缓存命中/接受率、图表 2×2、服务器列表右列内嵌滚动、AI服务详情通栏、掉线报警横幅、服务下拉页签）；网关面板交互（右上 ✕ 关闭、渠道/Key 折叠表单）；demo 仿真引擎参数化 --demo-count；修复合计图恒空/JS 重复声明/滚动条抖动等 |
| v2.0.0 | 2026-10-27 | **监控 + 网关 + 负载均衡 + 主屏**：API 分发网关（OpenAI 兼容反代/Key 管理/记账/熔断）；负载均衡（四种策略/主动探活/故障转移/负载均衡总表）；主屏（固定第一页签：全场合计卡×7+合计曲线+各机对比+机器明细 13 列+右列服务器列表纯硬件简况）；选版本安装脚本（raw 被墙自动 API 兜底） |


## 已知事项

- vLLM 0.32.0 无 KV 池 token 数 gauge（只有 perc）→ "池容量"显示 —，属上游指标缺失
- 计数器重启（引擎重启）自动识别，速率不回负
- 抓取失败按原因分类报人话：连接被拒=vLLM 未启动/端口不对（Linux Errno 111 / Windows 10061 都认）；HTTP 401/403=鉴权失败提示填 Key；404=路径不对；超时/域名解析错误各自提示；均附原始错误摘要
- 页面隐藏时暂停轮询，回来即恢复；时间范围 5m/15m/1h/全部
- 远程机落地的脚本/日志都在 `/root/dash_ops/`（工作区规约）
