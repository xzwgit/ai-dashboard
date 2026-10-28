# ai-dashboard 配置编写指南（给 AI agent）

大屏（AI 推理性能监控大屏）支持一键导入一份 JSON 配置，完成监控目标与网关渠道的初始化。
入口：页面 ⚙ 菜单 →「⬆ 导入配置」，或 POST /api/config/import（body: {"config": <下方结构>}）。

## 配置结构

```json
{
  "targets": [
    {"name": "svc-a", "url": "http://10.0.0.1:8123", "api_key": "",
     "model_path": "/data/models/Your-Model-A", "context_len": "1048576"}
  ],
  "gateway": {
    "policy": "round_robin",
    "channels": [
      {"name": "a", "url": "http://10.0.0.1:8123", "api_key": "", "priority": 1, "weight": 10}
    ]
  }
}
```

## 字段说明

- targets[]（监控目标，即每个 AI 推理服务）
  - name：显示名（页签/AI服务详情/服务器列表里的名字），必填、唯一
  - url：vLLM 服务根地址（OpenAI 端口；大屏自动拼 /metrics 抓指标），必填，http(s)://host:port
  - api_key：该服务 /metrics 需要 Bearer 时填（vLLM 的 /metrics 通常匿名，留空即可）
  - model_path：权重目录完整路径——**建议填**：AI服务详情表的「模型」列显示的是该路径的最后一段全名
  - context_len：上下文长度（显示用，字符串或数字）
- gateway（API 分发网关，--gw 启动时生效）
  - policy：负载均衡策略，五选一：weight（加权随机）/ round_robin（轮询）/ p2c（二选一）/ least_requests（最少负载）/ session（会话亲和，多轮对话保前缀缓存，推荐对话类负载）
  - channels[]：上游渠道（即各推理服务）
    - name：渠道名，必填、唯一；url：同 targets.url；api_key：转发时携带的上游 Bearer（无鉴权留空）
    - priority：数字越小越优先；同优先级内按 policy 分流，高优先级整体不可用时自动落到低优先级（灾备用）
    - weight：同优先级内的权重比例（weight 策略下生效）

## 语义与注意

- 导入为**整体替换**：现有 targets 与 channels 会被清空后按本文件重建；网关已生成的客户端 Key（sk-…）不受影响
- secrets 口径：targets/channels 的 api_key 会随「导出配置」一并导出（内网服务凭据）；客户端 Key 永不导出
- 同机多服务：每台引擎机如在 :8124 跑一个同款大屏实例，主屏「服务器列表」会自动接入该机 CPU/GPU/内存简况
- 示例可直接 GET /api/config/example 获取；当前配置可 GET /api/config/export 导出

## 生成后自检清单

1. 每个 url 可达（curl <url>/health 返回 200）
2. channels 与 targets 的 url 通常一一对应（同名同址）；灾备场景才用不同 priority 分层
3. policy 按负载类型选：多轮对话=session；一次性高并发短输出=round_robin/least_requests；混合=weight
