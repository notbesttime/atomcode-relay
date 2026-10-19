# atomcode-relay

把 AtomCode（AtomGit CodingPlan / AI Hub）的免费额度反代成标准 **Chat API**（OpenAI / Anthropic 兼容）。
单文件、**纯 Python 标准库、零依赖**——下载即跑，不需要 pip install 任何东西。

> English: A single-file, zero-dependency (stdlib-only) reverse proxy that turns an
> AtomCode (AtomGit CodingPlan) account into a standard OpenAI-compatible /
> Anthropic-compatible **chat** API — with a stream-reliability layer
> (keepalive, stall recovery, auto-continuation, repetition fuse) that
> similar projects generally don't have.

```
python atomcode2api.py --port 8787
# Base URL: http://127.0.0.1:8787/v1    API Key: 随便填
```

前提：本机登录过官方 AtomCode CLI（代理直接复用 `~/.atomcode/auth.toml` 的凭证，
token 过期自动刷新写回）。

---

## 核心思路：把上游"变成"标准 Chat API

上游 AtomCode 的网关**只会说 chat（`/v1/chat/completions`）这一种方言**，
而且带着各种不标准行为（200 伪装错误、思考吃预算、UA 签名墙、坏掉的 responses 端点）。
本项目做的事就是把它**规整成任何一个客户端都能直接使用的标准 Chat API**：

```
你的客户端（任何会说标准话的）          上游（只会 chat，还爱耍赖）
─────────────────────────────         ─────────────────────────
OpenAI Chat  /v1/chat/completions ──▶ 透传 + 预算/温度治理 + 伪错误拦截 + 复读熔断
Anthropic    /v1/messages          ──▶ 双向翻译成 chat，max_tokens 必填只能抬
OpenAI Responses /v1/responses     ──▶ 由 chat 桥接合成（上游原生实现不合规），
                                       含心跳/挂死自动续写/截断续写
GET /v1/models                    ──▶ AI Hub 目录 ∪ CodingPlan 名单（见下）
```

**给 agent 客户端用户的建议：优先用 chat 格式（`/v1/chat/completions`）**。
以 DSH 为例，provider 配 `api: openai-completions`——chat 路上思考内容
（`reasoning_content`）有原生明文回放，**跨轮思考连续**；responses 桥则受限于
上游没有加密思考（`encrypted_content`），跨轮不回放。Claude Code 用户直接指
`ANTHROPIC_BASE_URL=http://127.0.0.1:8787` 即可（Anthropic 也是 chat 形态）。

## 为什么选它（对比同类 AtomCode 反代）

同类项目（AtomCode2api 等）大多只做**协议转换**。这个项目在此之上解决的是
**"接上真实 agent 客户端后流会不会死"**——全部机制源自对官方 Rust 客户端
重试/超时行为的实测对齐，并有 20 个自动化回归场景背书。

| 能力 | atomcode-relay | 一般同类反代 |
|---|---|---|
| 依赖 | **零**（纯 stdlib 单文件） | Flask / requests 等 |
| 三协议 chat / responses / messages | ✅ | 部分支持 |
| **看得到 B 池**（见下节） | ✅ 目录并集 | 多数只见 A 池 |
| 思考模型心跳续命（防客户端 idle watchdog 超时） | ✅ 20s 合成增量，客户端无感 | ❌ |
| 上游挂死检测（120s 静默）→ 保留已输出、干净收尾 | ✅ 绝不伪造 `[DONE]` | ❌（多为读超时炸流） |
| 挂死/写满预算 → **自动续写**（带思考原文回灌 + 接缝去重） | ✅ ≤2 次，失败如实 incomplete | ❌ |
| 首个可见事件前静默 → 透明重开整个请求 | ✅（对齐官方 "metadata is replay-safe"） | ❌ |
| 复读熔断（模型退化重复时提前切断） | ✅ | ❌ |
| 思考模型温度缺省注入（防贪心复读） | ✅ 客户端传了绝不动 | ❌ |
| 输出预算治理（抬到 32768，防思考吃光正文） | ✅ | ❌ |
| 上游 200 伪装错误（"负载过高"/额度不足等）→ 改判 503 | ✅ | ✅ |
| 自动化回归（假上游剧本 ×20 场景） | ✅ 随包 | ❌ |

## 多看到一个池子：A 池 + B 池目录并集

AtomCode 账号背后其实有**两组模型、两套额度**，上游 `/v1/models` 只列其一：

- **A 池（CodingPlan 套餐专属）**：`glm5.3-flash`、`qwen3.8-27b`。
  上游目录**根本不列它们**（但能调）——同类反代大多只暴露这两个，或只列目录里的。
- **B 池（AI Hub 公共模型）**：**非套餐用户也能使用**的公共模型目录，走**独立的
  每月 token 配额**（不是套餐的 5 小时滚动窗口）。例如：

  ```
  deepseek-ai/DeepSeek-V4-Flash     zai-org/GLM-5.1
  Qwen/Qwen3-32B                    Qwen/Qwen3-30B-A3B-Thinking-2507
  Qwen/Qwen3-30B-A3B-Instruct-2507  Qwen/Qwen2.5-VL-72B (视觉)
  ```

本项目的 `/v1/models` 返回 **A ∪ B = 10 个模型**，一次看全两个池子。
注意：B 池月度配额耗尽后返回 503（代理会把上游拿 200 伪装的"每月token额度已不足"
如实改判），次月重置；别把 503 当 bug——那是池状态。

**反代出来的 API 理论上不消耗套餐的 5h×200 窗口**（2026-10-19 实测：多次反代
调用，`window_tokens_used` 零增长；机制上窗口计数只挂在官方客户端的签名路径，
中性 UA 通道不记——但上游未公开该机制，此说法请保守理解）。调用**会进 usage
台账**（60 天用量流水，仅记录、不限流）。A 池真正的硬约束是套餐到期日。

**并发**：实测 A 池同一模型并发上限约 **2**（两条同时在飞正常返回，第三条会被
上游 0.2s 快速拒绝并转 503）。也就是说可以同时开两个会话使用，别起并发压测。
不同模型各自有池，互不占额度位。

## 快速开始

```bash
python atomcode2api.py --port 8787
```

| 客户端 | 配置 |
|---|---|
| OpenAI SDK / 任意 OpenAI 兼容 | `base_url=http://127.0.0.1:8787/v1`，key 任意 |
| Claude Code | `ANTHROPIC_BASE_URL=http://127.0.0.1:8787`（走 `/v1/messages`） |
| DSH | provider 配 `api: openai-completions`（跨轮思考可回放，见上节） |

### 环境变量（全部可选）

| 变量 | 默认 | 说明 |
|---|---|---|
| `ATOMCODE_TOKEN_FLOOR` | 32768 | 输出预算下限（思考+正文共用，小了会被思考吃光） |
| `ATOMCODE_TEMPERATURE` | 0.8 | 客户端未传 temperature 时注入（0=关闭；防贪心复读） |
| `ATOMCODE_LOOP_CUT` | 1 | 复读熔断开关（连续 5 份相同周期即切断收尾） |
| `ATOMCODE_KEEPALIVE` | 20 | responses 桥心跳间隔秒 |
| `ATOMCODE_STALL` | 120 | 挂死判定秒（须小于客户端 watchdog） |
| `ATOMCODE_CONTINUES` | 2 | 自动续写次数上限（0=关闭） |
| `ATOMCODE_FIRST_EVENT` | 60 | 首可见事件预算秒（超时安全重开） |
| `ATOMCODE_UPSTREAM` | api-ai.gitcode.com/v1 | 上游地址 |
| `ATOMCODE_UA` | curl/8.6.0 | 中性 UA（上游按 UA 决定是否强制签名） |

## 测试

```bash
python tests/test_guard.py        # 熔断/温度 单测
python tests/test_passthrough.py  # 透传回归 ×8（重开/收尾/503/真流式/熔断）
python tests/test_continue.py     # 续写回归 ×12（挂死/接缝/耗尽/思考回灌）
```

## 已知限制（诚实清单）

- **上游无服务器端会话状态**：跨轮"思考连续性"只有 chat 协议有原生明文回放
  （`reasoning_content`）；responses 桥路径受限于上游无 `encrypted_content`。
- **CodingPlan Lite 有到期日**，到期后 A 池可能整体失效；AI Hub（B 池）另有
  月度 token 配额，耗尽后返回 503 属正常。
- 上游按模型分池限流，`glm5.3-flash` 等高峰期有可观概率回 200 伪装错误（已拦截为 503）。
- 自动续写会让同一次提问在 usage 台账多计一笔（有上限保护）。
- 本项目非官方，请自行评估与上游服务条款的兼容性，风险自负。

## 免责声明

- 本项目**仅供个人学习与技术研究**，用于理解 OpenAI/Anthropic 兼容网关与
  SSE 流可靠性工程；**不提倡、不赞同**将 API 额度进行分发、共享、转售或任何
  形式的牟利行为，**不赞同**以批量注册、邀请刷取等方式消耗上游的福利机制。
- 使用者应自行阅读并遵守上游服务条款；因使用本项目导致的账号受限、额度
  回收或其他后果，作者不承担任何责任。
- 请在你自己拥有的账号上测试，别把它变成别人的成本。

## License

MIT
