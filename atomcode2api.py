#!/usr/bin/env python3
"""
atomcode2api — 把 AtomCode（AtomGit CodingPlan / AI Hub）账号反代成标准
OpenAI / Anthropic API。纯标准库，零依赖。

实测依据（2026-10-18，均可复现）：
  * 签名墙按「主机 + User-Agent」判定：
      llm-api.atomgit.com  无条件强制签名（换 UA 无效）
      api-ai.gitcode.com   UA 含 "atomcode" 才强制签名；中性 UA 直接放行
    所以代理必须把 UA 洗成中性，且绝不自称 atomcode。
  * api-ai 的调用不进 CodingPlan 的 5h 窗口额度；AI Hub 目录名连当日台账都不进。
  * api-ai 自带 /v1/responses 是坏的（object=response.delta、camelCase、永不发
    response.completed）→ 不透传，改由 /v1/chat/completions 桥接合成。
  * 上游把「思考 + 正文」塞进同一个 max_tokens 预算：客户端传的小值会被思考吃光，
    表现为正文 0 字 / finish_reason=length；流式则长时间只动思考 → 撞 idle timeout。
    官方 AtomCode 从不发这个字段（其 openai_compat 日志 reasoning_effort /
    thinking_type 皆 omitted）。所以 chat/completions 一律删掉该字段；Anthropic 协议
    里 max_tokens 必填，只能抬到下限。
  * 上游出错时也回 200 + 一句中文（"参数错误"/"请求负载过高"/"为 AtomCode 独享"）
    → 必须识别并改判 5xx，否则客户端静默拿到空回复。

用法：
    python atomcode2api.py --port 8787
    # 客户端 base_url = http://127.0.0.1:8787/v1 ，api_key 随便填
"""
import argparse
import json
import os
import queue
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = os.environ.get("ATOMCODE_UPSTREAM", "https://api-ai.gitcode.com/v1")
ACS = "https://acs.atomgit.com"
CODINGPLAN_API = "https://api.gitcode.com/api/v5/coding-plan"
DEFAULT_AUTH = os.path.expanduser("~/.atomcode/auth.toml")

# 网关按 UA 决定要不要强制签名，所以绝不能带 atomcode 字样。
NEUTRAL_UA = os.environ.get("ATOMCODE_UA", "curl/8.6.0")

# 上游沉默超过这个秒数，就补发一个真实的 response.in_progress 事件续命。
# 必须显著小于客户端 idle watchdog 的 300s。
KEEPALIVE_S = float(os.environ.get("ATOMCODE_KEEPALIVE", "20"))

# 必填协议（Anthropic）用的输出预算下限。
# 上游静默超过这个秒数就判定"挂死"，代理主动收尾（保留已有正文 + incomplete）。
# 必须小于客户端 watchdog 的 300s，否则客户端会在我们收尾前就报错。
STALL_S = float(os.environ.get("ATOMCODE_STALL", "120"))
# 续写轮内的静默阈值更短：这条连接已被证明不稳，再等 120s 是白等。
STALL_AGAIN_S = float(os.environ.get("ATOMCODE_STALL_AGAIN", "45"))
# 自动续写次数上限（0=关闭）。超过就发 incomplete 让宿主决定。
MAX_CONTINUES = int(os.environ.get("ATOMCODE_CONTINUES", "2"))
# 单轮续写的总时长保险丝（秒）。
CONTINUE_WAIT_S = float(os.environ.get("ATOMCODE_CONTINUE_WAIT", "240"))
# 透明重开：只在「可见内容还没送到客户端」时重发整个请求。
# 依据官方 retry.rs:5 "Metadata alone is replay-safe" / openai_compat.rs:820。
MAX_STREAM_ATTEMPTS = int(os.environ.get("ATOMCODE_STREAM_ATTEMPTS", "2"))
RETRY_BACKOFF_S = float(os.environ.get("ATOMCODE_RETRY_BACKOFF", "2"))
# 等"首个可见事件"的上限（prefill/TTFB 阶段）。取值依据：
#   实测最坏 TTFB 6.5s（330KB 上下文 + 40 工具），60s ≈ 9 倍余量；
#   官方对 prefill 给 600s，但我们要把「N 次尝试」压在客户端 300s 之内：
#   60 + 退避2 + 60 = 122s < 300s。设太大会白等，太小会误重开（A 池多计一次）。
FIRST_EVENT_S = float(os.environ.get("ATOMCODE_FIRST_EVENT", "60"))
TOKEN_FLOOR = int(os.environ.get("ATOMCODE_TOKEN_FLOOR", "32768"))
# 是否极端地把 max_tokens 整个删掉（默认关闭：删掉会让服务端默认预算截断正文）。
DROP_MAX_TOKENS = os.environ.get("ATOMCODE_DROP_MAX_TOKENS", "0") in ("1", "true", "yes")

# 温度注入：客户端没传 temperature 时补一个默认值（传了绝不动）。
# 依据：glm/deepseek 系思考模型的"复读环"对采样温度极敏感 —— 温度越低
# 越容易在长上下文里退化成周期性重复（2026-10-18 实测 glm5.3-flash 反复重复）。
# 0.8 是折中：够打断贪心循环，又不至于让代码输出太飘。设 0 = 关闭注入。
DEFAULT_TEMPERATURE = float(os.environ.get("ATOMCODE_TEMPERATURE", "0.8"))

# 复读熔断：正文/思考尾部出现 >= LOOP_COPIES 段完全相同的周期（长度
# LOOP_PERIOD_MIN..LOOP_PERIOD_MAX），判定为退化复读环 => 立刻切断并收尾
# （保留已输出 + incomplete reason=loop_detected），不再陪它循环到挂死。
# 代价：切断前已流出的 1~2 个重复段收不回（约 <=1000 字）。
# 防误触设计：周期==1 不判 —— "===== 分隔线 / 哈哈哈 / 省略号"是排版不是病；
# 且要求连续 5 份完全相同，正常输出里已足够罕见，真实退化会连几十份直到挂死。
LOOP_CUT = os.environ.get("ATOMCODE_LOOP_CUT", "1") not in ("0", "false", "off")
LOOP_PERIOD_MIN = 12
LOOP_PERIOD_MAX = 200
LOOP_COPIES = 5
LOOP_CHECK_CHARS = 96          # 每积累这么多新字符才扫一次（省 CPU）

# 上游 /v1/models 只列 AI Hub 那批，CodingPlan 模型不在里面却能调用 → 做并集。
CODINGPLAN_MODEL_FALLBACK = [
    {"id": "glm5.3-flash", "context_window": 512000, "supports_vision": True},
    {"id": "qwen3.8-27b", "context_window": 262144, "supports_vision": True},
]

# 上游拿 200 伪装失败的话术。
MASKED_ERRORS = ("请求负载过高", "参数错误", "模型不存在", "rate limit", "overloaded",
                 "为 AtomCode 独享", "仅允许 AtomCode 环境", "三方请求失败",
                 "不支持的平台或代理配置缺失",
                 # AI Hub 月度额度打满时上游也拿 200 + 这句话当正文返回
                 # （2026-10-18 实测：B 池返回「每月token额度已不足」）
                 "额度已不足", "额度不足",
                  # 2026-10-19 实测新增：A 池按模型的配额打满时上游同样拿 200 +
                  # 这句话当正文 —— 而 status-v2 的 5h 窗口计数器此刻仍是
                  # 0/200、exhausted=false，说明窗口之外还有一道按模型的闸。
                  "用量窗口内已达上限", "窗口内已达上限")

HOP = {"host", "connection", "content-length", "transfer-encoding", "authorization",
       "x-api-key", "accept-encoding", "keep-alive", "proxy-authorization"}


# ------------------------------------------------------------------ 调用台账
class Metrics:
    """每次调用记一行 JSONL（stats.jsonl），并留内存索引给网页控制台。

    记录字段：时间/模型/协议/状态/总耗时/首字耗时/token(prompt,completion,
    reasoning,cached)/续写轮数。缓存命中来自上游 usage 的
    prompt_tokens_details.cached_tokens（实测存在：重复前缀命中 320/372）。
    """

    def __init__(self, path):
        self.path = path
        self.calls = []
        self.lock = threading.Lock()
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    for ln in fh:
                        try:
                            self.calls.append(json.loads(ln))
                        except Exception:
                            pass
                if len(self.calls) > 20000:
                    self.calls = self.calls[-20000:]
        except Exception:
            self.calls = []

    def record(self, rec):
        with self.lock:
            self.calls.append(rec)
            if len(self.calls) > 20000:
                self.calls = self.calls[-10000:]
            try:
                with open(self.path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            except Exception:
                pass

    @staticmethod
    def _sum(rows):
        s = {"calls": 0, "ok": 0, "prompt": 0, "completion": 0, "total": 0,
             "reasoning": 0, "cached": 0, "cache_calls": 0, "tok_s_sum": 0.0,
             "tok_s_n": 0}
        for r in rows:
            s["calls"] += 1
            if r.get("status") == 200:
                s["ok"] += 1
            for k in ("prompt", "completion", "total", "reasoning", "cached"):
                s[k] += int(r.get(k) or 0)
            if int(r.get("cached") or 0) > 0:
                s["cache_calls"] += 1
            if r.get("tok_s"):
                s["tok_s_sum"] += float(r["tok_s"])
                s["tok_s_n"] += 1
        s["tok_s_avg"] = round(s["tok_s_sum"] / s["tok_s_n"], 1) if s["tok_s_n"] else None
        return s

    def stats(self, days, frm=None, to=None):
        now = time.time()
        rows = []
        for r in self.calls:
            t = r.get("t", 0)
            if frm is not None:
                if frm <= t < to:
                    rows.append(r)
            elif now - t <= days * 86400:
                rows.append(r)
        return self._sum(rows)

    def recent(self, n):
        with self.lock:
            return list(self.calls[-n:])


def _usage_extract(obj):
    """从 chat 完成体 / usage 字典里抠出统一字段。"""
    u = obj.get("usage") if isinstance(obj, dict) else None
    if not isinstance(u, dict):
        u = obj if isinstance(obj, dict) else {}
    if not (u.get("prompt_tokens") or u.get("completion_tokens")):
        return None
    ptl = u.get("prompt_tokens_details") or {}
    ctd = u.get("completion_tokens_details") or {}
    return {"prompt": int(u.get("prompt_tokens") or 0),
            "completion": int(u.get("completion_tokens") or 0),
            "total": int(u.get("total_tokens") or 0),
            "reasoning": int(ctd.get("reasoning_tokens") or 0),
            "cached": int(ptl.get("cached_tokens") or 0)}


def _usage_from_raw(raw):
    try:
        return _usage_extract(json.loads(raw.decode("utf-8", "replace")))
    except Exception:
        return None


def _usage_from_lines(lines):
    """扫描一批 SSE 行，返回 (usage, 正文字符数)。usage 取最后出现的。
    兼容 chat chunk 与 responses 事件（output_text.delta）两种格式。"""
    usage, out_chars = None, 0
    for ln in lines:
        try:
            s = ln.decode("utf-8", "replace") if isinstance(ln, bytes) else ln
        except Exception:
            continue
        t = s.strip()
        if not t.startswith("data:"):
            continue
        p = t[5:].strip()
        if not p or p == "[DONE]":
            continue
        try:
            ev = json.loads(p)
        except Exception:
            continue
        if ev.get("type") == "response.output_text.delta":
            out_chars += len(ev.get("delta") or "")
        for ch in ev.get("choices") or []:
            d = ch.get("delta") or {}
            c = d.get("content") or ch.get("message", {}).get("content")
            if isinstance(c, str):
                out_chars += len(c)
        if ev.get("usage"):
            u = _usage_extract(ev)
            if u:
                usage = u
    return usage, out_chars


# ------------------------------------------------------------------ auth.toml
def _parse_auth_toml(path):
    data = {"user": {}}
    section = None
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1]
                data.setdefault(section, {})
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
                v = v[1:-1]
            (data if section is None else data[section])[k] = v
    return data


class TokenStore:
    """读 auth.toml；快过期就 POST /oauth/refresh 轮换并写回。

    refresh_token 是一次性的，刷新后必须落盘，否则会把本机 AtomCode 一起踢下线。
    """

    def __init__(self, auth_path=DEFAULT_AUTH, persist=True):
        self.path, self.persist = auth_path, persist
        self.lock = threading.Lock()
        self.at = self.rt = None
        self.created, self.expires = 0, 604800
        self.user = {}
        self.load()

    def load(self):
        d = _parse_auth_toml(self.path)
        self.at, self.rt = d.get("access_token"), d.get("refresh_token")
        self.created = int(d.get("created_at", 0) or 0)
        self.expires = int(d.get("expires_in", 604800) or 604800)
        self.user = d.get("user", {})
        return self

    def _expiring(self, slack=300):
        return (time.time() - self.created) > (self.expires - slack)

    def _refresh(self):
        req = urllib.request.Request(
            ACS + "/oauth/refresh",
            data=json.dumps({"refresh_token": self.rt}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            p = json.loads(resp.read().decode())
        if "access_token" not in p:
            raise RuntimeError("refresh failed: %s" % p)
        self.at = p["access_token"]
        self.rt = p.get("refresh_token", self.rt)
        self.created = int(time.time())
        self.expires = int(p.get("expires_in", 604800))
        if p.get("user"):
            self.user = p["user"]
        if self.persist:
            self._save()
        return self.at

    def _save(self):
        u = self.user or {}
        text = ('access_token = "%s"\nrefresh_token = "%s"\ntoken_type = "Bearer"\n'
                "expires_in = %d\ncreated_at = %d\n\n[user]\n"
                'id = "%s"\nusername = "%s"\nname = "%s"\nemail = "%s"\n'
                'avatar_url = "%s"\n'
                % (self.at, self.rt or "", self.expires, self.created,
                   u.get("id", ""), u.get("username", ""), u.get("name", ""),
                   u.get("email", ""), u.get("avatar_url", "")))
        tmp = self.path + ".proxy.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(tmp, self.path)

    def get(self):
        with self.lock:
            if self.at and not self._expiring():
                return self.at
            try:
                return self._refresh()
            except Exception as exc:
                sys.stderr.write("[token] refresh failed: %s\n" % exc)
                if self.at:
                    return self.at
                raise


# ---------------------------------------------------------------- 错误识别
def _chat_event_is_error(ev):
    for ch in ev.get("choices") or []:
        msg = dict(ch.get("message") or {})
        msg.update(ch.get("delta") or {})
        c = msg.get("content")
        if isinstance(c, str) and any(k in c for k in MASKED_ERRORS):
            return True
    return False


def _masked_error(payload):
    try:
        obj = json.loads(payload.decode("utf-8", "replace"))
    except Exception:
        return False
    if not isinstance(obj, dict):
        return False
    if _chat_event_is_error(obj):
        return True
    if obj.get("type") == "error":
        return True
    if obj.get("object") in ("response", "response.delta"):
        for item in obj.get("output") or []:
            if item.get("finishReason") in ("error", "no_choices"):
                return True
    return False


# ------------------------------------------------------------ 输出预算治理
def _scrub_budget(body, floor, drop):
    """把输出预算抬到足够大。

    实测（直连上游，同一道长文题）：
        不传 max_tokens -> 服务端默认预算很小，正文写满就 fin=length 截断
        mt=32768        -> glm 正文8125字/思考9129 fin=stop；DS 正文8120 fin=stop
        mt=2048         -> glm 思考就吃光，正文 0 字

    所以既不能删（默认预算会截断），也不能留小值（思考吃光正文）。
    做法：统一抬到 floor（默认 32768）。
    """
    if not body:
        return body
    try:
        obj = json.loads(body.decode("utf-8"))
    except Exception:
        return body
    if not isinstance(obj, dict):
        return body
    changed = False
    if drop:
        for k in ("max_tokens", "max_completion_tokens"):
            if obj.pop(k, None) is not None:
                changed = True
    else:
        cur = obj.get("max_tokens")
        cur2 = obj.get("max_completion_tokens")
        effective = max([v for v in (cur, cur2) if isinstance(v, int)] or [0])
        if effective < floor:
            if "max_completion_tokens" in obj:
                obj["max_completion_tokens"] = floor
            if "max_tokens" in obj or "max_completion_tokens" not in obj:
                obj["max_tokens"] = floor
            changed = True
    # 温度注入只在客户端没传时发生（显式传了 0 也是"传了"，尊重）。
    if DEFAULT_TEMPERATURE and obj.get("temperature") is None:
        obj["temperature"] = DEFAULT_TEMPERATURE
        changed = True
    if not changed:
        return body
    return json.dumps(obj, separators=(",", ":")).encode()


# ------------------------------------------------------- Responses 协议桥接
def _responses_to_chat(req):
    inp = req.get("input")
    if isinstance(inp, str):
        messages = [{"role": "user", "content": inp}]
    else:
        messages = []
        for item in inp or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "function_call_output":
                messages.append({"role": "tool",
                                 "tool_call_id": item.get("call_id", ""),
                                 "content": item.get("output") or ""})
                continue
            content = item.get("content")
            if isinstance(content, list):
                parts = []
                for c in content:
                    if not isinstance(c, dict):
                        continue
                    t = c.get("text")
                    if t is None and isinstance(c.get("content"), str):
                        t = c["content"]
                    if t:
                        parts.append(t)
                content = "\n".join(parts)
            if content:
                messages.append({"role": item.get("role", "user"), "content": content})

    chat = {"model": req.get("model"), "messages": messages,
            "stream": bool(req.get("stream", True))}
    for src in ("temperature", "top_p", "tools", "tool_choice"):
        if req.get(src) is not None:
            chat[src] = req[src]
    # 与 _scrub_budget 同一策略：没传才注入，传了绝不动。
    if DEFAULT_TEMPERATURE and chat.get("temperature") is None:
        chat["temperature"] = DEFAULT_TEMPERATURE
    r = req.get("reasoning")
    effort = r.get("effort") if isinstance(r, dict) else (r if isinstance(r, str) else None)
    if effort in ("minimal", "low", "medium", "high"):
        chat["reasoning_effort"] = "low" if effort == "minimal" else effort
    # 不擅自注入小预算；调用方显式给了才透传。
    if req.get("max_output_tokens"):
        chat["max_tokens"] = int(req["max_output_tokens"])
    return chat


def _sse(obj):
    return "event: %s\ndata: %s\n\n" % (obj["type"], json.dumps(obj, separators=(",", ":")))


def _iter_sse_dicts(body):
    for block in body.replace(b"\r\n", b"\n").split(b"\n\n"):
        for line in block.split(b"\n"):
            if not line.startswith(b"data:"):
                continue
            t = line[5:].strip()
            if not t or t == b"[DONE]":
                continue
            try:
                yield json.loads(t.decode("utf-8"))
            except Exception:
                pass


def _overlap(tail, head, cap=40):
    """tail 的后缀与 head 的前缀最长重叠（续写接缝去重用）。"""
    m = min(len(tail), len(head), cap)
    for k in range(m, 0, -1):
        if tail[-k:] == head[:k]:
            return k
    return 0


def _trailing_repeat(win):
    """win 尾部是否存在 >=LOOP_COPIES 段完全相同的周期（周期 LOOP_PERIOD_MIN..MAX）。"""
    hi = min(LOOP_PERIOD_MAX, len(win) // LOOP_COPIES)
    for p in range(LOOP_PERIOD_MIN, hi + 1):
        unit = win[-p:]
        if win[-p * LOOP_COPIES:] == unit * LOOP_COPIES:
            return True
    return False


class _LoopGuard:
    """复读熔断器：feed() 增量文本，检测到退化复读环返回 True（一次即够）。

    只在尾部窗口上找"连续 N 份相同周期"，每积累 LOOP_CHECK_CHARS 个新字符
    才扫一次；心跳的 U+200B 周期=1，落在 LOOP_PERIOD_MIN 之下，不会误触。
    """

    def __init__(self):
        self.buf = []
        self.size = 0
        self.since = 0

    def feed(self, text):
        if not LOOP_CUT or not text:
            return False
        self.buf.append(text)
        self.size += len(text)
        self.since += len(text)
        window = LOOP_PERIOD_MAX * LOOP_COPIES + 8
        if self.since < LOOP_CHECK_CHARS or self.size < LOOP_PERIOD_MIN * LOOP_COPIES:
            if self.size > 2 * window:      # 滚动窗口，不攒全文
                tail = "".join(self.buf)[-window:]
                self.buf = [tail]
                self.size = len(tail)
            return False
        self.since = 0
        if self.size > 2 * window:
            tail = "".join(self.buf)[-window:]
            self.buf = [tail]
            self.size = len(tail)
        return _trailing_repeat("".join(self.buf)[-window:])


def _chat_chunks_to_responses_events(chunks, model, resp_id):
    """把 chat/completions 的 chunk 流合成 Responses 事件序列（含终止事件）。"""
    created = int(time.time())
    yield _sse({"type": "response.created",
                "response": {"id": resp_id, "object": "response", "model": model,
                             "created_at": created, "status": "in_progress",
                             "output": []}})
    yield _sse({"type": "response.in_progress",
                "response": {"id": resp_id, "status": "in_progress"}})

    text_id = "msg_" + resp_id[5:]
    reason_id = "rs_" + resp_id[5:]
    text_buf, text_open, text_index = [], False, 0
    reason_open, reason_index = False, 0
    fn_items, order, finish, usage = {}, [], None, {}

    for ch in chunks:
        if ch.get("usage"):
            usage = ch["usage"]
        for choice in ch.get("choices") or []:
            if choice.get("finish_reason"):
                finish = choice["finish_reason"]
            delta = choice.get("delta") or {}

            # 思考内容必须作为真正的 reasoning 事件转发出去。
            # 客户端（pi-ai）只认 output_item.added(type=reasoning) 建出来的槽 +
            # reasoning_text.delta / reasoning_summary_text.delta 填充；
            # 若把它丢掉，重度思考模型在思考期间一个 chunk 都不产出，
            # 客户端的 idle watchdog（单次 .next() 上限 300s）就会判超时 ——
            # 表现就是"莫名中断 / idle timeout after 300000ms"。
            think = delta.get("reasoning_content")
            if think:
                if not reason_open:
                    reason_open = True
                    reason_index = len(order)
                    order.append(reason_id)
                    yield _sse({"type": "response.output_item.added",
                                "output_index": reason_index,
                                "item": {"id": reason_id, "type": "reasoning",
                                         "summary": [], "status": "in_progress"}})
                yield _sse({"type": "response.reasoning_text.delta",
                            "item_id": reason_id,
                            "output_index": reason_index,
                            "content_index": 0, "delta": think})

            piece = delta.get("content")
            if piece:
                if not text_open:
                    text_open = True
                    text_index = len(order)
                    order.append(text_id)
                    yield _sse({"type": "response.output_item.added",
                                "output_index": text_index,
                                "item": {"id": text_id, "type": "message",
                                         "role": "assistant", "content": [],
                                         "status": "in_progress"}})
                    yield _sse({"type": "response.content_part.added",
                                "item_id": text_id, "output_index": text_index,
                                "content_index": 0,
                                "part": {"type": "output_text", "text": ""}})
                text_buf.append(piece)
                yield _sse({"type": "response.output_text.delta", "item_id": text_id,
                            "output_index": text_index, "content_index": 0,
                            "delta": piece})
            for tc in delta.get("tool_calls") or []:
                idx = tc.get("index", 0)
                fn = tc.get("function") or {}
                if idx not in fn_items and fn.get("name"):
                    info = {"item_id": "fc_" + os.urandom(8).hex(), "name": fn["name"],
                            "args": [], "call_id": tc.get("id") or "",
                            "output_index": len(order)}
                    fn_items[idx] = info
                    order.append(info["item_id"])
                    yield _sse({"type": "response.output_item.added",
                                "output_index": info["output_index"],
                                "item": {"id": info["item_id"], "type": "function_call",
                                         "name": info["name"], "arguments": "",
                                         "status": "in_progress"}})
                info = fn_items.get(idx)
                if info:
                    if tc.get("id"):
                        info["call_id"] = tc["id"]
                    if fn.get("arguments"):
                        info["args"].append(fn["arguments"])
                        yield _sse({"type": "response.function_call_arguments.delta",
                                    "item_id": info["item_id"],
                                    "output_index": info["output_index"],
                                    "delta": fn["arguments"]})

    if reason_open:
        yield _sse({"type": "response.output_item.done",
                    "output_index": reason_index,
                    "item": {"id": reason_id, "type": "reasoning",
                             "summary": [], "status": "completed"}})
    if text_open:
        full = "".join(text_buf)
        yield _sse({"type": "response.output_text.done", "item_id": text_id,
                    "output_index": text_index, "content_index": 0, "text": full})
        yield _sse({"type": "response.content_part.done", "item_id": text_id,
                    "output_index": text_index, "content_index": 0,
                    "part": {"type": "output_text", "text": full}})
        yield _sse({"type": "response.output_item.done", "output_index": text_index,
                    "item": {"id": text_id, "type": "message", "role": "assistant",
                             "status": "completed",
                             "content": [{"type": "output_text", "text": full,
                                          "annotations": []}]}})
    for info in fn_items.values():
        yield _sse({"type": "response.output_item.done",
                    "output_index": info["output_index"],
                    "item": {"id": info["item_id"], "type": "function_call",
                             "name": info["name"], "call_id": info["call_id"],
                             "arguments": "".join(info["args"]), "status": "completed"}})

    status = "incomplete" if finish == "length" else "completed"
    yield _sse({"type": "response.completed" if status == "completed"
                else "response.incomplete",
                "response": {"id": resp_id, "object": "response", "model": model,
                             "created_at": created, "status": status, "output": [],
                             "incomplete_details": ({"reason": "max_output_tokens"}
                                                     if status == "incomplete" else None),
                             "usage": ({"input_tokens": usage.get("prompt_tokens", 0),
                                        "output_tokens": usage.get("completion_tokens", 0),
                                        "total_tokens": usage.get("total_tokens", 0)}
                                       if usage else None)}})
    yield "data: [DONE]\n\n"


# --------------------------------------------------------------------- 服务

def _quiet_close(resp):
    """在后台关闭上游响应，绝不阻塞调用线程。

    urllib 的 HTTPResponse.close() 会去拿 buffered reader 的锁；此时 feed
    线程正阻塞在 resp.readline() 上、正握着那把锁 —— 同步 close() 会一直
    等下去，主线程就被钉死，终止事件一个字节也发不出去。
    """
    try:
        resp.close()
    except Exception:
        pass


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    store = None
    token_floor = TOKEN_FLOOR
    drop_max = DROP_MAX_TOKENS
    _client_gone = False
    metrics = None          # main() 里挂 Metrics 实例

    def _tally(self, ctx, status, usage=None, chars=0, continues=0):
        """记一笔调用台账。ctx={t0,t1,fb,model,proto}；绝不抛错影响主流程。"""
        try:
            if self.metrics is None or not ctx:
                return
            now = time.time()
            dur = round(max(ctx.get("t1") or now, ctx.get("t0", now)) - ctx.get("t0", now), 2)
            u = usage or {}
            comp = int(u.get("completion") or 0)
            est = int(chars / 1.8) if not comp and chars else 0
            tok_s = None
            secs = max(dur - (max(0.0, (ctx.get("fb") or ctx.get("t0", now)) - ctx.get("t0", now))), 0.1) if ctx.get("fb") else max(dur, 0.1)
            n = comp or est
            if n and dur > 0:
                tok_s = round(n / secs, 1)
            self.metrics.record({
                "t": round(ctx.get("t0", now), 2), "model": ctx.get("model"),
                "proto": ctx.get("proto"), "status": status, "dur": dur,
                "ttft": round((ctx["fb"] - ctx["t0"]), 2) if ctx.get("fb") else None,
                "prompt": u.get("prompt", 0), "completion": comp,
                "total": u.get("total", 0), "reasoning": u.get("reasoning", 0),
                "cached": u.get("cached", 0), "est_out": est,
                "cont": continues, "tok_s": tok_s})
        except Exception:
            pass

    def log_message(self, fmt, *args):
        sys.stderr.write("[proxy] %s - %s\n" % (self.address_string(), fmt % args))

    # ---- 基础
    def _read_body(self):
        n = int(self.headers.get("content-length") or 0)
        if not n:
            return b""
        buf, left = bytearray(), n
        while left > 0:
            chunk = self.rfile.read(min(left, 65536))
            if not chunk:
                break
            buf += chunk
            left -= len(chunk)
        return bytes(buf)

    def _send_json(self, status, payload, ctype="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _chunk(self, data):
        # 客户端可能已断开（它自己的 idle timeout 到点、切模型、手动取消）。
        # 这里必须自己吞掉，否则每次断开都往上抛 traceback；ThreadingHTTPServer
        # 虽不会因此退出，但日志会淹没真正的故障信号。
        try:
            self.wfile.write(b"%X\r\n%s\r\n" % (len(data), data))
            self.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            self._client_gone = True
            return False

    def _open(self, method, path, body, stream):
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        # UA 决定要不要强制签名：先按大小写洗掉所有变体（避免重复头），
        # 自称 atomcode 的一律换成中性值。
        ua = ""
        for k in [k for k in headers if k.lower() == "user-agent"]:
            ua = ua or headers.pop(k)
        if not ua or "atomcode" in ua.lower():
            ua = NEUTRAL_UA
        headers["User-Agent"] = ua
        headers["Authorization"] = "Bearer " + self.store.get()
        headers["Accept-Encoding"] = "identity"
        headers["Accept"] = "text/event-stream" if stream else "application/json"
        req = urllib.request.Request(UPSTREAM + path, data=body or None,
                                     headers=headers, method=method)
        return urllib.request.urlopen(req, timeout=600)

    # ---- 透传
    def _forward(self, method, path, body):
        sub = path.split("?", 1)[0].rstrip("/")
        stream = sub.endswith(("/chat/completions", "/messages", "/completions"))
        if method == "POST" and sub.endswith(("/chat/completions", "/completions")):
            body = _scrub_budget(body, self.token_floor, self.drop_max)
        elif method == "POST" and sub.endswith("/messages"):
            # Anthropic 协议 max_tokens 必填：只能抬，不能删
            body = _scrub_budget(body, self.token_floor, False)
        # 台账上下文
        ctx = {"t0": time.time(), "model": None,
               "proto": ("anthropic" if sub.endswith("/messages")
                         else "chat"), "fb": None, "t1": None}
        try:
            if body:
                ctx["model"] = json.loads(body.decode("utf-8", "replace")).get("model")
        except Exception:
            pass
        try:
            resp = self._open(method, path, body, stream)
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            self._send_json(exc.code, payload)
            ctx["t1"] = time.time()
            self._tally(ctx, exc.code)
            return
        except Exception as exc:
            self.send_error(502, "upstream: %s" % exc)
            ctx["t1"] = time.time()
            self._tally(ctx, 502)
            return

        status = getattr(resp, "status", 200)
        ctype = resp.headers.get("Content-Type") or ""
        try:
            if "event-stream" in ctype:
                def reopen():
                    # 只在"尚无可见内容送达客户端"时被调用 —— 见 _passthrough_sse。
                    r = self._open(method, path, body, True)
                    c = r.headers.get("Content-Type") or ""
                    if "event-stream" not in c:
                        raw = r.read()
                        threading.Thread(target=_quiet_close, args=(r,),
                                         daemon=True).start()
                        raise RuntimeError("reopen not a stream: %s" % raw[:80])
                    return r

                lines = self._passthrough_sse(resp, reopen, ctx)
                usage, chars = _usage_from_lines(lines)
                ctx["t1"] = time.time()
                self._tally(ctx, ctx.get("status", 200), usage, chars)
                return
            raw = resp.read()
            ctx["t1"] = time.time()
            if status == 200 and _masked_error(raw):
                self._send_json(503, raw)   # 上游拿 200 伪装的失败
                self._tally(ctx, 503)
                return
            self.send_response(status)
            if ctype:
                self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            usage = _usage_from_raw(raw) if status == 200 else None
            self._tally(ctx, status, usage)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass  # 客户端主动断流
        finally:
            # 同步 close() 会和阻塞在 readline() 的线程抢同一把锁 -> 主线程钉死
            threading.Thread(target=_quiet_close, args=(resp,), daemon=True).start()

    def _passthrough_sse(self, resp, reopen=None, ctx=None):
        """逐行转发上游 SSE；带「死流透明重开」与「补发 [DONE]」。返回转过的
        数据行（给台账统计 usage/输出量）。

        三条规矩都来自官方 provider 实现（atomcode-capabilities）：
          1. retry.rs:5   "Metadata alone is replay-safe"
          2. openai_compat.rs:820 "No replay-sensitive output reached the
             consumer yet -> re-open the whole request transparently"
             重开前必须退避，否则把正在过载的网关再锤一次。
          3. openai_compat.rs:794 网关在最后一个事件之后、[DONE] 之前关掉
             keep-alive，要当**干净结束**，否则一整轮已经生成完的回复会被判
             成失败（官方注释说这曾杀掉跑满 20 分钟的一轮）。

        重开只发生在"一个字节都还没送给客户端"时（opened is False）——
        那时重发整个请求不会产生任何重复内容。

        读法用 reader 线程 + 队列，不用 socket 超时：readline() 走
        BufferedReader，中途超时会让缓冲区留下半行状态，重开时读到垃圾。
        """
        attempt = 0
        seen_lines = []
        while True:
            attempt += 1
            opened, eof, saw_finish = self._pump_sse_once(resp, attempt, ctx, seen_lines)
            if eof in ("client", "ok"):
                return seen_lines
            if (eof == "dead" and not opened and reopen
                    and attempt < MAX_STREAM_ATTEMPTS):
                sys.stderr.write("[reopen] 第 %d 次：上游在首个可见事件前静默，重发整个请求\n"
                                 % (attempt + 1))
                sys.stderr.flush()
                threading.Thread(target=_quiet_close, args=(resp,), daemon=True).start()
                time.sleep(RETRY_BACKOFF_S * attempt)
                try:
                    resp = reopen()
                    continue
                except Exception as exc:
                    sys.stderr.write("[reopen] 失败: %s\n" % exc)
                    sys.stderr.flush()
                    break
            if eof == "dead" and not opened:
                # 从没建立过流：明确报错，绝不回一个静默的空 200
                self._send_json(502, json.dumps({"error": {
                    "message": "上游在首个事件前静默 %.0fs（已试 %d 次）"
                               % (FIRST_EVENT_S, attempt),
                    "type": "upstream_stalled"}}).encode())
                return seen_lines
            if eof == "dead" and opened and saw_finish:
                # 规矩 3 的另一半：已 finish_reason 但流死了 -> 补信标收尾
                self._chunk(b"data: [DONE]\n\n")
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass
                return seen_lines
            if eof == "dead" and opened:
                # 最危险的一格：流已经开、又没有 finish_reason，重发不安全。
                # 但**必须关闭 chunked 传输**，否则客户端会永远等下去
                # —— 那正是我们一直在修的"卡住不结束"。
                sys.stderr.write("[stall] 流中途静默 %.0fs，正常收尾保留已输出\n" % STALL_S)
                sys.stderr.flush()
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass
                return seen_lines
            return seen_lines

    def _pump_sse_once(self, resp, attempt, ctx=None, seen=None):
        """转发一段上游流。返回 (是否已开流, 结束原因, 是否见过 finish_reason)。

        结束原因: "ok" 正常收尾 | "dead" 静默或读失败 | "client" 客户端断开
        转过的数据行 append 进 seen（台账统计用）；首个可见内容时刻写 ctx["fb"]。
        """
        q = queue.Queue()
        stop = threading.Event()

        def reader():
            try:
                while not stop.is_set():
                    line = resp.readline()
                    if not line:
                        break
                    q.put(line)
            except Exception as exc:
                q.put(exc)
            finally:
                q.put(None)

        threading.Thread(target=reader, daemon=True).start()

        buf, verdict, opened = b"", None, False
        saw_done = saw_finish = False
        pump_guard = _LoopGuard()   # 透传路复读熔断（chat SSE 上盯 content/思考增量）

        def feed_guard(ev):
            """把事件里的可见增量喂给熔断器；命中复读环返回 True。"""
            for ch in ev.get("choices") or []:
                d = ch.get("delta") or {}
                if pump_guard.feed(d.get("content") or ""):
                    return True
                if pump_guard.feed(d.get("reasoning_content") or ""):
                    return True
            return False

        def open_stream():
            nonlocal opened
            if opened:
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            opened = True
            if ctx is not None and ctx.get("fb") is None:
                ctx["fb"] = time.time()   # 首字节（= 首个可见内容）时刻

        def note(data_line):
            if seen is not None:
                seen.append(data_line)

        # 没开流 => 给"首个可见事件"较短预算（超了就能安全重开）
        # 已开流 => 用 STALL_S，此时重发不安全，只能收尾
        def wait_for():
            return FIRST_EVENT_S if not opened else STALL_S

        while True:
            if getattr(self, "_client_gone", False):
                stop.set()
                return opened, "client", saw_finish
            try:
                item = q.get(timeout=wait_for())
            except queue.Empty:
                stop.set()
                return opened, "dead", saw_finish
            if item is None:
                break
            if isinstance(item, Exception):
                stop.set()
                return opened, "dead", saw_finish
            line = item
            if verdict is False:
                if self._event_finish(line):
                    saw_finish = True
                if not self._chunk(line):
                    stop.set()
                    return True, "client", saw_finish
                note(line)
                # 复读熔断（已开流阶段）：转发的同时盯内容。命中 => 切断并
                # 关 chunked 传输（不伪造 finish/[DONE]，与挂死收尾同一规矩）。
                st0 = line.strip()
                if st0.startswith(b"data:") and st0[5:].strip() != b"[DONE]":
                    try:
                        gev = json.loads(st0[5:].strip().decode("utf-8"))
                    except Exception:
                        gev = None
                    if gev is not None and feed_guard(gev):
                        sys.stderr.write("[loop] 透传路复读环，切断收尾\n")
                        sys.stderr.flush()
                        stop.set()
                        try:
                            self.wfile.write(b"0\r\n\r\n")
                            self.wfile.flush()
                        except Exception:
                            pass
                        return True, "ok", saw_finish
                continue
            buf += line
            if len(buf) > 65536:
                verdict = False          # 认不出内容也别把流憋死
                open_stream()
                if not self._chunk(buf):
                    stop.set()
                    return True, "client", saw_finish
                note(buf)
                buf = b""
                continue
            st = line.strip()
            if not st.startswith(b"data:"):
                continue
            payload = st[5:].strip()
            if payload == b"[DONE]":
                verdict = False
                saw_done = True
                open_stream()
                self._chunk(buf)
                note(buf)
                buf = b""
                break
            try:
                ev = json.loads(payload.decode("utf-8"))
            except Exception:
                continue
            if _chat_event_is_error(ev):
                if opened:
                    # 已经开流，状态码改不了了：丢掉这条伪错误直接收尾，
                    # 绝不把它当正文写给客户端。
                    verdict = False
                    buf = b""
                    break
                # 上游拿 200 伪装的失败（负载过高/参数错误/独享）：
                # 还没开流时可以直接改状态码，绝不能当正文漏给客户端。
                verdict = json.dumps(ev).encode()
                break
            if self._event_visible(ev):
                # 首个"可见内容"才提交 200 并冲刷缓冲。role-only / usage
                # 之前的元数据继续攒着 —— 官方 retry.rs:5："Metadata alone
                # is replay-safe"，上游卡在元数据之后时我们才能安全重开。
                verdict = False
                if self._event_finish(line):
                    saw_finish = True
                open_stream()
                if not self._chunk(buf):
                    stop.set()
                    return True, "client", saw_finish
                note(buf)
                buf = b""
                # 这个事件本身也要进熔断窗口（后续行走 verdict is False 分支）
                if feed_guard(ev):
                    sys.stderr.write("[loop] 透传路复读环，切断收尾\n")
                    sys.stderr.flush()
                    stop.set()
                    try:
                        self.wfile.write(b"0\r\n\r\n")
                        self.wfile.flush()
                    except Exception:
                        pass
                    return True, "ok", saw_finish

        if isinstance(verdict, bytes):
            # 上游拿 200 伪装的失败：尚未开流时可以直接改状态码
            self._send_json(503, verdict)
            if ctx is not None:
                ctx["status"] = 503
            return opened, "ok", saw_finish

        open_stream()
        if buf:
            self._chunk(buf)
            note(buf)
        # 规矩 3：上游关了连接却没发 [DONE]，但已经 finish_reason
        #         => 补一个 [DONE]，让客户端把这轮当正常结束。
        if not saw_done and saw_finish:
            self._chunk(b"data: [DONE]\n\n")
        try:
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return True, "client", saw_finish
        return True, "ok", saw_finish

    @staticmethod
    def _event_visible(ev):
        """这个事件是否已经把「可见内容」交给客户端？

        role-only、空 delta 只算元数据。区分二者的意义：元数据阶段死掉可以
        整条重开（用户无感），内容阶段死掉绝不能重发（会重复内容）。
        """
        for ch in ev.get("choices") or []:
            d = ch.get("delta") or {}
            if d.get("content") or d.get("reasoning_content") or d.get("tool_calls"):
                return True
            if ch.get("finish_reason"):
                return True
            m = ch.get("message") or {}
            if m.get("content") or m.get("reasoning_content"):
                return True
        return False

    @staticmethod
    def _event_finish(line):
        """精确判断这一行是否携带**非空** finish_reason。

        不能用 b'"finish_reason"' in line —— 每个中间 delta 都带
        "finish_reason": null，会把没写完的流误判成已完成、提前补发 [DONE]。
        """
        st = line.strip()
        if not st.startswith(b"data:"):
            return False
        payload = st[5:].strip()
        if not payload or payload == b"[DONE]":
            return False
        try:
            ev = json.loads(payload.decode("utf-8"))
        except Exception:
            return False
        for ch in ev.get("choices") or []:
            if ch.get("finish_reason"):
                return True
        return False

    # ---- Responses 桥接
    def _responses_bridge(self, body):
        try:
            req = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            self.send_error(400, "bad json")
            return
        chat = _responses_to_chat(req)
        # 宿主（DSH 等）走的是这条 Responses 路，它传的 max_output_tokens 往往
        # 偏小；重度思考模型会把预算全烧在思考上 -> 正文永远出不来 -> idle
        # timeout。所以这里也按同一个策略治理。
        if self.drop_max:
            chat.pop("max_tokens", None)
        else:
            chat["max_tokens"] = max(int(chat.get("max_tokens") or 0), self.token_floor)
        want_stream = bool(chat["stream"])
        resp_id = "resp_" + os.urandom(16).hex()
        payload = json.dumps(chat, separators=(",", ":")).encode()
        ctx = {"t0": time.time(), "model": chat.get("model"), "proto": "responses",
               "fb": None, "t1": None}
        try:
            up = self._open("POST", "/chat/completions", payload, want_stream)
        except urllib.error.HTTPError as exc:
            self._send_json(exc.code, exc.read())
            ctx["t1"] = time.time()
            self._tally(ctx, exc.code)
            return
        except Exception as exc:
            self.send_error(502, "upstream: %s" % exc)
            ctx["t1"] = time.time()
            self._tally(ctx, 502)
            return
        try:
            if want_stream:
                self._responses_stream(up, chat.get("model"), resp_id, chat, ctx)
            else:
                raw = up.read()
                ctx["t1"] = time.time()
                self._responses_plain(raw, chat.get("model"), resp_id)
                self._tally(ctx, 503 if _masked_error(raw) else 200,
                            _usage_from_raw(raw))
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            # 同样不能同步 close：上游若还卡在 readline，这里会等锁等到线程死。
            threading.Thread(target=_quiet_close, args=(up,), daemon=True).start()

    def _upstream_events(self, up):
        """逐行增量切 SSE。上游偶尔无视 stream 返回整包 JSON，那时只产一条。"""
        buf, saw_data = b"", False
        while True:
            try:
                line = up.readline()
            except Exception:
                break
            if not line:
                break
            buf += line
            if not saw_data and b"data:" in buf:
                saw_data = True
            if saw_data:
                st = buf.strip()
                buf = b""
                if not st.startswith(b"data:"):
                    continue
                t = st[5:].strip()
                if not t or t == b"[DONE]":
                    continue
                try:
                    yield json.loads(t.decode("utf-8"))
                except Exception:
                    pass
            elif len(buf) > 262144:
                try:
                    yield json.loads(buf.decode("utf-8"))
                    buf, saw_data = b"", True
                except Exception:
                    pass
        if not saw_data and buf.strip():
            try:
                yield json.loads(buf.decode("utf-8"))
            except Exception:
                pass

    def _responses_stream(self, up, model, resp_id, chat=None, ctx=None):
        gen = self._upstream_events(up)
        first = next(gen, None)
        if first is not None and _chat_event_is_error(first):
            self._send_json(503, json.dumps(first).encode())
            if ctx is not None:
                ctx["t1"] = time.time()
                self._tally(ctx, 503)
            return

        def chained():
            if first is not None:
                yield first
            for ev in gen:
                yield ev

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        # 上游「思考十几分钟不出字」时，代理必须主动续命。
        #
        # 客户端的 idle watchdog（dsh-timeout/src/index.ts:149）语义是：
        #     next(iterator) { arm(); return await iterator.next() }
        # 即**必须真的拿到一个 chunk** 才重置，单次 next() 超过 300s 就判超时。
        # 注意 SSE 注释行（": keepalive"）被解析器直接丢弃、不产生 chunk，
        # 靠它续命无效 —— 必须发真实事件。
        #
        # 这里用队列 + 超时：上游沉默超过 KEEPALIVE_S 就补一个合成思考增量。
        stop = threading.Event()

        # 续命必须让**生成器自己**产出「建槽 + delta」这对事件。
        #
        # 客户端 pi-ai 的规则（openai-responses-shared.js:329）：
        #     getSlot(i, type) → slot?.type === type ? slot : undefined
        #     if (!slot) continue;        // 槽不存在，事件被静默丢弃
        # 而槽只有 response.output_item.added(item.type==="reasoning") 才建。
        # 所以不能在外面直接塞 reasoning_text.delta —— 上游没产出过思考时
        # 槽根本不存在，那种心跳等于没发（这正是"没看到思考跳字"时的情形）。
        #
        # 做法：把合成事件推进**输入队列**，让生成器走正常路径自己建槽。
        up_q = queue.Queue()      # 上游 chat 事件
        out_q = queue.Queue()     # 已转换好的 Responses 事件
        stop = threading.Event()
        last_real = [time.time()]  # 最后一个**真实**上游事件的时刻
        usage_box = [None]         # 上游 chat 事件里的 usage（feed 线程顺手抠）
        cont_box = [0]             # 已发生的自动续写轮数（台账用）

        def feed():
            try:
                for ev in chained():
                    if stop.is_set():
                        return
                    if ev.get("usage"):
                        u = _usage_extract(ev)
                        if u:
                            usage_box[0] = u
                    up_q.put(ev)
            except Exception as exc:
                up_q.put(exc)
            finally:
                up_q.put(None)

        def upstream_iter():
            while True:
                ev = up_q.get()
                if ev is None:
                    return
                if isinstance(ev, Exception):
                    raise ev
                yield ev

        def emit():
            try:
                for out in _chat_chunks_to_responses_events(
                        upstream_iter(), model, resp_id):
                    if stop.is_set():
                        return
                    out_q.put(out)
            except Exception as exc:
                out_q.put(exc)
            finally:
                out_q.put(None)

        threading.Thread(target=feed, daemon=True).start()
        threading.Thread(target=emit, daemon=True).start()

        # 心跳的 delta 必须是 U+200B：与 classify() 的指纹判据一致。
        # 用普通空格会和"模型真的吐了一个空格"混淆，导致挂死计时被错误清零。
        SYNTH = {"choices": [{"delta": {"reasoning_content": "\u200b"}}]}

        # 心跳有个致命副作用，必须用 STALL_S 兜住：
        # 客户端 watchdog 是"每次 next() 拿到 chunk 就重置"，所以持续心跳
        # 等于**永久续命** —— 上游若真的挂死，客户端就永远不报错也不结束
        # （这正是"吐了一点突然卡住、没有结束也没有吐字"的形态）。
        # 连续 KEEPALIVE 攒够 STALL_S 都没等到上游真实事件 => 判定挂死，
        # 由我们自己发终止事件收尾：保留已有正文 + response.incomplete。
        text_id = "msg_" + resp_id[5:]
        text_buf, text_index = [], 0
        reason_buf = []       # 已输出的思考正文（续写要带着，否则模型从头再想）
        stalled = False
        truncated = False     # 上游 fin=length（写满预算）
        saw_terminal = False
        tool_seen = False
        looped = False
        guard = _LoopGuard()
        ka_slot = [None, None]   # reasoning 槽 [item_id, output_index]，续写复用

        def classify(raw):
            """返回 "ka"（自己注入的心跳）/ ("delta", dict) / (type, dict)。

            心跳的 delta 是 U+200B：UI 不可见，且真实模型不会只吐一个孤立
            零宽字符，可以当精确指纹用。
            """
            try:
                ev = json.loads(raw.decode("utf-8").split("data:", 1)[1].strip())
            except Exception:
                return None
            if ev.get("type") == "response.reasoning_text.delta" \
                    and ev.get("delta") == "\u200b":
                return "ka"
            return ev.get("type"), ev

        try:
            while True:
                try:
                    item = out_q.get(timeout=KEEPALIVE_S)
                except queue.Empty:
                    if stop.is_set():
                        break
                    stale = time.time() - last_real[0]
                    # 心跳一旦流回 out_q，get 就不再超时，所以"超时分支"单独
                    # 判挂死是不够的 —— 真实事件到来时 last_real 会被刷新。
                    sys.stderr.write("[stream] 静默 %.1fs (stall 阈值 %.0fs)\n"
                                     % (stale, STALL_S))
                    sys.stderr.flush()
                    if stale >= STALL_S:
                        stalled = True
                        break
                    up_q.put(SYNTH)
                    continue
                if item is None:
                    break
                if isinstance(item, Exception):
                    # 上游异常不往外抛：交给下面的兜底收尾，保证有终止事件
                    sys.stderr.write("[emit-err] %s\n" % item)
                    break
                raw = item.encode("utf-8") if isinstance(item, str) else item
                got = classify(raw)
                if got == "ka":
                    # 心跳不算上游活动、不计入正文；但它建的槽要记住（续写复用）
                    try:
                        kev = json.loads(
                            raw.decode("utf-8").split("data:", 1)[1].strip())
                        if ka_slot[0] is None:
                            ka_slot[0] = kev.get("item_id")
                            ka_slot[1] = kev.get("output_index")
                    except Exception:
                        pass
                    if not self._chunk(raw):
                        break
                    continue
                if isinstance(got, tuple):
                    ty, ev = got
                    if ty == "response.output_text.delta":
                        d = ev.get("delta") or ""
                        text_buf.append(d)
                        text_index = ev.get("output_index", text_index)
                        text_id = ev.get("item_id") or text_id
                        if guard.feed(d):
                            sys.stderr.write("[loop] 检测到复读环，熔断切断\n")
                            sys.stderr.flush()
                            looped = True
                            break
                    if ty == "response.reasoning_text.delta":
                        # 思考通道的复读同样烧预算，一并盯着
                        rd = ev.get("delta") or ""
                        if guard.feed(rd):
                            sys.stderr.write("[loop] 思考复读环，熔断切断\n")
                            sys.stderr.flush()
                            looped = True
                            break
                        if rd and rd.strip("\u200b"):
                            reason_buf.append(rd)
                        if ka_slot[0] is None:
                            ka_slot[0] = ev.get("item_id")
                            ka_slot[1] = ev.get("output_index")
                    if ty == "response.function_call_arguments.delta":
                        tool_seen = True   # 工具参数拼一半，续写会拼出坏 JSON
                    if ty in ("response.incomplete", "response.failed"):
                        det = ((ev.get("response") or {})
                               .get("incomplete_details") or {})
                        # 上游 fin=length => 桥翻成 incomplete(max_output_tokens)。
                        # 这不是错误是"写满预算"：官方客户端对此的动作是续写，我们
                        # 压掉终止事件和 [DONE]，交给下面的续写接管收尾。条件不符
                        # （无正文/工具在拼/关了续写）才原样透传。
                        if (ty == "response.incomplete"
                                and det.get("reason") == "max_output_tokens"
                                and chat and text_buf and not tool_seen
                                and MAX_CONTINUES > 0 and not looped):
                            truncated = True
                            break
                        saw_terminal = True
                    elif ty == "response.completed":
                        saw_terminal = True
                    last_real[0] = time.time()   # 真实事件：解除挂死计时
                if not self._chunk(raw):
                    break

            # 正常收尾：写 chunked 终止块。缺了它连接永不关闭，
            # 客户端要等到自己超时才结束（实测 curl 卡满 200s）。
            if saw_terminal:
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass

            # ---- 挂死/写满预算后自动续写（对齐官方 "continue from preserved
            # safe progress"；官方实测 without loss / the recovery that always
            # worked）。门槛：有正文可保、没有工具调用在拼、chat 可重放。
            # truncated=上游 fin=length（终止事件已在上面压掉）；
            # stalled=流中途静默挂死。两者都走同一条续写路。
            if ((stalled or truncated) and not saw_terminal and not tool_seen
                    and chat and text_buf and MAX_CONTINUES > 0):
                st = {"text_buf": text_buf, "text_id": text_id,
                      "text_index": text_index, "ka_slot": ka_slot,
                      "reason_buf": reason_buf,
                      "looped": False, "truncated": False}
                stop.set()   # 停掉原桥接线程，之后由续写直接写
                for i in range(1, MAX_CONTINUES + 1):
                    cont_box[0] = i
                    sys.stderr.write("[cont] 第 %d/%d 次自动续写（已保留 %d 字）\n"
                                     % (i, MAX_CONTINUES, len("".join(text_buf))))
                    sys.stderr.flush()
                    try:
                        ok = self._continue_stream(chat, model, resp_id, i, st,
                                                   up if i == 1 else None)
                    except Exception as exc:
                        sys.stderr.write("[cont%d] 异常(弃): %r\n" % (i, exc))
                        sys.stderr.flush()
                        ok = False
                    if ok:
                        saw_terminal = True
                        break
                    if st.get("looped"):
                        looped = True    # 续写轮熔断 => 收尾 reason 用 loop_detected
                        break
                    if st.get("truncated"):
                        # 续写轮也写满了预算：收尾如实报 max_output_tokens
                        truncated = True

            if not saw_terminal:
                # 判据只有"有没有发过终止事件"：续写成功时 saw_terminal 已置真，
                # 绝不能再补一份（此前 "stalled or not saw_terminal" 会在成功后
                # 再发一遍 incomplete，白写 200 多字节，靠客户端先收了 [DONE]
                # 才没造成双重终止事件 —— 是运气不是设计）。
                # 兜底：任何退出路径（上游正常 EOF、异常、挂死）只要没发过
                # 终止事件，就必须自己补一个。流"无声结束"会让客户端
                # 永远挂着 —— 既不报错也不结束，这就是你看到的现象。
                reason = "loop_detected" if looped else (
                    "max_output_tokens" if truncated else (
                        "stalled" if stalled else "upstream_ended"))
                sys.stderr.write("[tail] 补发终止事件 reason=%s\n" % reason)
                sys.stderr.flush()
                stop.set()
                # 不能同步 up.close()：feed 线程正阻塞在 up.readline() 上并握着
                # 那把锁，close() 会一直等锁 -> 主线程钉死 -> 终止事件发不出去。
                # 交给后台线程关，主线程立刻继续写终止事件。
                threading.Thread(target=_quiet_close, args=(up,), daemon=True).start()
                full = "".join(text_buf)
                tail = []
                if full:
                    tail.append(_sse({"type": "response.output_text.done",
                                      "item_id": text_id, "output_index": text_index,
                                      "content_index": 0, "text": full}))
                    tail.append(_sse({"type": "response.content_part.done",
                                      "item_id": text_id, "output_index": text_index,
                                      "content_index": 0,
                                      "part": {"type": "output_text", "text": full}}))
                    tail.append(_sse({"type": "response.output_item.done",
                                      "output_index": text_index,
                                      "item": {"id": text_id, "type": "message",
                                               "role": "assistant", "status": "completed",
                                               "content": [{"type": "output_text",
                                                            "text": full,
                                                            "annotations": []}]}}))
                tail.append(_sse({"type": "response.incomplete",
                                  "response": {"id": resp_id, "object": "response",
                                               "model": model, "status": "incomplete",
                                               "incomplete_details": {"reason": reason},
                                               "output": []}}))
                tail.append("data: [DONE]\n\n")
                wrote = 0
                try:
                    for ev in tail:
                        blob = ev.encode("utf-8") if isinstance(ev, str) else ev
                        if not self._chunk(blob):
                            break
                        wrote += len(blob)
                except Exception as exc:
                    sys.stderr.write("[tail] 写出异常: %r\n" % (exc,))
                sys.stderr.write("[tail] 已写出 %d 字节\n" % wrote)
                sys.stderr.flush()
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except Exception:
                    pass
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            # 客户端主动断开（超时/切模型/取消），属正常现象，不要打 traceback。
            pass
        finally:
            stop.set()
            if ctx is not None:
                # 台账：status 用"是否发出终止事件"区分 200/499(客户端断)；
                # usage 优先上游回传，缺失时按正文字符估算输出量。
                u = usage_box[0]
                chars = len("".join(text_buf))
                if u is None and chars:
                    u = {"prompt": 0, "completion": 0, "total": 0,
                         "reasoning": 0, "cached": 0}
                self._tally(ctx, 200 if saw_terminal else 499, u, chars,
                            continues=cont_box[0])


    def _continue_stream(self, chat, model, resp_id, n, st, up_prev):
        """第 n 次自动续写。返回 True 表示续写流正常 finish 并已收尾。

        只在「流已开、有正文、无工具调用在拼」时进入（调用方判）。做法：
        把已输出正文作为 assistant 前缀 + 一句"继续"重新请求，新流接到
        **同一条客户端流后面**（复用原 msg 项），接缝做后缀/前缀重叠裁剪。
        """
        if up_prev is not None:
            threading.Thread(target=_quiet_close, args=(up_prev,),
                             daemon=True).start()
        prefix = "".join(st["text_buf"])
        reason_prefix = "".join(st.get("reason_buf") or [])
        chat2 = dict(chat)
        asst = {"role": "assistant", "content": prefix}
        # 把已输出的思考一并回传（DS/GLM 的 chat 接口认 reasoning_content），
        # 续写才能"接着想"而不是"从头再想一遍"——不带的后果是模型看到自己
        # 写到一半的正文却不知道为什么这么写，思考里出现"我上轮说过要…"
        # 这类脱节独白（2026-10-19 实测 glm5.3-flash 续写）。
        # 截尾 8000 字：思考链太长时保留最近的收尾部分最有续写价值。
        if reason_prefix:
            asst["reasoning_content"] = reason_prefix[-8000:]
        msgs = list(chat2.get("messages") or []) + [
            asst,
            {"role": "user",
             "content": "继续。从你上面未输出完的地方接着写，"
                        "不要重复任何已经输出的内容。"},
        ]
        chat2["messages"] = msgs
        try:
            body2 = json.dumps(chat2, separators=(",", ":")).encode("utf-8")
            up2 = self._open("POST", "/chat/completions", body2, True)
        except Exception as exc:
            sys.stderr.write("[cont%d] 重开失败: %s\n" % (n, str(exc)[:80]))
            sys.stderr.flush()
            return False

        q = queue.Queue()

        def rd():
            try:
                for ev in self._upstream_events(up2):
                    q.put(ev)
            except Exception as exc:
                q.put(exc)
            finally:
                q.put(None)

        threading.Thread(target=rd, daemon=True).start()

        tid, tix = st["text_id"], st["text_index"]
        slot = list(st["ka_slot"])
        tail40 = prefix[-40:]
        seam, buffering = "", True
        saw_finish = False
        cguard = _LoopGuard()     # 续写轮专用熔断（前缀带进来的旧环不再算）
        aborted = False
        last = time.time()
        deadline = time.time() + CONTINUE_WAIT_S

        def emit(obj):
            return self._chunk(_sse(obj).encode("utf-8"))

        def text_delta(out):
            return emit({"type": "response.output_text.delta", "item_id": tid,
                         "output_index": tix, "content_index": 0, "delta": out})

        def think_delta(txt, is_new_slot=False):
            if slot[0] is None or len(slot) < 2 or slot[1] is None:
                slot[:] = [None, None]        # 半坏状态 => 当作没有槽
            if slot[0] is None:
                slot[0], slot[1] = ("rs_c%d" % n, 900 + n)
                if not emit({"type": "response.output_item.added",
                             "output_index": slot[1],
                             "item": {"id": slot[0], "type": "reasoning",
                                      "summary": [], "status": "in_progress"}}):
                    return False
            return emit({"type": "response.reasoning_text.delta",
                         "item_id": slot[0], "output_index": slot[1],
                         "content_index": 0, "delta": txt})

        try:
            while time.time() < deadline:
                try:
                    ev = q.get(timeout=KEEPALIVE_S)
                except queue.Empty:
                    if time.time() - last >= STALL_AGAIN_S:
                        break                     # 本轮又静默死了
                    if not think_delta("\u200b"):
                        return False              # 客户端已断
                    continue
                if ev is None or isinstance(ev, Exception):
                    break
                if _chat_event_is_error(ev):
                    break                         # 伪错误，弃本轮，绝不拼进正文
                for ch in ev.get("choices") or []:
                    fr = ch.get("finish_reason")
                    if fr == "length":
                        # 续写这轮又写满预算：不算成功，交下一轮接着写；
                        # 耗尽后由主循环按 max_output_tokens 如实收尾。
                        sys.stderr.write("[cont%d] 本轮仍写满预算(length)\n" % n)
                        sys.stderr.flush()
                        st["truncated"] = True
                        saw_finish = False
                        break
                    if fr:
                        saw_finish = True
                    d = ch.get("delta") or {}
                    if d.get("tool_calls"):
                        sys.stderr.write("[cont%d] 出现工具调用，停止续写\n" % n)
                        sys.stderr.flush()
                        return False
                    rc = d.get("reasoning_content")
                    if rc:
                        last = time.time()
                        # 本轮新思考续进缓冲区，下一轮续写接得上
                        st.setdefault("reason_buf", []).append(rc)
                        if not think_delta(rc):
                            return False
                        if cguard.feed(rc):
                            sys.stderr.write("[cont%d] 续写轮复读环，熔断\n" % n)
                            sys.stderr.flush()
                            aborted = True
                            break
                    pc = d.get("content")
                    if pc:
                        last = time.time()
                        if buffering:
                            seam += pc
                            if len(seam) >= len(tail40) \
                                    or not tail40.startswith(seam):
                                k = _overlap(tail40, seam)
                                out = seam[k:]
                                seam = ""
                                buffering = False
                                if out:
                                    st["text_buf"].append(out)
                                    if not text_delta(out):
                                        return False
                                    if cguard.feed(out):
                                        sys.stderr.write(
                                            "[cont%d] 续写轮复读环，熔断\n" % n)
                                        sys.stderr.flush()
                                        aborted = True
                                        break
                        else:
                            st["text_buf"].append(pc)
                            if not text_delta(pc):
                                return False
                            if cguard.feed(pc):
                                sys.stderr.write("[cont%d] 续写轮复读环，熔断\n" % n)
                                sys.stderr.flush()
                                aborted = True
                                break
                if aborted or saw_finish:
                    break
        finally:
            threading.Thread(target=_quiet_close, args=(up2,), daemon=True).start()

        if buffering and seam:
            k = _overlap(tail40, seam)
            out = seam[k:]
            if out:
                st["text_buf"].append(out)
                if not text_delta(out):
                    return False
        if aborted:
            # 复读熔断：再续一轮也只会再复读一遍，直接交主循环收尾。
            st["looped"] = True
            return False
        if not saw_finish:
            sys.stderr.write("[cont%d] 本轮未正常结束，交下一轮或收尾\n" % n)
            sys.stderr.flush()
            return False

        full = "".join(st["text_buf"])
        close = [
            {"type": "response.output_text.done", "item_id": tid,
             "output_index": tix, "content_index": 0, "text": full},
            {"type": "response.content_part.done", "item_id": tid,
             "output_index": tix, "content_index": 0,
             "part": {"type": "output_text", "text": full}},
            {"type": "response.output_item.done", "output_index": tix,
             "item": {"id": tid, "type": "message", "role": "assistant",
                      "status": "completed",
                      "content": [{"type": "output_text", "text": full,
                                   "annotations": []}]}},
            {"type": "response.completed",
             "response": {"id": resp_id, "object": "response", "model": model,
                          "status": "completed", "output": []}},
        ]
        for obj in close:
            if not emit(obj):
                return True      # 内容已完成，客户端断了就断了
        self._chunk(b"data: [DONE]\n\n")
        try:
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except Exception:
            pass
        sys.stderr.write("[cont%d] 续写完成，总正文 %d 字\n" % (n, len(full)))
        sys.stderr.flush()
        return True

    def _responses_plain(self, raw, model, resp_id):
        if _masked_error(raw):
            self._send_json(503, raw)
            return
        try:
            obj = json.loads(raw.decode("utf-8"))
        except Exception:
            self.send_error(502, "bad upstream json")
            return
        text, calls = "", []
        for choice in obj.get("choices") or []:
            msg = choice.get("message") or {}
            text += msg.get("content") or ""
            for tc in msg.get("tool_calls") or []:
                calls.append({"type": "function_call", "status": "completed",
                              "call_id": tc.get("id") or "",
                              "name": (tc.get("function") or {}).get("name", ""),
                              "arguments": (tc.get("function") or {}).get("arguments", "")})
        out = []
        if text:
            out.append({"type": "message", "id": "msg_" + resp_id[5:],
                        "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": text,
                                     "annotations": []}]})
        out += calls
        usage = obj.get("usage") or {}
        doc = json.dumps({"id": resp_id, "object": "response",
                          "model": obj.get("model") or model,
                          "created_at": int(time.time()), "status": "completed",
                          "output": out,
                          "usage": ({"input_tokens": usage.get("prompt_tokens", 0),
                                     "output_tokens": usage.get("completion_tokens", 0),
                                     "total_tokens": usage.get("total_tokens", 0)}
                                    if usage else None)}).encode()
        self._send_json(200, doc)

    # ---- 路由
    def do_GET(self):
        only = self.path.split("?", 1)[0].rstrip("/")
        if only in ("", "/console"):
            self._send_json(200, CONSOLE_HTML.encode("utf-8"),
                            ctype="text/html; charset=utf-8")
            return
        if only == "/stats":
            self._stats()
            return
        if only in ("/health", "/healthz"):
            self._send_json(200, json.dumps({
                "ok": True, "upstream": UPSTREAM,
                "user": self.store.user.get("username"),
                "drop_max_tokens": self.drop_max,
                "token_floor": self.token_floor,
                "temperature_default": DEFAULT_TEMPERATURE or None,
                "loop_cut": LOOP_CUT,
                "continues": MAX_CONTINUES}).encode())
            return
        if only.endswith("/models"):
            self._models()
            return
        if self.path.startswith("/v1"):
            self._forward("GET", self.path[3:], b"")
            return
        self.send_error(404)

    def do_POST(self):
        if not self.path.startswith("/v1"):
            self.send_error(404)
            return
        body = self._read_body()
        only = self.path[3:].split("?", 1)[0].rstrip("/")
        if only == "/responses":
            self._responses_bridge(body)
            return
        self._forward("POST", self.path[3:], body)

    def _models(self):
        """并集：上游 /v1/models + CodingPlan models-v2。"""
        seen, data = set(), []
        try:
            req = urllib.request.Request(
                UPSTREAM + "/models",
                headers={"Authorization": "Bearer " + self.store.get(),
                         "User-Agent": NEUTRAL_UA})
            with urllib.request.urlopen(req, timeout=30) as resp:
                for m in json.loads(resp.read().decode()).get("data", []):
                    if m.get("id") and m["id"] not in seen:
                        seen.add(m["id"])
                        data.append(m)
        except Exception as exc:
            sys.stderr.write("[models] upstream list failed: %s\n" % exc)
        try:
            req = urllib.request.Request(
                CODINGPLAN_API + "/models-v2?plan_type=Lite",
                headers={"Authorization": "Bearer " + self.store.get()})
            with urllib.request.urlopen(req, timeout=30) as resp:
                for m in json.loads(resp.read().decode()):
                    name = m.get("display_model_name")
                    if name and name not in seen:
                        seen.add(name)
                        data.append({"id": name, "object": "model", "created": 0,
                                     "owned_by": "codingplan",
                                     "context_window": m.get("context_window"),
                                     "supports_vision": m.get("supports_vision")})
        except Exception as exc:
            sys.stderr.write("[models] codingplan list failed: %s\n" % exc)
            for m in CODINGPLAN_MODEL_FALLBACK:
                if m["id"] not in seen:
                    seen.add(m["id"])
                    data.append({"id": m["id"], "object": "model", "created": 0,
                                 "owned_by": "codingplan-fallback",
                                 "context_window": m["context_window"],
                                 "supports_vision": m["supports_vision"]})
        self._send_json(200, json.dumps({"object": "list", "data": data}).encode())

    def _stats(self):
        """/stats?days=7 或 /stats?from=..&to=..（epoch 秒）。返回汇总+最近明细。"""
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(self.path).query)
        try:
            days = int((q.get("days") or ["1"])[0])
        except Exception:
            days = 1
        frm = to = None
        try:
            if "from" in q and "to" in q:
                frm, to = float(q["from"][0]), float(q["to"][0])
        except Exception:
            frm = to = None
        if self.metrics is None:
            self._send_json(200, b'{"error":"metrics off"}')
            return
        with self.metrics.lock:
            calls = list(self.metrics.calls)
        if frm is not None:
            rows = [r for r in calls if frm <= r.get("t", 0) < to]
        else:
            cutoff = time.time() - days * 86400
            rows = [r for r in calls if r.get("t", 0) >= cutoff]
        # 按模型分组
        by_model = {}
        for r in rows:
            m = by_model.setdefault(r.get("model") or "?", {
                "calls": 0, "ok": 0, "prompt": 0, "completion": 0, "total": 0,
                "reasoning": 0, "cached": 0})
            m["calls"] += 1
            if r.get("status") == 200:
                m["ok"] += 1
            for k in ("prompt", "completion", "total", "reasoning", "cached"):
                m[k] += int(r.get(k) or 0)
        # 按天分桶（本地时区）
        by_day = {}
        for r in rows:
            d = time.strftime("%Y-%m-%d", time.localtime(r.get("t", 0)))
            b = by_day.setdefault(d, {"calls": 0, "total": 0, "completion": 0,
                                      "cached": 0, "tok_s_sum": 0.0, "tok_s_n": 0})
            b["calls"] += 1
            b["total"] += int(r.get("total") or 0)
            b["completion"] += int(r.get("completion") or 0)
            b["cached"] += int(r.get("cached") or 0)
            if r.get("tok_s"):
                b["tok_s_sum"] += r["tok_s"]
                b["tok_s_n"] += 1
        for b in by_day.values():
            b["tok_s_avg"] = round(b["tok_s_sum"] / b["tok_s_n"], 1) if b["tok_s_n"] else None
            del b["tok_s_sum"], b["tok_s_n"]
        self._send_json(200, json.dumps({
            "range": {"days": days, "from": frm, "to": to},
            "total": self.metrics._sum(rows),
            "by_model": by_model,
            "by_day": dict(sorted(by_day.items())),
            "recent": sorted(calls, key=lambda r: -r.get("t", 0))[:80],
        }, ensure_ascii=False).encode("utf-8"))


CONSOLE_HTML = """<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<title>atomcode-relay 控制台</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#0f1115;--card:#171a21;--tx:#d7dce4;--mut:#8b93a3;--ok:#3fb950;--bad:#f85149;--ac:#58a6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);
font:14px/1.5 ui-monospace,Consolas,"Cascadia Mono",monospace;padding:18px}
h1{font-size:17px;margin:0 0 12px}
.tabs{display:flex;gap:6px;margin-bottom:14px;flex-wrap:wrap}
.tabs button{background:var(--card);border:1px solid #2a2f3a;color:var(--mut);
padding:6px 14px;border-radius:6px;cursor:pointer;font:inherit}
.tabs button.on{color:var(--tx);border-color:var(--ac)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:16px}
.card{background:var(--card);border:1px solid #2a2f3a;border-radius:8px;padding:12px}
.card .k{color:var(--mut);font-size:12px}.card .v{font-size:21px;margin-top:3px}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:8px;overflow:hidden}
th,td{padding:7px 10px;text-align:right;border-bottom:1px solid #22262f;white-space:nowrap}
th:first-child,td:first-child{text-align:left}
th{color:var(--mut);font-weight:500;font-size:12px}
tr:hover td{background:#1c2029}
.ok{color:var(--ok)}.bad{color:var(--bad)}.mut{color:var(--mut)}
.custom{display:none;gap:8px;margin-bottom:12px}
.custom.on{display:flex}
input[type=datetime-local]{background:var(--card);border:1px solid #2a2f3a;
color:var(--tx);padding:6px;border-radius:6px;font:inherit}
.upd{color:var(--mut);font-size:12px;margin-left:8px}
</style></head><body>
<h1>atomcode-relay 控制台<span class="upd" id="upd"></span></h1>
<div class="tabs">
<button data-r="1" class="on">今天</button>
<button data-r="7">7 天</button>
<button data-r="30">30 天</button>
<button data-r="0">自定义</button>
</div>
<div class="custom" id="custom">
<input type="datetime-local" id="from"><span class="mut">→</span>
<input type="datetime-local" id="to"><button id="go">查询</button>
</div>
<div class="cards" id="cards"></div>
<h1>按模型</h1><div style="overflow-x:auto"><table id="tm"></table></div>
<h1 style="margin-top:18px">按天</h1><div style="overflow-x:auto"><table id="td"></table></div>
<h1 style="margin-top:18px">最近调用</h1><div style="overflow-x:auto"><table id="tr"></table></div>
<script>
let range=1, timer=null;
const $=id=>document.getElementById(id);
const fmt=n=>n==null?'-':(n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':''+n);
const time=t=>new Date(t*1000).toLocaleString('zh-CN',{hour12:false});
async function load(){
  let url='/stats?days='+range;
  if(range===0){
    const f=$('from').value,t=$('to').value;
    if(!f||!t)return;
    url='/stats?from='+ (new Date(f).getTime()/1000) +'&to='+ (new Date(t).getTime()/1000);
  }
  const r=await fetch(url);const d=await r.json();
  const s=d.total||{};
  const cachePct=s.prompt? (100*s.cached/s.prompt).toFixed(1)+'%':'-';
  $('cards').innerHTML=[
   ['调用',s.calls+' (成'+s.ok+')'],['总 tokens',fmt(s.total)],
   ['输入',fmt(s.prompt)],['输出',fmt(s.completion)],
   ['思考',fmt(s.reasoning)],
   ['缓存命中',fmt(s.cached)+' / '+cachePct],
   ['平均速度',(s.tok_s_avg!=null?s.tok_s_avg+' tok/s':'-')]
  ].map(x=>'<div class="card"><div class="k">'+x[0]+'</div><div class="v">'+x[1]+'</div></div>').join('');
  let h='<tr><th>模型</th><th>调用</th><th>成</th><th>输入</th><th>输出</th><th>思考</th><th>缓存</th></tr>';
  for(const [m,v] of Object.entries(d.by_model||{}))
    h+='<tr><td>'+m+'</td><td>'+v.calls+'</td><td class="'+(v.ok===v.calls?'ok':'bad')+'">'+v.ok+'</td><td>'+fmt(v.prompt)+'</td><td>'+fmt(v.completion)+'</td><td>'+fmt(v.reasoning)+'</td><td>'+fmt(v.cached)+'</td></tr>';
  $('tm').innerHTML=h;
  h='<tr><th>日期</th><th>调用</th><th>总 tokens</th><th>输出</th><th>缓存</th><th>tok/s</th></tr>';
  for(const [k,v] of Object.entries(d.by_day||{}))
    h+='<tr><td>'+k+'</td><td>'+v.calls+'</td><td>'+fmt(v.total)+'</td><td>'+fmt(v.completion)+'</td><td>'+fmt(v.cached)+'</td><td>'+(v.tok_s_avg??'-')+'</td></tr>';
  $('td').innerHTML=h;
  h='<tr><th>时间</th><th>模型</th><th>协议</th><th>状态</th><th>首字</th><th>耗时</th><th>输入</th><th>输出</th><th>思考</th><th>缓存</th><th>tok/s</th><th>续写</th></tr>';
  for(const c of (d.recent||[]))
    h+='<tr><td>'+time(c.t)+'</td><td>'+(c.model||'-')+'</td><td class="mut">'+(c.proto||'-')+'</td><td class="'+(c.status===200?'ok':'bad')+'">'+c.status+'</td><td>'+(c.ttft??'-')+'s</td><td>'+c.dur+'s</td><td>'+fmt(c.prompt)+'</td><td>'+fmt(c.completion||c.est_out)+'</td><td>'+fmt(c.reasoning)+'</td><td>'+fmt(c.cached)+'</td><td>'+(c.tok_s??'-')+'</td><td>'+(c.cont||'')+'</td></tr>';
  $('tr').innerHTML=h;
  $('upd').textContent='更新于 '+new Date().toLocaleTimeString('zh-CN',{hour12:false});
}
document.querySelectorAll('.tabs button').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.tabs button').forEach(x=>x.classList.remove('on'));
  b.classList.add('on');range=+b.dataset.r;
  $('custom').classList.toggle('on',range===0);
  if(range!==0)load();
});
$('go').onclick=load;
load();timer=setInterval(load,5000);
</script></body></html>"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--auth", default=DEFAULT_AUTH)
    ap.add_argument("--upstream", default=os.environ.get(
        "ATOMCODE_UPSTREAM", "https://api-ai.gitcode.com/v1"))
    ap.add_argument("--token-floor", type=int, default=TOKEN_FLOOR,
                    help="输出预算下限，默认 32768（小了会被思考吃光或提前截断）")
    ap.add_argument("--drop-max-tokens", action="store_true",
                    help="极端模式：整个删掉 max_tokens。会让服务端默认预算截断正文，一般不用")
    ap.add_argument("--no-persist", action="store_true",
                    help="不把轮换后的 token 写回 auth.toml")
    args = ap.parse_args()

    global UPSTREAM
    UPSTREAM = args.upstream.rstrip("/")

    # pythonw 下 sys.stdout/stderr 是 None，print 会抛 AttributeError 直接干掉进程。
    log = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.log")
    if sys.stdout is None or sys.stderr is None:
        fh = open(log, "a", encoding="utf-8", buffering=1)
        if sys.stdout is None:
            sys.stdout = fh
        if sys.stderr is None:
            sys.stderr = fh

    store = TokenStore(args.auth, persist=not args.no_persist)
    Proxy.store = store
    Proxy.token_floor = args.token_floor
    Proxy.drop_max = args.drop_max_tokens
    Proxy.metrics = Metrics(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "stats.jsonl"))

    print("atomcode2api")
    print("  upstream : %s" % UPSTREAM)
    print("  auth     : %s (user=%s)" % (args.auth, store.user.get("username", "?")))
    print("  listen   : http://%s:%d/v1" % (args.host, args.port))
    print("  控制台   : http://%s:%d/console" % (args.host, args.port))
    print("  预算策略 : %s / 温度缺省 %s" % (
        "删掉 max_tokens(极端)" if Proxy.drop_max else "抬到 %d" % args.token_floor,
        "不注入" if not DEFAULT_TEMPERATURE else DEFAULT_TEMPERATURE))
    print("  静默策略 : 心跳 %.0fs / 挂死 %.0fs -> 自动续写 <=%d 次（轮内 %.0fs）"
          % (KEEPALIVE_S, STALL_S, MAX_CONTINUES, STALL_AGAIN_S))
    try:
        ThreadingHTTPServer((args.host, args.port), Proxy).serve_forever()
    except OSError as exc:
        print("[fatal] cannot bind %s:%d — %s（端口被占用？）" % (args.host, args.port, exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
