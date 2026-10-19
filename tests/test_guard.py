import sys, os, random
import importlib.util
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("ac2", os.path.join(_ROOT, "atomcode2api.py"))
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
g = m._LoopGuard

# 1) 真复读环：>=12字周期 x8 => 必须检出
unit = "这是一个被反复输出的句子用来验证熔断能不能识别出来"  # 24字
feed = ("开头正常内容" * 10) + unit * 8
gg = g(); hit = any(gg.feed(feed[i:i+50]) for i in range(0, len(feed), 50))
assert hit, "1 应检出复读环"

# 2) 分隔线/感叹号 周期1~2 不误触
gg = g()
noise = ("\n" + "=" * 40 + "\n") * 3 + ("哈" * 30 + "！" * 20) * 3
assert not any(gg.feed(c) for c in noise), "2 分隔线误触"

# 3) 正常变化长文不误触
random.seed(1)
gg = g()
txt = "".join(chr(random.randint(0x4e00, 0x9fff)) for _ in range(3000))
assert not any(gg.feed(txt[i:i+97]) for i in range(0, 3000, 97)), "3 随机正常文误触"

# 3b) 重复但不达 5 份（4 份相同周期）=> 不熔断（防正常排版误伤）
gg = g()
txt = unit * 4 + "收尾不同"
assert not any(gg.feed(txt[i:i+50]) for i in range(0, len(txt), 50)), "3b 4份不该熔断"

# 4) 温度注入：没传补 0.8；传了不动
b = m._scrub_budget(b'{"model":"x","messages":[]}', 32768, False)
assert b'"temperature":0.8' in b, b
assert b'"max_tokens":32768' in b, b
b2 = m._scrub_budget(b'{"model":"x","temperature":0.2,"max_tokens":99999}', 32768, False)
assert b'"temperature":0.2' in b2, b2
assert b2 == b'{"model":"x","temperature":0.2,"max_tokens":99999}', ("传了温度且预算够大应原样返回", b2)
c = m._responses_to_chat({"model": "m", "input": [{"role": "user", "content": "hi"}], "stream": True})
assert c.get("temperature") == 0.8, c
c2 = m._responses_to_chat({"model": "m", "input": "hi", "temperature": 0.3})
assert c2["temperature"] == 0.3, c2

print("PASS  LoopGuard + 温度注入 单测")
