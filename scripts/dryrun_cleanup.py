"""唯讀：分析 2026 台北／台中「專員收現」填寫情況（不印電話、客戶姓名）。"""
import os
import re
import sys
from collections import Counter
from datetime import datetime, date

sys.path.insert(0, os.getcwd())
from modules import auth  # noqa: E402

FILES = {"台北": ("1bNcJuFuP--jdpNo2zJKOpvuq-5rSHW3LgGE8HEepf44", "1AvoUGlFCxaKsPOPavgBmqHoyn-_t4c0I82bIth7SvRQ"),
         "台中": ("1AlsgBL7uAooiU8hb0v-02J2MdBgDVJtGHgvD3U84hCM", "1q4-S4cbSPXklNfE380_e7lrHYMaQ7Y_0ZGmgTeWfGfI")}
jgc = auth.get_jenny_gspread_client()
TIME = re.compile(r"(\d{1,2})\s*[:：]?\s*(\d{2})?\s*[-~～－到至]+\s*(\d{1,2})\s*[:：]?\s*(\d{2})?")


def d(v):
    s = str(v or "").strip()
    m = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", s)
    return date(int(m[1]), int(m[2]), int(m[3])) if m else None


for region, (fid, salary_id) in FILES.items():
    ss = jgc.open_by_key(fid)
    try:
        rows = ss.worksheet("專員收現").get_all_values()
    except Exception as e:
        print(region, "讀取失敗", e)
        continue
    hdr, data = rows[0], [r + [""] * 20 for r in rows[1:] if any(x.strip() for x in r)]
    print(f"\n===== {region} 專員收現：{len(data)} 筆；表頭 {hdr}")
    print("回報類別：", Counter(r[6] for r in data).most_common())
    adj = [r for r in data if "加減時" in r[6]]
    print(f"含加減時：{len(adj)} 筆")
    print("顧客類別加減時（J）：", Counter(re.sub(r"\d+\s*人", "N人", r[9]) for r in adj).most_common(12))
    ppl = Counter(re.search(r"(\d+)\s*人", r[9])[1] if re.search(r"(\d+)\s*人", r[9]) else "未寫人數" for r in adj)
    print("J 欄人數：", ppl.most_common())
    ok = sum(1 for r in adj if TIME.search(r[8]))
    print(f"實際起迄（I）能解析出起迄：{ok}/{len(adj)}；範例：", [r[8] for r in adj[:12]])
    delays = []
    for r in data:
        sd = d(r[3])
        st = re.match(r"(\d{4})/(\d{1,2})/(\d{1,2})", r[1])
        if sd and st:
            delays.append((date(int(st[1]), int(st[2]), int(st[3])) - sd).days)
    c = Counter("當天" if x <= 0 else "隔天" if x == 1 else "2-3天" if x <= 3 else "4-7天" if x <= 7 else "8天以上" for x in delays)
    print("提交距服務日：", c.most_common())
    print("O 欄（未命名）值：", Counter(r[14] for r in data).most_common(6))
    # 對得到訂單嗎：訂單資料 用 服務日期＋電話
    try:
        od = jgc.open_by_key(salary_id).worksheet("訂單資料").get("B1:AH40000")
        h = od[0]
        ci = {k: h.index(k) for k in ("訂單編號", "服務日期", "服務時間", "服務時數", "聯絡電話", "服務專員")}
        idx = {}
        for o in od[1:]:
            o = o + [""] * 40
            od_d = d(o[ci["服務日期"]])
            ph = re.sub(r"\D", "", o[ci["聯絡電話"]])[-9:]
            if od_d and ph:
                idx.setdefault((od_d, ph), []).append(o)
        hit = multi = 0
        staff_in = 0
        for r in adj:
            key = (d(r[3]), re.sub(r"\D", "", r[5])[-9:])
            m = idx.get(key, [])
            if m:
                hit += 1
                multi += len(m) > 1
                staff_in += any(r[2].strip() and r[2].strip() in x[ci["服務專員"]] for x in m)
        print(f"以「服務日期＋電話」對到訂單：{hit}/{len(adj)}（多筆 {multi}；專員名在該單服務專員內 {staff_in}）")
    except Exception as e:
        print("訂單資料比對失敗：", e)
