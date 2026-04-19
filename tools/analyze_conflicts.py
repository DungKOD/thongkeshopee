"""Phân tích cross-file duplicate (level, name) gây UPSERT overwrite."""
import csv
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')
DOCS = Path(__file__).parent.parent / 'docs'


def detect_kind(headers):
    h = [x.lower().strip() for x in headers]
    if 'tên nhóm quảng cáo' in h: return 'fb_ad_group'
    if 'tên chiến dịch' in h: return 'fb_campaign'
    return None


def parse_num(v):
    if not v: return None
    s = str(v).strip().rstrip('%').replace(',', '')
    try: return float(s)
    except: return None


def is_valuable(spend, link, all_, result):
    sp = spend or 0
    cl = link or all_ or result or 0
    return sp != 0 or cl != 0


def normalize_clicks(link, all_, result):
    return link if link is not None else (all_ if all_ is not None else result)


def parse_rows(p):
    """Return list of (level, name, spend, clicks, day_date)."""
    with open(p, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    if not rows: return []
    kind = detect_kind(list(rows[0].keys()))
    if not kind: return []
    name_key = 'Tên chiến dịch' if kind == 'fb_campaign' else 'Tên nhóm quảng cáo'
    level = 'campaign' if kind == 'fb_campaign' else 'ad_group'
    out = []
    for r in rows:
        name = (r.get(name_key) or '').strip()
        rs = (r.get('Lượt bắt đầu báo cáo') or '').strip()
        if not name or not rs: continue
        day = rs[:10] if re.fullmatch(r'\d{4}-\d{2}-\d{2}', rs[:10]) else None
        if not day: continue
        spend = parse_num(r.get('Số tiền đã chi tiêu (VND)'))
        link = parse_num(r.get('Lượt click vào liên kết'))
        all_ = parse_num(r.get('Lượt click (tất cả)'))
        result = parse_num(r.get('Kết quả'))
        if not is_valuable(spend, link, all_, result): continue
        clicks = normalize_clicks(link, all_, result) or 0
        out.append((level, name, spend or 0, int(clicks), day, p.name))
    return out


# ==========================================================
# Collect all valuable rows across files, group by (day, level, name)
# ==========================================================
by_key = defaultdict(list)  # (day, level, name) → list of (file, spend, clicks)
for p in sorted(DOCS.glob('*.csv')):
    for (level, name, spend, clicks, day, filename) in parse_rows(p):
        by_key[(day, level, name)].append((filename, spend, clicks))

# Find conflicts
print('=== Cross-file / same-file conflicts trên (day, level, name) ===')
print('Những key xuất hiện >1 lần → UPSERT current đè row trước, mất data.')
print()
total_lost_spend = 0
total_lost_clicks = 0
for (day, level, name), entries in sorted(by_key.items()):
    if len(entries) > 1:
        # Sort by file name (import order) → last wins in current UPSERT
        lost = entries[:-1]  # tất cả trừ cái cuối bị đè
        kept = entries[-1]
        for (f, s, c) in lost:
            total_lost_spend += s
            total_lost_clicks += c
        print(f'  [{day}] {level}/{name}')
        for (f, s, c) in entries:
            mark = 'KEPT ' if (f, s, c) == kept else 'LOST '
            print(f'    {mark} {f[:55]:55}  spend={s:>8.0f}  clicks={c:>4}')
        print()

print(f'TỔNG DATA MẤT: spend={total_lost_spend:.0f}  clicks={total_lost_clicks}')
