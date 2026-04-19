"""Test simulate import toàn bộ CSV docs/ vào temp DB, so sánh với CSV gốc.

Replicate đúng logic:
- JS detectKind + toFbXxxRow parser
- JS filter isFbValuable (spend=0 AND clicks=0 → skip)
- Rust normalize_clicks/cpc
- Rust UPSERT với UNIQUE(day_date, level, name)
"""
import csv
import hashlib
import os
import re
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8')

DOCS = Path(__file__).parent.parent / 'docs'
SCHEMA = Path(__file__).parent.parent / 'src-tauri' / 'src' / 'db' / 'schema.sql'


def detect_kind(headers):
    h = [x.lstrip('\ufeff').lower().strip() for x in headers]
    if 'tên nhóm quảng cáo' in h: return 'fb_ad_group'
    if 'tên chiến dịch' in h: return 'fb_campaign'
    if 'id đơn hàng' in h and any(x.startswith('sub_id2') for x in h): return 'shopee_commission'
    if 'click id' in h and 'sub_id' in h: return 'shopee_clicks'
    return 'unknown'


def parse_num(v):
    if v is None or v == '': return None
    s = str(v).strip().rstrip('%').replace(',', '')
    try: return float(s)
    except: return None


def parse_sub_ids(name):
    parts = [p.strip() for p in name.split('-')]
    out = ['', '', '', '', '']
    for i in range(min(5, len(parts))): out[i] = parts[i]
    if len(parts) > 5: out[4] = '-'.join(parts[4:])
    return out


def extract_date(s):
    if not s or len(s) < 10: return None
    head = s[:10]
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', head): return head
    return None


def is_fb_valuable(row):
    spend = row.get('spend') or 0
    clicks = row.get('link_clicks') or row.get('all_clicks') or row.get('result_count') or 0
    return spend != 0 or clicks != 0


def normalize_clicks(link, all_, result):
    for v in (link, all_, result):
        if v is not None: return int(v)
    return None


def normalize_cpc(link, all_, cost_per_result):
    for v in (link, all_, cost_per_result):
        if v is not None: return float(v)
    return None


def parse_fb_campaign(r):
    name = (r.get('Tên chiến dịch') or '').strip()
    rs = (r.get('Lượt bắt đầu báo cáo') or '').strip()
    re_ = (r.get('Lượt kết thúc báo cáo') or '').strip()
    if not name or not rs or not re_: return None
    return {
        'level': 'campaign', 'name': name, 'sub_ids': parse_sub_ids(name),
        'report_start': rs, 'report_end': re_,
        'spend': parse_num(r.get('Số tiền đã chi tiêu (VND)')),
        'impressions': parse_num(r.get('Lượt hiển thị')),
        'reach': parse_num(r.get('Người tiếp cận')),
        'link_clicks': parse_num(r.get('Lượt click vào liên kết')),
        'all_clicks': parse_num(r.get('Lượt click (tất cả)')),
        'result_count': parse_num(r.get('Kết quả')),
        'link_cpc': parse_num(r.get('CPC (chi phí trên mỗi lượt click vào liên kết) (VND)')),
        'all_cpc': parse_num(r.get('CPC (tất cả) (VND)')),
        'cost_per_result': parse_num(r.get('Chi phí trên mỗi kết quả')),
    }


def parse_shopee_order(r):
    """Parse 1 row shopee order. Return None nếu thiếu required field."""
    order_id = (r.get('ID đơn hàng') or r.get('\ufeffID đơn hàng') or '').strip()
    checkout_id = (r.get('Checkout id') or '').strip()
    item_id = (r.get('Item id') or '').strip()
    order_time = (r.get('Thời Gian Đặt Hàng') or '').strip()
    if not order_id or not checkout_id or not item_id or not order_time: return None
    return {
        'order_id': order_id,
        'checkout_id': checkout_id,
        'item_id': item_id,
        'model_id': (r.get('ID Model') or '').strip(),
        'order_time': order_time,
        'net_commission': parse_num(r.get('Hoa hồng ròng tiếp thị liên kết(₫)')),
        'commission_total': parse_num(r.get('Tổng hoa hồng sản phẩm(₫)')),
        'order_value': parse_num(r.get('Giá trị đơn hàng (₫)')),
    }


def parse_shopee_click(r):
    click_id = (r.get('Click id') or r.get('\ufeffClick id') or '').strip()
    click_time = (r.get('Thời gian Click') or '').strip()
    if not click_id or not click_time: return None
    return {'click_id': click_id, 'click_time': click_time}


def parse_fb_ad_group(r):
    name = (r.get('Tên nhóm quảng cáo') or '').strip()
    rs = (r.get('Lượt bắt đầu báo cáo') or '').strip()
    re_ = (r.get('Lượt kết thúc báo cáo') or '').strip()
    if not name or not rs or not re_: return None
    return {
        'level': 'ad_group', 'name': name, 'sub_ids': parse_sub_ids(name),
        'report_start': rs, 'report_end': re_,
        'spend': parse_num(r.get('Số tiền đã chi tiêu (VND)')),
        'impressions': parse_num(r.get('Lượt hiển thị')),
        'reach': parse_num(r.get('Người tiếp cận')),
        'link_clicks': parse_num(r.get('Lượt click vào liên kết')),
        'all_clicks': parse_num(r.get('Lượt click (tất cả)')),
        'result_count': parse_num(r.get('Kết quả')),
        'link_cpc': parse_num(r.get('CPC (chi phí trên mỗi lượt click vào liên kết) (VND)')),
        'all_cpc': parse_num(r.get('CPC (tất cả) (VND)')),
        'cost_per_result': parse_num(r.get('Chi phí trên mỗi kết quả')),
    }


def load_csv(path):
    """Return (kind, day_date, rows_raw_unfiltered, rows_after_filter)."""
    with open(path, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)
    if not raw_rows: return ('empty', None, [], [])
    kind = detect_kind(list(raw_rows[0].keys()))

    if kind == 'fb_campaign':
        parser, get_date = parse_fb_campaign, lambda r: extract_date(r['report_start'])
    elif kind == 'fb_ad_group':
        parser, get_date = parse_fb_ad_group, lambda r: extract_date(r['report_start'])
    elif kind == 'shopee_commission':
        parser, get_date = parse_shopee_order, lambda r: extract_date(r['order_time'])
    elif kind == 'shopee_clicks':
        parser, get_date = parse_shopee_click, lambda r: extract_date(r['click_time'])
    else:
        return (kind, None, raw_rows, raw_rows)

    parsed = [parser(r) for r in raw_rows]
    parsed = [r for r in parsed if r is not None]
    dates = set(get_date(r) for r in parsed)
    day_date = list(dates)[0] if len(dates) == 1 else None

    if kind in ('fb_campaign', 'fb_ad_group'):
        valuable = [r for r in parsed if is_fb_valuable(r)]
    elif kind == 'shopee_commission':
        valuable = dedup_shopee_orders(parsed)
    else:
        valuable = parsed
    return (kind, day_date, parsed, valuable)


def dedup_shopee_orders(rows):
    """Shopee export có duplicate (checkout_id, item_id, model_id) — 1 row data
    thật + N rows dummy (comm=0). UPSERT `DO UPDATE` đè mất → giữ row net MAX.
    """
    by_key = {}
    for r in rows:
        key = (r['checkout_id'], r['item_id'], r['model_id'])
        ex = by_key.get(key)
        cur = r['net_commission'] or 0
        if ex is None or cur > (ex['net_commission'] or 0):
            by_key[key] = r
    return list(by_key.values())


def init_db():
    """Create in-memory DB với schema from schema.sql."""
    conn = sqlite3.connect(':memory:')
    conn.executescript(SCHEMA.read_text(encoding='utf-8'))
    return conn


def insert_fb_rows(conn, rows, day_date, source_file_id):
    """Mimic FB_ADS_UPSERT_SQL."""
    sql = """
        INSERT INTO raw_fb_ads
        (level, name, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5,
         report_start, report_end, status,
         spend, clicks, cpc, impressions, reach,
         raw_json, day_date, source_file_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(day_date, level, name) DO UPDATE SET
           sub_id1 = excluded.sub_id1, sub_id2 = excluded.sub_id2,
           sub_id3 = excluded.sub_id3, sub_id4 = excluded.sub_id4,
           sub_id5 = excluded.sub_id5,
           report_start = excluded.report_start, report_end = excluded.report_end,
           status = excluded.status,
           spend = excluded.spend, clicks = excluded.clicks, cpc = excluded.cpc,
           impressions = excluded.impressions, reach = excluded.reach,
           raw_json = excluded.raw_json, source_file_id = excluded.source_file_id
    """
    inserted = 0
    for r in rows:
        clicks = normalize_clicks(r['link_clicks'], r['all_clicks'], r['result_count'])
        cpc = normalize_cpc(r['link_cpc'], r['all_cpc'], r['cost_per_result'])
        conn.execute(sql, (
            r['level'], r['name'],
            *r['sub_ids'], r['report_start'], r['report_end'], None,
            r['spend'], clicks, cpc, r['impressions'], r['reach'],
            None, day_date, source_file_id,
        ))
        inserted += 1
    return inserted


def register_day_file(conn, kind, filename, day_date, file_id, row_count):
    conn.execute("INSERT OR IGNORE INTO days(date, created_at) VALUES(?, datetime('now'))", (day_date,))
    conn.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, row_count, file_hash, day_date) "
        "VALUES(?, ?, ?, datetime('now'), ?, ?, ?)",
        (file_id, filename, kind, row_count, f'hash{file_id}', day_date)
    )


def insert_shopee_orders(conn, rows, day_date, source_file_id):
    """Mimic import_shopee_orders UPSERT."""
    sql = """
        INSERT INTO raw_shopee_order_items
        (order_id, checkout_id, item_id, model_id, order_time,
         order_value, net_commission, commission_total,
         day_date, source_file_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(checkout_id, item_id, model_id) DO UPDATE SET
           order_time = excluded.order_time,
           order_value = excluded.order_value,
           net_commission = excluded.net_commission,
           commission_total = excluded.commission_total,
           source_file_id = excluded.source_file_id
    """
    for r in rows:
        conn.execute(sql, (
            r['order_id'], r['checkout_id'], r['item_id'], r['model_id'],
            r['order_time'], r['order_value'], r['net_commission'],
            r['commission_total'], day_date, source_file_id,
        ))


def insert_shopee_clicks(conn, rows, day_date, source_file_id):
    sql = """
        INSERT OR IGNORE INTO raw_shopee_clicks
        (click_id, click_time, day_date, source_file_id)
        VALUES (?, ?, ?, ?)
    """
    for r in rows:
        conn.execute(sql, (r['click_id'], r['click_time'], day_date, source_file_id))


# =============================================================
# MAIN
# =============================================================

print('=' * 70)
print('STAGE 1: Import simulation từ docs/')
print('=' * 70)

fb_files = []
shopee_order_files = []
shopee_click_files = []
seen_hashes = set()  # Mimic imported_files.file_hash UNIQUE
for p in sorted(DOCS.glob('*.csv')):
    content = p.read_bytes()
    h = hashlib.sha256(content).hexdigest()
    if h in seen_hashes:
        print(f'  [SKIP hash dup] {p.name}')
        continue
    seen_hashes.add(h)
    kind, day, raw, valuable = load_csv(p)
    if kind in ('fb_campaign', 'fb_ad_group'):
        fb_files.append((p, kind, day, raw, valuable))
    elif kind == 'shopee_commission':
        shopee_order_files.append((p, day, raw, valuable))
    elif kind == 'shopee_clicks':
        shopee_click_files.append((p, day, raw, valuable))

# Group theo ngày
by_day = defaultdict(list)
for item in fb_files:
    by_day[item[2]].append(item)

print(f'\nTìm thấy {len(fb_files)} file FB trong docs/')
print(f'Nhóm theo ngày: {dict((d, len(v)) for d, v in sorted(by_day.items()))}')

# Init DB, import
conn = init_db()
file_id = 0
for day in sorted(by_day):
    print(f'\n--- FB ngày {day} ---')
    for (p, kind, _, raw, valuable) in by_day[day]:
        file_id += 1
        register_day_file(conn, kind, p.name, day, file_id, len(valuable))
        ins = insert_fb_rows(conn, valuable, day, file_id)
        skipped = len(raw) - len(valuable)
        print(f'  {p.name[:60]:60} raw={len(raw):4} filter={len(valuable):4} skipped={skipped:3} inserted={ins}')

# Import Shopee orders
print(f'\n--- Shopee commission ({len(shopee_order_files)} file) ---')
for (p, day, raw, valuable) in shopee_order_files:
    file_id += 1
    register_day_file(conn, 'shopee_commission', p.name, day, file_id, len(valuable))
    insert_shopee_orders(conn, valuable, day, file_id)
    print(f'  {p.name[:60]:60} day={day} rows={len(valuable):4}')

# Import Shopee clicks
print(f'\n--- Shopee clicks ({len(shopee_click_files)} file) ---')
for (p, day, raw, valuable) in shopee_click_files:
    file_id += 1
    register_day_file(conn, 'shopee_clicks', p.name, day, file_id, len(valuable))
    insert_shopee_clicks(conn, valuable, day, file_id)
    print(f'  {p.name[:60]:60} day={day} rows={len(valuable):4}')

# Now compare each file vs DB — strict 100%
print()
print('=' * 70)
print('STAGE 2: Compare CSV vs DB per file (strict 100% match)')
print('=' * 70)

global_issues = []

for (p, kind, day, raw, valuable) in fb_files:
    # Group by (level, name) — UNIQUE constraint key
    by_name = defaultdict(list)
    for r in valuable:
        by_name[(r['level'], r['name'])].append(r)

    issues = []
    for (level, name), rows in by_name.items():
        # Expected behavior: "khớp 100%" = DB phải có data FULL của ROW có giá trị trong CSV.
        # Với duplicate rows: tổng spend/clicks = SUM các row valuable cùng name
        # (Hoặc pick 1 row có spend max - tranh luận, ưu tiên SUM cho strict).
        exp_spend = sum((r['spend'] or 0) for r in rows)
        exp_clicks = sum(
            (normalize_clicks(r['link_clicks'], r['all_clicks'], r['result_count']) or 0)
            for r in rows
        )

        # Query DB
        row = conn.execute(
            "SELECT spend, clicks FROM raw_fb_ads WHERE day_date=? AND level=? AND name=?",
            (day, level, name)
        ).fetchone()
        if row is None:
            issues.append(f'  MISSING: {level}/{name} expected spend={exp_spend:.0f}')
            continue
        db_spend = row[0] or 0
        db_clicks = row[1] or 0
        if abs(db_spend - exp_spend) > 0.01 or db_clicks != exp_clicks:
            issues.append(
                f'  MISMATCH {level}/{name}: DB spend={db_spend:.0f} clicks={db_clicks} '
                f'| CSV valuable SUM spend={exp_spend:.0f} clicks={exp_clicks} '
                f'(csv có {len(rows)} row valuable trùng tên)'
            )

    if issues:
        print(f'\n[{p.name}]')
        for i in issues[:10]: print(i)
        if len(issues) > 10: print(f'  ... còn {len(issues)-10} mismatches')
        global_issues.extend(issues)

print()
print('=' * 70)
print(f'TỔNG KẾT: {len(global_issues)} discrepancies')
print('=' * 70)

# Cross-check aggregate totals
print()
print('=== Totals CSV valuable vs DB per day ===')
for day in sorted(by_day):
    csv_spend = 0
    csv_clicks = 0
    for (p, kind, d, raw, valuable) in fb_files:
        if d != day: continue
        for r in valuable:
            csv_spend += r['spend'] or 0
            csv_clicks += normalize_clicks(r['link_clicks'], r['all_clicks'], r['result_count']) or 0

    db = conn.execute(
        "SELECT SUM(spend), SUM(clicks) FROM raw_fb_ads WHERE day_date=?", (day,)
    ).fetchone()
    db_spend = db[0] or 0
    db_clicks = db[1] or 0
    mark = 'OK' if abs(csv_spend - db_spend) < 0.01 and csv_clicks == db_clicks else 'FAIL'
    print(f'  [{mark}] {day}  CSV spend={csv_spend:>10.0f} clicks={csv_clicks:>5} '
          f'|  DB spend={db_spend:>10.0f} clicks={db_clicks:>5}')

# ==========================================================
# Shopee compare
# ==========================================================
print()
print('=== Shopee commission: CSV SUM(AK) vs DB SUM(net_commission) per day ===')
# Tổng hợp expected per day
csv_by_day = defaultdict(lambda: {'spend': 0, 'clicks': 0, 'net': 0, 'orders': set(), 'rows': 0})
for (p, day, raw, valuable) in shopee_order_files:
    for r in valuable:
        csv_by_day[day]['net'] += r['net_commission'] or 0
        csv_by_day[day]['orders'].add(r['order_id'])
        csv_by_day[day]['rows'] += 1

for day in sorted(csv_by_day):
    csv = csv_by_day[day]
    db = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT order_id), SUM(net_commission) "
        "FROM raw_shopee_order_items WHERE day_date=?", (day,)
    ).fetchone()
    db_rows, db_orders, db_net = db[0], db[1], db[2] or 0
    mark = 'OK' if abs(csv['net'] - db_net) < 0.5 and db_orders == len(csv['orders']) else 'FAIL'
    print(f'  [{mark}] {day}  CSV rows={csv["rows"]:4} orders={len(csv["orders"]):4} net={csv["net"]:>10.0f}  |  '
          f'DB rows={db_rows:4} orders={db_orders:4} net={db_net:>10.0f}')
    if mark == 'FAIL':
        global_issues.append(f'Shopee commission day {day}')

print()
print('=== Shopee clicks: CSV rows vs DB rows per day ===')
csv_clicks_by_day = defaultdict(set)
for (p, day, raw, valuable) in shopee_click_files:
    for r in valuable:
        csv_clicks_by_day[day].add(r['click_id'])
for day in sorted(csv_clicks_by_day):
    db = conn.execute("SELECT COUNT(*) FROM raw_shopee_clicks WHERE day_date=?", (day,)).fetchone()[0]
    csv_n = len(csv_clicks_by_day[day])
    mark = 'OK' if csv_n == db else 'FAIL'
    print(f'  [{mark}] {day}  CSV distinct click_ids={csv_n:5}  |  DB rows={db:5}')
    if mark == 'FAIL':
        global_issues.append(f'Shopee clicks day {day}')

sys.exit(1 if global_issues else 0)
