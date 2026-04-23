"""
Quit For Life — Enrollment Dip Dashboard Generator

Usage:
    python generate_dashboard.py

Reads credentials from .env (or environment variables on Netlify),
fetches data from a Looker Dashboard or Look URL,
computes month-over-month and year-over-year enrollment changes,
and writes a self-contained dashboard.html.
"""

import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlparse, parse_qs

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests python-dotenv")

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("Missing dependency: pip install requests python-dotenv")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

LOOKER_BASE_URL = os.getenv("LOOKER_BASE_URL", "").rstrip("/")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID", "")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET", "")
LOOKER_URL = os.getenv("LOOKER_URL", "")  # dashboard or look URL

# CSV files (downloaded from Looker or captured by download_data.py)
CSV_PATH           = Path(__file__).parent / "enrollments.csv"
TRENDS_CSV_PATH    = Path(__file__).parent / "Monthly_Enrollment_Trends.csv"

HAS_API_CREDS = all([LOOKER_BASE_URL, LOOKER_CLIENT_ID, LOOKER_CLIENT_SECRET, LOOKER_URL])
DEMO_MODE = not HAS_API_CREDS and not CSV_PATH.exists()


# ---------------------------------------------------------------------------
# Looker API — auth
# ---------------------------------------------------------------------------

def get_token() -> str:
    resp = requests.post(
        f"{LOOKER_BASE_URL}/api/4.0/login",
        data={"client_id": LOOKER_CLIENT_ID, "client_secret": LOOKER_CLIENT_SECRET},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------------
# Looker API — Look URL
# ---------------------------------------------------------------------------

def fetch_look_data(headers: dict) -> list[dict]:
    match = re.search(r"/looks/(\d+)", LOOKER_URL)
    if not match:
        sys.exit(f"Could not extract look ID from URL: {LOOKER_URL}")
    look_id = match.group(1)

    resp = requests.post(
        f"{LOOKER_BASE_URL}/api/4.0/looks/{look_id}/run/json",
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Looker API — Dashboard URL
# ---------------------------------------------------------------------------

def fetch_dashboard_data(headers: dict) -> list[dict]:
    match = re.search(r"/dashboards/([^?/]+)", LOOKER_URL)
    if not match:
        sys.exit(f"Could not extract dashboard ID from URL: {LOOKER_URL}")
    dashboard_id = match.group(1)

    # Parse filter values from the URL query string
    url_filters = {
        k: v[0]
        for k, v in parse_qs(urlparse(LOOKER_URL).query).items()
        if v[0]  # skip empty filter values
    }

    # Fetch all dashboard elements (tiles)
    resp = requests.get(
        f"{LOOKER_BASE_URL}/api/4.0/dashboards/{dashboard_id}/dashboard_elements",
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    elements = resp.json()

    if not elements:
        sys.exit("No dashboard elements found for this dashboard ID.")

    best_rows = None

    for element in elements:
        result_maker = element.get("result_maker") or {}
        query = result_maker.get("query") or {}

        if not query.get("model") or not query.get("view"):
            continue

        # Map dashboard filter labels → query field names via filterables
        filters = dict(query.get("filters") or {})
        for filterable in (result_maker.get("filterables") or []):
            listen = filterable.get("listen") or {}
            for dash_label, field_name in listen.items():
                if dash_label in url_filters and url_filters[dash_label]:
                    filters[field_name] = url_filters[dash_label]

        query_body = {
            "model": query["model"],
            "view": query["view"],
            "fields": query.get("fields") or [],
            "filters": filters,
            "sorts": query.get("sorts") or [],
            "limit": "5000",
        }

        try:
            resp = requests.post(
                f"{LOOKER_BASE_URL}/api/4.0/queries/run/json",
                headers=headers,
                json=query_body,
                timeout=60,
            )
            if resp.status_code != 200:
                continue
            rows = resp.json()
        except Exception:
            continue

        if not rows or not isinstance(rows, list) or not rows[0]:
            continue

        keys = list(rows[0].keys())
        has_state = any("client" in k.lower() or "state" in k.lower() for k in keys)
        has_date = any("month" in k.lower() or "date" in k.lower() or "week" in k.lower() for k in keys)
        has_count = any("enroll" in k.lower() or "count" in k.lower() or "total" in k.lower() for k in keys)

        if has_state and has_date and has_count:
            # Prefer the element with the most rows (most complete data)
            if best_rows is None or len(rows) > len(best_rows):
                best_rows = rows

    if best_rows:
        return best_rows

    sys.exit(
        "Could not find enrollment data (state/client + date/month + count) in any dashboard tile.\n"
        "Make sure the dashboard has a tile with client/state, month/date, and an enrollment count field."
    )


# ---------------------------------------------------------------------------
# Unified fetch entry point
# ---------------------------------------------------------------------------

def fetch_looker_data() -> list[dict]:
    token = get_token()
    headers = {"Authorization": f"token {token}"}

    if "/dashboards/" in LOOKER_URL:
        print("  Detected dashboard URL — querying dashboard elements.")
        return fetch_dashboard_data(headers)
    elif "/looks/" in LOOKER_URL:
        print("  Detected Look URL — running look directly.")
        return fetch_look_data(headers)
    else:
        sys.exit(
            f"Unrecognized Looker URL format: {LOOKER_URL}\n"
            "Expected a URL containing /dashboards/ or /looks/"
        )


# ---------------------------------------------------------------------------
# Demo data (used when .env is not configured)
# ---------------------------------------------------------------------------

def generate_demo_data() -> list[dict]:
    import random
    random.seed(42)

    states = [
        "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
        "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
        "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
        "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
        "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
        "New Hampshire", "New Jersey", "New Mexico", "New York",
        "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
        "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
        "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
        "West Virginia", "Wisconsin", "Wyoming",
    ]

    rows = []
    today = date.today()
    for state in states:
        base = random.randint(200, 2000)
        for m in range(24):
            month_offset = 23 - m
            yr = today.year
            mo = today.month - month_offset
            while mo <= 0:
                mo += 12
                yr -= 1
            month_str = f"{yr}-{mo:02d}"
            delta = random.randint(-120, 100)
            if random.random() < 0.05:
                delta = random.randint(-400, -200)
            base = max(10, base + delta)
            rows.append({"state": state, "month": month_str, "enrollments": base})
    return rows


# ---------------------------------------------------------------------------
# Normalise raw Looker rows → {state, month, enrollments}
# ---------------------------------------------------------------------------

def normalise_rows(raw: list[dict]) -> list[dict]:
    if not raw:
        return []

    sample = raw[0]
    keys = list(sample.keys())

    def find_key(candidates):
        for c in candidates:
            for k in keys:
                if c.lower() in k.lower():
                    return k
        return None

    state_key = find_key(["client_name", "client", "state", "region"])
    month_key = find_key(["month", "date", "period", "year"])
    enroll_key = find_key(["enroll", "count", "total", "members", "participants"])

    if not all([state_key, month_key, enroll_key]):
        sys.exit(
            f"Could not auto-detect columns. Found: {keys}\n"
            "Expected columns containing: state/client, month/date, enroll/count"
        )

    print(f"  Using columns: state={state_key!r}, month={month_key!r}, enrollments={enroll_key!r}")

    normalised = []
    for row in raw:
        month_raw = str(row[month_key])
        month_match = re.search(r"(\d{4})-(\d{2})", month_raw)
        if month_match:
            month_str = f"{month_match.group(1)}-{month_match.group(2)}"
        else:
            try:
                dt = datetime.strptime(month_raw, "%B %Y")
                month_str = dt.strftime("%Y-%m")
            except ValueError:
                continue

        try:
            count = int(float(str(row[enroll_key]).replace(",", "")))
        except (ValueError, TypeError):
            count = 0

        normalised.append({
            "state": str(row[state_key]).strip(),
            "month": month_str,
            "enrollments": count,
        })

    return normalised


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def load_monthly_trends() -> list[dict]:
    """Load overall (all-state) monthly enrollment totals from the trends CSV.
    Returns [{month, count}, ...] sorted oldest→newest, or [] if file missing."""
    if not TRENDS_CSV_PATH.exists():
        return []
    import csv
    rows = []
    with open(TRENDS_CSV_PATH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            keys = list(row.keys())
            month_key = next((k for k in keys if "month" in k.lower() or "date" in k.lower()), None)
            # Count key: must contain count/total but NOT month/date (to avoid matching "Enrollment Month")
            count_key = next((k for k in keys if ("count" in k.lower() or "total" in k.lower())
                              and "month" not in k.lower() and "year" not in k.lower()
                              and "change" not in k.lower() and "last" not in k.lower()), None)
            if not month_key or not count_key:
                continue
            month_raw = row[month_key].strip()
            m = re.search(r"(\d{4})-(\d{2})", month_raw)
            if not m:
                continue
            try:
                count = int(float(row[count_key].replace(",", "").lstrip("'")))
            except (ValueError, TypeError):
                continue
            rows.append({"month": f"{m.group(1)}-{m.group(2)}", "count": count})
    return sorted(rows, key=lambda r: r["month"])


def compute_metrics(rows: list[dict]) -> tuple[list[dict], str]:
    data: dict[str, dict[str, int]] = defaultdict(dict)
    all_months = set()

    for row in rows:
        data[row["state"]][row["month"]] = row["enrollments"]
        all_months.add(row["month"])

    sorted_months = sorted(all_months)
    if len(sorted_months) < 2:
        sys.exit("Need at least 2 months of data to compute trends.")

    current_month = sorted_months[-1]
    prior_month = sorted_months[-2]

    cur_year, cur_mo = current_month.split("-")
    yoy_month = f"{int(cur_year) - 1}-{cur_mo}"

    trailing_12 = sorted_months[-12:]

    results = []
    for state, months in data.items():
        cur = months.get(current_month)
        prior = months.get(prior_month)
        yoy = months.get(yoy_month)

        if cur is None or prior is None:
            continue

        mom_pct = round((cur - prior) / prior * 100, 1) if prior else None
        yoy_pct = round((cur - yoy) / yoy * 100, 1) if yoy else None
        sparkline = [months.get(m) for m in trailing_12]

        results.append({
            "state": state,
            "current": cur,
            "prior": prior,
            "mom_pct": mom_pct,
            "yoy_pct": yoy_pct,
            "sparkline": sparkline,
            "current_month_label": current_month,
            "prior_month_label": prior_month,
        })

    results.sort(key=lambda x: (x["mom_pct"] is None, x["mom_pct"] if x["mom_pct"] is not None else 0))
    return results, current_month


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def fmt_month(ym: str) -> str:
    try:
        return datetime.strptime(ym, "%Y-%m").strftime("%b %Y")
    except ValueError:
        return ym


def pct_color_class(pct) -> str:
    if pct is None:
        return "neutral"
    if pct <= -15:
        return "red-3"
    if pct <= -8:
        return "red-2"
    if pct <= -2:
        return "red-1"
    if pct >= 8:
        return "green-2"
    if pct >= 2:
        return "green-1"
    return "neutral"


def pct_display(pct) -> str:
    if pct is None:
        return "—"
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def build_html(metrics: list[dict], current_month: str, demo: bool, trends: list[dict] = None) -> str:
    top5 = metrics[:5]
    cur_label = fmt_month(current_month)
    has_sparklines = any(m["sparkline"] for m in metrics)
    trends = trends or []

    # Overall trend chart block
    if trends:
        trend_labels = json.dumps([fmt_month(r["month"]) for r in trends])
        trend_values = json.dumps([r["count"] for r in trends])
        total_cur  = trends[-1]["count"]
        total_prev = trends[-2]["count"] if len(trends) >= 2 else None
        total_mom  = round((total_cur - total_prev) / total_prev * 100, 1) if total_prev else None
        total_mom_class = pct_color_class(total_mom)
        trend_section = f"""
  <div class="trend-section">
    <div class="trend-stat">
      <div class="trend-stat-label">Total Enrollments — {fmt_month(trends[-1]['month'])}</div>
      <div class="trend-stat-num">{total_cur:,}</div>
      <div class="trend-stat-mom {total_mom_class}">{pct_display(total_mom)} vs prior month</div>
    </div>
    <div class="trend-chart-wrap">
      <canvas id="overall-trend"></canvas>
    </div>
  </div>
  <script id="trend-data"
    data-labels='{trend_labels}'
    data-values='{trend_values}'></script>"""
    else:
        trend_section = ""

    chart_data = json.dumps([
        {
            "state": m["state"],
            "sparkline": [v if v is not None else 0 for v in m["sparkline"]],
            "mom_pct": m["mom_pct"],
        }
        for m in metrics
    ])

    spark_th = '<th class="num-cell">12-Month Trend</th>' if has_sparklines else ""

    table_rows_html = ""
    for i, m in enumerate(metrics):
        mom_class = pct_color_class(m["mom_pct"])
        yoy_class = pct_color_class(m["yoy_pct"])
        prior_display = f"{m['prior']:,}" if m["prior"] is not None else "—"
        spark_td = f'<td class="spark-cell"><canvas id="spark-{i}" width="120" height="36"></canvas></td>' if has_sparklines else ""
        table_rows_html += f"""
        <tr data-mom="{m['mom_pct'] if m['mom_pct'] is not None else 9999}"
            data-yoy="{m['yoy_pct'] if m['yoy_pct'] is not None else 9999}"
            data-state="{m['state']}"
            data-current="{m['current']}"
            data-prior="{m['prior'] if m['prior'] is not None else 0}">
          <td class="state-cell">{m['state']}</td>
          <td class="num-cell">{m['current']:,}</td>
          <td class="num-cell">{prior_display}</td>
          <td class="num-cell pct-cell {mom_class}">{pct_display(m['mom_pct'])}</td>
          <td class="num-cell pct-cell {yoy_class}">{pct_display(m['yoy_pct'])}</td>
          {spark_td}
        </tr>"""

    hero_cards_html = ""
    for m in top5:
        prior_display = f"{m['prior']:,}" if m["prior"] is not None else "—"
        hero_cards_html += f"""
        <div class="hero-card">
          <div class="hero-card-eyebrow">Month over month</div>
          <div class="hero-state">{m['state']}</div>
          <div class="hero-mom red-3">{pct_display(m['mom_pct'])}</div>
          <div class="hero-sub">change</div>
          <div class="hero-divider"></div>
          <div class="hero-counts">{m['current']:,} vs {prior_display}</div>
          <div class="hero-yoy pct-cell {pct_color_class(m['yoy_pct'])}">YoY: {pct_display(m['yoy_pct'])}</div>
        </div>"""

    demo_banner = """
    <div class="demo-banner">
      ⚠️ Running in <strong>demo mode</strong> — no .env credentials found.
      Copy <code>.env.example</code> → <code>.env</code>, fill in your Looker credentials, then re-run.
    </div>""" if demo else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Quit For Life® — Enrollment Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --qfl-teal:         #118575;
      --qfl-teal-deep:    #0B584D;
      --qfl-teal-light:   #67C3B7;
      --qfl-teal-whisper: rgba(103,195,183,0.15);
      --qfl-navy:         #24425A;
      --qfl-slate:        #556C7F;
      --qfl-ink:          #232020;
      --qfl-gray-500:     #A0A1A1;
      --qfl-gray-300:     #CDCFCF;
      --qfl-gray-100:     #F0F1F1;
      --qfl-white:        #FFFFFF;
      --font-sans: 'Poppins', 'Helvetica Neue', Arial, system-ui, sans-serif;
      --shadow-sm: 0 2px 6px rgba(36,66,90,0.08);
      --shadow-md: 0 8px 20px rgba(36,66,90,0.10);
      --shadow-focus: 0 0 0 3px rgba(17,133,117,0.35);
      --radius-card: 16px;
      --radius-sm: 8px;
    }}

    body {{
      font-family: var(--font-sans);
      background: var(--qfl-gray-100);
      color: var(--qfl-ink);
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
      font-size: 15px;
      line-height: 1.45;
    }}

    .demo-banner {{
      background: #fef9e7;
      border-bottom: 2px solid #f0c040;
      padding: 10px 24px;
      font-size: 13px;
      font-family: var(--font-sans);
      text-align: center;
      color: var(--qfl-navy);
    }}
    .demo-banner code {{
      background: rgba(36,66,90,0.08);
      padding: 1px 6px;
      border-radius: 4px;
      font-size: 12px;
    }}

    .header {{
      background: var(--qfl-navy);
      color: var(--qfl-white);
      padding: 18px 32px;
      display: flex;
      align-items: center;
      gap: 16px;
    }}
    .header-logo {{
      display: flex;
      align-items: center;
      gap: 10px;
    }}
    .header-logo-mark {{
      width: 36px;
      height: 36px;
      background: var(--qfl-teal);
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 700;
      font-size: 15px;
      color: #fff;
      letter-spacing: -0.5px;
      flex-shrink: 0;
    }}
    .header h1 {{
      font-size: 18px;
      font-weight: 600;
      letter-spacing: -0.02em;
      color: var(--qfl-white);
      margin: 0;
      line-height: 1.15;
    }}
    .header h1 span {{
      font-weight: 300;
      opacity: 0.7;
    }}
    .header .as-of {{
      font-size: 13px;
      color: var(--qfl-teal-light);
      margin-left: auto;
      font-weight: 500;
    }}

    .container {{ max-width: 1280px; margin: 0 auto; padding: 32px 24px; }}

    .qfl-eyebrow {{
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--qfl-teal);
      margin-bottom: 14px;
    }}

    .hero-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
      gap: 16px;
      margin-bottom: 40px;
    }}
    .hero-card {{
      background: var(--qfl-white);
      border: 1px solid var(--qfl-gray-300);
      border-radius: var(--radius-card);
      padding: 20px 22px;
      box-shadow: var(--shadow-sm);
      transition: box-shadow 220ms cubic-bezier(0.2,0,0,1), transform 220ms cubic-bezier(0.2,0,0,1);
    }}
    .hero-card:hover {{ box-shadow: var(--shadow-md); transform: translateY(-1px); }}
    .hero-card-eyebrow {{
      font-size: 10px;
      font-weight: 600;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--qfl-slate);
      margin-bottom: 8px;
    }}
    .hero-state {{
      font-size: 15px;
      font-weight: 600;
      color: var(--qfl-navy);
      margin-bottom: 8px;
      line-height: 1.15;
    }}
    .hero-mom {{
      font-size: 34px;
      font-weight: 700;
      line-height: 1;
      margin-bottom: 2px;
      letter-spacing: -0.02em;
    }}
    .hero-sub {{
      font-size: 10px;
      color: var(--qfl-gray-500);
      text-transform: uppercase;
      letter-spacing: 0.1em;
      margin-bottom: 12px;
      font-weight: 500;
    }}
    .hero-divider {{
      height: 1px;
      background: var(--qfl-gray-300);
      margin-bottom: 10px;
    }}
    .hero-counts {{
      font-size: 12px;
      color: var(--qfl-slate);
      margin-bottom: 4px;
    }}
    .hero-yoy {{ font-size: 12px; font-weight: 600; }}

    .table-wrap {{
      background: var(--qfl-white);
      border: 1px solid var(--qfl-gray-300);
      border-radius: var(--radius-card);
      overflow-x: auto;
      box-shadow: var(--shadow-sm);
    }}
    .table-toolbar {{
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 16px 20px;
      border-bottom: 1px solid var(--qfl-gray-100);
    }}
    .search-wrap {{
      position: relative;
      display: flex;
      align-items: center;
    }}
    .search-icon {{
      position: absolute;
      left: 10px;
      color: var(--qfl-gray-500);
      font-size: 14px;
      pointer-events: none;
    }}
    .table-toolbar input {{
      border: 1.5px solid var(--qfl-gray-300);
      border-radius: 9999px;
      padding: 7px 14px 7px 32px;
      font-size: 13px;
      font-family: var(--font-sans);
      width: 240px;
      outline: none;
      color: var(--qfl-ink);
      background: var(--qfl-gray-100);
      transition: border-color 140ms, box-shadow 140ms;
    }}
    .table-toolbar input:focus {{
      border-color: var(--qfl-teal);
      background: var(--qfl-white);
      box-shadow: var(--shadow-focus);
    }}
    .table-toolbar input::placeholder {{ color: var(--qfl-gray-500); }}
    .record-count {{ font-size: 12px; color: var(--qfl-gray-500); margin-left: auto; font-weight: 500; }}

    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 780px;
      font-size: 13px;
    }}
    thead th {{
      background: var(--qfl-gray-100);
      padding: 11px 16px;
      text-align: left;
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--qfl-slate);
      border-bottom: 1px solid var(--qfl-gray-300);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
      font-family: var(--font-sans);
    }}
    thead th:hover {{ color: var(--qfl-navy); background: #e8eaeb; }}
    thead th .sort-arrow {{ display: inline-block; margin-left: 4px; opacity: 0.4; }}
    thead th.sort-asc .sort-arrow::after {{ content: "▲"; opacity: 1; color: var(--qfl-teal); }}
    thead th.sort-desc .sort-arrow::after {{ content: "▼"; opacity: 1; color: var(--qfl-teal); }}
    thead th:not(.sort-asc):not(.sort-desc) .sort-arrow::after {{ content: "⇅"; }}

    tbody tr {{ border-bottom: 1px solid var(--qfl-gray-100); }}
    tbody tr:last-child {{ border-bottom: none; }}
    tbody tr:hover {{ background: rgba(103,195,183,0.06); }}

    td {{ padding: 10px 16px; vertical-align: middle; }}
    .state-cell {{ font-weight: 500; color: var(--qfl-navy); }}
    .num-cell {{ text-align: right; font-variant-numeric: tabular-nums; color: var(--qfl-ink); }}
    .spark-cell {{ text-align: center; padding: 6px 12px; }}

    .red-3 {{ color: #b91c1c !important; font-weight: 700; }}
    .red-2 {{ color: #dc2626 !important; font-weight: 600; }}
    .red-1 {{ color: #e07000 !important; font-weight: 500; }}
    .neutral {{ color: var(--qfl-gray-500); }}
    .green-1 {{ color: #1a7a50 !important; font-weight: 500; }}
    .green-2 {{ color: var(--qfl-teal-deep) !important; font-weight: 700; }}

    /* ---- Overall trend ---- */
    .trend-section {{
      background: var(--qfl-white);
      border: 1px solid var(--qfl-gray-300);
      border-radius: var(--radius-card);
      padding: 22px 28px;
      margin-bottom: 32px;
      display: flex;
      align-items: center;
      gap: 36px;
      box-shadow: var(--shadow-sm);
    }}
    .trend-stat {{ flex: 0 0 auto; min-width: 170px; }}
    .trend-stat-label {{
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.1em;
      color: var(--qfl-slate);
      margin-bottom: 6px;
    }}
    .trend-stat-num {{
      font-size: 40px;
      font-weight: 700;
      color: var(--qfl-navy);
      line-height: 1;
      margin-bottom: 5px;
      letter-spacing: -0.02em;
    }}
    .trend-stat-mom {{ font-size: 13px; font-weight: 600; }}
    .trend-chart-wrap {{ flex: 1; min-width: 0; height: 110px; position: relative; }}
    .trend-chart-wrap canvas {{ position: absolute; top: 0; left: 0; width: 100% !important; height: 100% !important; }}

    footer {{
      text-align: center;
      font-size: 11px;
      color: var(--qfl-gray-500);
      padding: 28px 24px;
      font-family: var(--font-sans);
    }}
    footer strong {{ color: var(--qfl-slate); font-weight: 600; }}
  </style>
</head>
<body>

{demo_banner}

<header class="header">
  <div class="header-logo">
    <div class="header-logo-mark">QFL</div>
    <h1>Quit For Life® <span>Enrollment Dashboard</span></h1>
  </div>
  <span class="as-of">Data as of {cur_label}</span>
</header>

<div class="container">

{trend_section}
  <div class="qfl-eyebrow">States needing attention — biggest month-over-month dips</div>
  <div class="hero-grid">
    {hero_cards_html}
  </div>

  <div class="qfl-eyebrow">All states</div>
  <div class="table-wrap">
    <div class="table-toolbar">
      <div class="search-wrap">
        <span class="search-icon">⌕</span>
        <input type="text" id="search" placeholder="Filter states…" />
      </div>
      <span class="record-count" id="record-count"></span>
    </div>
    <table id="main-table">
      <thead>
        <tr>
          <th data-col="state">State <span class="sort-arrow"></span></th>
          <th data-col="current" class="num-cell">{fmt_month(current_month)} <span class="sort-arrow"></span></th>
          <th data-col="prior" class="num-cell">Prior Month <span class="sort-arrow"></span></th>
          <th data-col="mom" class="num-cell">MoM Change <span class="sort-arrow"></span></th>
          <th data-col="yoy" class="num-cell">YoY Change <span class="sort-arrow"></span></th>
          {spark_th}
        </tr>
      </thead>
      <tbody id="table-body">
        {table_rows_html}
      </tbody>
    </table>
  </div>

</div>

<footer>Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} &nbsp;·&nbsp; <strong>Quit For Life®</strong> enrollment tracker &nbsp;·&nbsp; RVO Health, Inc.</footer>

<script>
const chartData = {chart_data};

function drawSparklines(rows) {{
  rows.forEach((row, i) => {{
    const canvas = document.getElementById(`spark-${{i}}`);
    if (!canvas) return;
    const d = chartData[i];
    if (!d) return;
    const vals = d.sparkline.filter(v => v !== null && v !== 0);
    if (!vals.length) return;
    const min = Math.min(...vals);
    const max = Math.max(...vals);
    const color = (d.mom_pct !== null && d.mom_pct < -2) ? '#dc2626' : '#118575';
    new Chart(canvas, {{
      type: 'line',
      data: {{
        labels: d.sparkline.map((_, idx) => idx),
        datasets: [{{ data: d.sparkline, borderColor: color, borderWidth: 1.5,
          pointRadius: 0, tension: 0.3, fill: false }}]
      }},
      options: {{
        animation: false,
        plugins: {{ legend: {{ display: false }}, tooltip: {{ enabled: false }} }},
        scales: {{
          x: {{ display: false }},
          y: {{ display: false, min: Math.max(0, min * 0.85), max: max * 1.1 }}
        }},
        responsive: false,
      }}
    }});
  }});
}}

let sortCol = 'mom';
let sortDir = 'asc';

function getVal(row, col) {{
  switch(col) {{
    case 'state': return row.dataset.state;
    case 'current': return +row.dataset.current;
    case 'prior': return +row.dataset.prior;
    case 'mom': return +row.dataset.mom;
    case 'yoy': return +row.dataset.yoy;
  }}
  return '';
}}

function sortTable(col) {{
  if (sortCol === col) {{
    sortDir = sortDir === 'asc' ? 'desc' : 'asc';
  }} else {{
    sortCol = col;
    sortDir = 'asc';
  }}
  const tbody = document.getElementById('table-body');
  const rows = Array.from(tbody.querySelectorAll('tr:not([style*="display: none"])'));
  const hidden = Array.from(tbody.querySelectorAll('tr[style*="display: none"]'));
  rows.sort((a, b) => {{
    const av = getVal(a, col), bv = getVal(b, col);
    const cmp = typeof av === 'string' ? av.localeCompare(bv) : av - bv;
    return sortDir === 'asc' ? cmp : -cmp;
  }});
  [...rows, ...hidden].forEach(r => tbody.appendChild(r));
  document.querySelectorAll('thead th').forEach(th => {{
    th.classList.remove('sort-asc', 'sort-desc');
    if (th.dataset.col === col) th.classList.add(sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
  }});
  drawSparklines(rows);
  updateCount();
}}

document.getElementById('search').addEventListener('input', e => {{
  const q = e.target.value.toLowerCase();
  document.querySelectorAll('#table-body tr').forEach(row => {{
    row.style.display = row.dataset.state.toLowerCase().includes(q) ? '' : 'none';
  }});
  updateCount();
}});

function updateCount() {{
  const visible = document.querySelectorAll('#table-body tr:not([style*="display: none"])').length;
  document.getElementById('record-count').textContent = `${{visible}} states`;
}}

document.querySelectorAll('thead th[data-col]').forEach(th => {{
  th.addEventListener('click', () => sortTable(th.dataset.col));
}});

// Overall trend chart
const trendEl = document.getElementById('overall-trend');
if (trendEl) {{
  const td = document.getElementById('trend-data');
  const labels = JSON.parse(td.dataset.labels);
  const values = JSON.parse(td.dataset.values);
  new Chart(trendEl, {{
    type: 'line',
    data: {{
      labels,
      datasets: [{{
        data: values,
        borderColor: '#118575',
        backgroundColor: 'rgba(103,195,183,0.12)',
        borderWidth: 2.5,
        pointRadius: 3,
        pointBackgroundColor: '#118575',
        tension: 0.3,
        fill: true,
      }}]
    }},
    options: {{
      animation: false,
      plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{
        label: ctx => ' ' + ctx.parsed.y.toLocaleString() + ' enrollments'
      }} }} }},
      scales: {{
        x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 11, family: "'Poppins', sans-serif" }} }} }},
        y: {{ grid: {{ color: '#F0F1F1' }}, ticks: {{ font: {{ size: 11, family: "'Poppins', sans-serif" }}, callback: v => v.toLocaleString() }} }}
      }},
      responsive: true,
      maintainAspectRatio: false,
    }}
  }});
}}

// Init: sort by MoM ascending (biggest dips first)
sortTable('mom');
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# CSV loader — reads pre-computed MoM/YoY columns from Looker export
# ---------------------------------------------------------------------------

# Sorted longest-first so multi-word states match before single-word ones
_STATES = sorted([
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming",
    "DC", "Guam", "Puerto Rico",
], key=len, reverse=True)

_ABBREVS = {
    "NC": "North Carolina", "SC": "South Carolina",
    "ND": "North Dakota",   "SD": "South Dakota",
    "WV": "West Virginia",  "NM": "New Mexico",
    "NJ": "New Jersey",     "NH": "New Hampshire",
    "NY": "New York",       "DC": "DC",
    "VA": "Virginia",       "IN": "Indiana",
    "FL": "Florida",        "GA": "Georgia",
    "OK": "Oklahoma",       "OH": "Ohio",
    "OR": "Oregon",         "WI": "Wisconsin",
}


def _extract_state(client_name: str) -> str:
    name = re.sub(r"_DTC_Coach$", "", client_name).strip()
    # Check abbreviations at word boundaries OR at start/end of compound words (e.g. "DCQuitNow", "QuitlineNC")
    for abbrev, state in _ABBREVS.items():
        if (name.startswith(abbrev) or name.endswith(abbrev)
                or re.search(rf"\b{abbrev}\b", name)):
            return state
    # Check full state names (longest first to avoid "Virginia" beating "West Virginia")
    for state in _STATES:
        if re.search(rf"\b{re.escape(state)}\b", name, re.IGNORECASE):
            return state
    # Fall back to stripped name
    return re.sub(r"\s+", " ", name).strip()


def _parse_pct(s: str):
    if not s or s.strip() in ("", "-"):
        return None
    try:
        return round(float(s.strip().rstrip("%")), 1)
    except ValueError:
        return None


def _parse_int(s: str):
    if not s or s.strip() in ("", "-"):
        return None
    try:
        return int(float(s.replace(",", "").strip()))
    except ValueError:
        return None


def load_csv_precomputed() -> tuple[list[dict], str]:
    import csv
    metrics = []
    months = set()

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            state = _extract_state(row["Client Name"])
            current_month = row["Program Enrollment Month"].strip()
            months.add(current_month)

            current = _parse_int(row["Enrollment Count"]) or 0
            prior = _parse_int(row.get("Enrollment Count Last Month", ""))
            mom_pct = _parse_pct(row.get("Change Enrollment Count Last Month", ""))
            yoy_pct = _parse_pct(row.get("Change Enrollment Count Last Year", ""))

            metrics.append({
                "state": state,
                "current": current,
                "prior": prior,
                "mom_pct": mom_pct,
                "yoy_pct": yoy_pct,
                "sparkline": [],
                "current_month_label": current_month,
                "prior_month_label": "",
            })

    current_month = sorted(months)[-1] if months else "unknown"
    metrics.sort(key=lambda x: (x["mom_pct"] is None, x["mom_pct"] if x["mom_pct"] is not None else 0))
    return metrics, current_month


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if DEMO_MODE:
        print("⚠️  No credentials or CSV found — running in demo mode with sample data.")
        print("   To use real data, either:")
        print("   1. Drop enrollments.csv in this folder (export from Looker), OR")
        print("   2. Fill in .env with Looker API credentials.\n")
        raw = generate_demo_data()
        normalised = raw
    elif CSV_PATH.exists() and not HAS_API_CREDS:
        print(f"📂 Reading data from {CSV_PATH.name} ...")
        metrics, current_month = load_csv_precomputed()
        print(f"  Loaded {len(metrics)} clients. Month: {current_month}")
    else:
        print(f"Fetching data from Looker: {LOOKER_URL}")
        raw = fetch_looker_data()
        print(f"  Got {len(raw)} rows.")
        normalised = normalise_rows(raw)
        print(f"  Normalised to {len(normalised)} rows.")
        metrics, current_month = compute_metrics(normalised)
        print(f"  Computed metrics for {len(metrics)} states. Most recent month: {current_month}")

    trends = load_monthly_trends()
    html = build_html(metrics, current_month, demo=DEMO_MODE, trends=trends)
    out_path = Path(__file__).parent / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\n✅ Dashboard written to: {out_path}")
    print("   Open it in any browser to view.")


if __name__ == "__main__":
    main()
