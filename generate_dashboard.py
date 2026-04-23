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

# CSV fallback: place any file named enrollments.csv in this directory
CSV_PATH = Path(__file__).parent / "enrollments.csv"

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


def build_html(metrics: list[dict], current_month: str, demo: bool) -> str:
    top5 = metrics[:5]
    cur_label = fmt_month(current_month)
    has_sparklines = any(m["sparkline"] for m in metrics)

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
          <div class="hero-state">{m['state']}</div>
          <div class="hero-mom red-3">{pct_display(m['mom_pct'])}</div>
          <div class="hero-sub">MoM</div>
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
  <title>Quit For Life — Enrollment Dip Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f4f5f7;
      color: #1a1a2e;
      min-height: 100vh;
    }}

    .demo-banner {{
      background: #fff3cd;
      border-bottom: 2px solid #ffc107;
      padding: 10px 24px;
      font-size: 13px;
      text-align: center;
    }}
    .demo-banner code {{
      background: #e9ecef;
      padding: 1px 5px;
      border-radius: 3px;
      font-size: 12px;
    }}

    .header {{
      background: #1a1a2e;
      color: #fff;
      padding: 20px 32px;
      display: flex;
      align-items: baseline;
      gap: 16px;
    }}
    .header h1 {{ font-size: 22px; font-weight: 700; letter-spacing: -0.3px; }}
    .header .as-of {{
      font-size: 13px;
      color: #9fa8da;
      margin-left: auto;
    }}

    .container {{ max-width: 1280px; margin: 0 auto; padding: 28px 24px; }}

    .section-label {{
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 1.2px;
      text-transform: uppercase;
      color: #6b7280;
      margin-bottom: 12px;
    }}

    .hero-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 14px;
      margin-bottom: 36px;
    }}
    .hero-card {{
      background: #fff;
      border: 1px solid #e5e7eb;
      border-left: 5px solid #ef4444;
      border-radius: 8px;
      padding: 18px 20px;
      box-shadow: 0 1px 3px rgba(0,0,0,.06);
    }}
    .hero-state {{
      font-size: 15px;
      font-weight: 600;
      color: #111827;
      margin-bottom: 6px;
    }}
    .hero-mom {{
      font-size: 32px;
      font-weight: 800;
      line-height: 1;
      margin-bottom: 2px;
    }}
    .hero-sub {{
      font-size: 11px;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.8px;
      margin-bottom: 10px;
    }}
    .hero-counts {{
      font-size: 12px;
      color: #6b7280;
      margin-bottom: 4px;
    }}
    .hero-yoy {{ font-size: 12px; font-weight: 600; }}

    .table-wrap {{
      background: #fff;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      overflow-x: auto;
      box-shadow: 0 1px 3px rgba(0,0,0,.06);
    }}
    .table-toolbar {{
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 14px 18px;
      border-bottom: 1px solid #f3f4f6;
    }}
    .table-toolbar input {{
      border: 1px solid #d1d5db;
      border-radius: 6px;
      padding: 6px 12px;
      font-size: 13px;
      width: 220px;
      outline: none;
    }}
    .table-toolbar input:focus {{ border-color: #6366f1; box-shadow: 0 0 0 2px rgba(99,102,241,.15); }}
    .record-count {{ font-size: 12px; color: #9ca3af; margin-left: auto; }}

    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 780px;
      font-size: 13px;
    }}
    thead th {{
      background: #f9fafb;
      padding: 10px 16px;
      text-align: left;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: #6b7280;
      border-bottom: 1px solid #e5e7eb;
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    thead th:hover {{ color: #374151; background: #f3f4f6; }}
    thead th .sort-arrow {{ display: inline-block; margin-left: 4px; opacity: 0.4; }}
    thead th.sort-asc .sort-arrow::after {{ content: "▲"; opacity: 1; }}
    thead th.sort-desc .sort-arrow::after {{ content: "▼"; opacity: 1; }}
    thead th:not(.sort-asc):not(.sort-desc) .sort-arrow::after {{ content: "⇅"; }}

    tbody tr {{ border-bottom: 1px solid #f3f4f6; }}
    tbody tr:last-child {{ border-bottom: none; }}
    tbody tr:hover {{ background: #fafafa; }}

    td {{ padding: 9px 16px; vertical-align: middle; }}
    .state-cell {{ font-weight: 500; color: #111827; }}
    .num-cell {{ text-align: right; font-variant-numeric: tabular-nums; color: #374151; }}
    .spark-cell {{ text-align: center; padding: 6px 12px; }}

    .red-3 {{ color: #b91c1c !important; font-weight: 700; }}
    .red-2 {{ color: #dc2626 !important; font-weight: 600; }}
    .red-1 {{ color: #f97316 !important; font-weight: 500; }}
    .neutral {{ color: #6b7280; }}
    .green-1 {{ color: #16a34a !important; font-weight: 500; }}
    .green-2 {{ color: #15803d !important; font-weight: 700; }}

    footer {{
      text-align: center;
      font-size: 11px;
      color: #9ca3af;
      padding: 24px;
    }}
  </style>
</head>
<body>

{demo_banner}

<header class="header">
  <h1>Quit For Life — Enrollment Dashboard</h1>
  <span class="as-of">Data as of {cur_label}</span>
</header>

<div class="container">

  <div class="section-label">States needing attention — biggest month-over-month dips</div>
  <div class="hero-grid">
    {hero_cards_html}
  </div>

  <div class="section-label">All states</div>
  <div class="table-wrap">
    <div class="table-toolbar">
      <input type="text" id="search" placeholder="Filter states…" />
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

<footer>Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} · Quit For Life enrollment tracker</footer>

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
    const color = (d.mom_pct !== null && d.mom_pct < -2) ? '#ef4444' : '#22c55e';
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

    html = build_html(metrics, current_month, demo=DEMO_MODE)
    out_path = Path(__file__).parent / "dashboard.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\n✅ Dashboard written to: {out_path}")
    print("   Open it in any browser to view.")


if __name__ == "__main__":
    main()
