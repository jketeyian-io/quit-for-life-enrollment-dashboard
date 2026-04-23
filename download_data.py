"""
Downloads enrollment data from Looker by logging in with browser credentials
and intercepting the dashboard tile API response.

Saves output to enrollments.csv (same format as a manual Looker export).
"""

import csv
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sys.exit("Missing dependency: pip install playwright && playwright install chromium")

LOOKER_BASE = "https://looker-prod.rvohealth.com"
LOOKER_USERNAME = os.getenv("LOOKER_USERNAME", "")
LOOKER_PASSWORD = os.getenv("LOOKER_PASSWORD", "")

DASHBOARD_URL = (
    "https://looker-prod.rvohealth.com/dashboards/TxuYIYtGLxp1XNoTlAHjPK"
    "?Client+Name=&Client+Type=State+Quit+Line"
    "&Main+Program+Enrollment+%28Yes+%2F+No%29=Yes"
    "&Program+Enrollment+Date=12+month+ago+for+12+month"
)

OUT_PATH = Path(__file__).parent / "enrollments.csv"

# These are the column names we expect from the Looker tile response.
# Adjust if your Looker field names differ.
EXPECTED_COLS = {
    "program_enrollment_month": "Program Enrollment Month",
    "client_name":              "Client Name",
    "enrollment_count":         "Enrollment Count",
    "enrollment_count_ly":      "Enrollment Count Last Year",
    "delta_ly":                 "Delta from This Month Last Year",
    "change_ly":                "Change Enrollment Count Last Year",
    "enrollment_count_lm":      "Enrollment Count Last Month",
    "change_lm":                "Change Enrollment Count Last Month",
}


def _is_enrollment_data(rows: list) -> bool:
    """Return True if the row list looks like state + month + enrollment data."""
    if not rows or not isinstance(rows, list) or not isinstance(rows[0], dict):
        return False
    keys = " ".join(rows[0].keys()).lower()
    return "client" in keys and ("enroll" in keys or "count" in keys) and ("month" in keys or "date" in keys)


def _normalise_rows(rows: list) -> list[dict]:
    """Map whatever Looker field names came back to our canonical CSV column names."""
    if not rows:
        return []

    src_keys = list(rows[0].keys())

    def find(candidates):
        for c in candidates:
            for k in src_keys:
                if c.lower() in k.lower():
                    return k
        return None

    col_map = {
        find(["month", "date", "period"]):          "Program Enrollment Month",
        find(["client_name", "client", "state"]):   "Client Name",
        find(["enroll", "count", "total"]):          "Enrollment Count",
    }
    # optional columns
    col_map.update({
        find(["last_year", "ly", "year_ago"]):            "Enrollment Count Last Year",
        find(["delta", "diff"]):                          "Delta from This Month Last Year",
        find(["change.*year", "pct.*year", "year.*pct"]): "Change Enrollment Count Last Year",
        find(["last_month", "lm", "month_ago"]):          "Enrollment Count Last Month",
        find(["change.*month", "pct.*month", "mom"]):     "Change Enrollment Count Last Month",
    })
    col_map = {k: v for k, v in col_map.items() if k}

    out = []
    for i, row in enumerate(rows, 1):
        mapped = {"": str(i)}
        for src_key, dst_col in col_map.items():
            mapped[dst_col] = row.get(src_key, "")
        out.append(mapped)
    return out


def download():
    if not LOOKER_USERNAME or not LOOKER_PASSWORD:
        sys.exit(
            "Missing credentials. Set LOOKER_USERNAME and LOOKER_PASSWORD "
            "in your .env file or environment."
        )

    captured: list = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        # Intercept API responses from the dashboard tile queries
        def on_response(response):
            if "/api/4.0/queries/" not in response.url:
                return
            try:
                body = response.json()
                if _is_enrollment_data(body) and len(body) > len(captured):
                    captured.clear()
                    captured.extend(body)
            except Exception:
                pass

        page.on("response", on_response)

        # ---- Login ----
        print("  Logging in to Looker...")
        page.goto(f"{LOOKER_BASE}/login", wait_until="domcontentloaded")

        # Fill credentials — Looker login selectors (handles most versions)
        page.locator(
            'input[type="email"], input[name="email"], input[id*="email"]'
        ).first.fill(LOOKER_USERNAME)
        page.locator(
            'input[type="password"], input[name="password"], input[id*="password"]'
        ).first.fill(LOOKER_PASSWORD)
        page.locator(
            'button[type="submit"], input[type="submit"], button:has-text("Log In"), button:has-text("Sign In")'
        ).first.click()

        page.wait_for_url(f"{LOOKER_BASE}/**", timeout=30_000)
        print("  Logged in.")

        # ---- Load dashboard (tile queries fire automatically) ----
        print("  Loading dashboard...")
        page.goto(DASHBOARD_URL, wait_until="domcontentloaded")

        # Wait up to 60s for network to settle
        try:
            page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            pass  # partial load is fine — we only need the one tile response

        browser.close()

    if not captured:
        sys.exit(
            "Could not capture enrollment data from the dashboard.\n"
            "Make sure the dashboard URL is correct and the tile is visible."
        )

    print(f"  Captured {len(captured)} rows.")

    rows = _normalise_rows(captured)
    if not rows:
        sys.exit("Could not map response columns to expected enrollment fields.")

    fieldnames = [""] + [v for v in EXPECTED_COLS.values() if v in rows[0]]
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved to {OUT_PATH.name} ({len(rows)} rows).")


if __name__ == "__main__":
    print("Downloading enrollment data from Looker...")
    download()
    print("Done.")
