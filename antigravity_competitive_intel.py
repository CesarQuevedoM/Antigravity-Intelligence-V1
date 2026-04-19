"""
antigravity_competitive_intel.py
=================================
Antigravity B2B Market Intelligence — Module 1
-----------------------------------------------
Pipeline:
    1. Interactive CSV loader  (auto-detects comma OR semicolon separator)
    2. Shopify Sitemap Hacker  (requests · rotating User-Agents · 5 s timeout)
    3. XML Parser              (stdlib ElementTree + BeautifulSoup fallback)
    4. Tier Scorer             (A / B / C business logic)
    5. CSV Exporter            (Leads_Clasificados.csv · Tier-sorted)

Author  : Antigravity Intelligence Stack
Version : 2.0.0
Python  : 3.9+
Deps    : pip install pandas requests beautifulsoup4 lxml
"""

import random
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False


# =============================================================================
# CONSTANTS
# =============================================================================

SITEMAP_TEMPLATE = "https://{domain}/sitemap_products.1.xml"
REQUEST_TIMEOUT  = 5        # seconds
TIER_A_MIN_PROD  = 30
TIER_A_MAX_DAYS  = 30
TIER_B_MAX_DAYS  = 183      # ~6 months
OUTPUT_FILE      = "Leads_Clasificados.csv"
REQUEST_DELAY    = 0.3      # polite delay between requests (seconds)

TIER_SORT_KEY = {"A": 0, "B": 1, "C": 2}

USER_AGENTS = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.4 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Mobile/15E148 Safari/604.1"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
]

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


# =============================================================================
# STEP 1 — INTERACTIVE CSV LOADER
# =============================================================================

def list_csv_files(directory: str = ".") -> list:
    """Return all .csv files in directory, sorted alphabetically."""
    return sorted(Path(directory).glob("*.csv"))


def interactive_csv_menu(search_dir: str = ".") -> pd.DataFrame:
    """
    Display a numbered terminal menu of every CSV in search_dir.
    User picks by index; returns a cleaned DataFrame.

    pd.read_csv params used:
        sep=None            auto-detects: comma, semicolon, tab, pipe …
        engine='python'     required for sep=None (uses csv.Sniffer)
        on_bad_lines='skip' silently drops malformed rows
        dtype=str           prevents unwanted coercion on domain strings
    """
    csv_files = list_csv_files(search_dir)

    if not csv_files:
        print(f"\n[ERROR] No CSV files found in '{search_dir}'.")
        print("Place your lead files there and re-run.\n")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("   ANTIGRAVITY COMPETITIVE INTEL — CSV LOADER")
    print("=" * 60)
    print(f"  Directory : {Path(search_dir).resolve()}")
    print(f"  CSV files : {len(csv_files)} found\n")

    for idx, fp in enumerate(csv_files, start=1):
        size_kb = fp.stat().st_size / 1024
        print(f"  [{idx}] {fp.name:<42} ({size_kb:,.1f} KB)")

    print("\n  [0] Exit")
    print("-" * 60)

    while True:
        raw = input("  Select a file by number: ").strip()
        if not raw.isdigit():
            print("  ⚠  Please enter a valid number.")
            continue
        choice = int(raw)
        if choice == 0:
            print("\n  Aborted. Goodbye.\n")
            sys.exit(0)
        if 1 <= choice <= len(csv_files):
            selected = csv_files[choice - 1]
            break
        print(f"  ⚠  Enter a number between 0 and {len(csv_files)}.")

    print(f"\n  Loading: {selected.name} …")

    df = pd.read_csv(
        selected,
        sep=None,               # auto-detect separator (, ; \t | …)
        engine="python",        # required for sep=None
        on_bad_lines="skip",    # skip corrupt rows silently
        dtype=str,              # keep all values as strings
    )

    # Strip accidental whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    if "TIENDA" not in df.columns:
        print(f"\n[ERROR] Column 'TIENDA' not found in {selected.name}.")
        print(f"  Columns detected: {list(df.columns)}")
        print("  Rename the domain column to 'TIENDA' and re-run.\n")
        sys.exit(1)

    df = df.dropna(subset=["TIENDA"])
    df["TIENDA"] = df["TIENDA"].str.strip()
    df = df[df["TIENDA"] != ""].reset_index(drop=True)

    print(f"  ✔  {len(df):,} valid records loaded.\n")
    return df


# =============================================================================
# STEP 2 — SHOPIFY SITEMAP HACKER
# =============================================================================

def _build_headers() -> dict:
    """Return HTTP headers with a randomly rotated User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }


def _clean_domain(raw: str) -> str:
    """Strip protocol prefix and trailing slash from a domain string."""
    domain = raw.strip()
    for prefix in ("https://", "http://"):
        if domain.startswith(prefix):
            domain = domain[len(prefix):]
    return domain.rstrip("/")


def fetch_sitemap(domain: str) -> Optional[str]:
    """
    GET the Shopify products sitemap for domain.

    Returns raw XML string on HTTP 200.
    Returns None on any error (timeout, DNS, 404, 403, non-Shopify).
    All exceptions are swallowed so the pipeline never halts.
    """
    clean = _clean_domain(domain)
    url = SITEMAP_TEMPLATE.format(domain=clean)

    try:
        resp = requests.get(
            url,
            headers=_build_headers(),
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code == 200:
            return resp.text
        return None
    except requests.exceptions.RequestException:
        return None


# =============================================================================
# STEP 3 — XML PARSER
# =============================================================================

def _parse_etree(xml_text: str) -> tuple:
    """
    Parse with stdlib xml.etree.ElementTree (fast, no extra deps).
    Returns (total_products: int, last_update: datetime | None).
    Raises ET.ParseError on malformed XML (caught by caller).
    """
    root = ET.fromstring(xml_text)

    urls = root.findall(".//sm:url", SITEMAP_NS)
    if not urls:
        # Some themes omit the namespace
        urls = root.findall(".//url")

    total = len(urls)
    dates = []

    for url_tag in urls:
        lastmod = url_tag.find("sm:lastmod", SITEMAP_NS)
        if lastmod is None:
            lastmod = url_tag.find("lastmod")
        if lastmod is not None and lastmod.text:
            try:
                dt = datetime.fromisoformat(lastmod.text.strip())
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dates.append(dt)
            except ValueError:
                pass

    last_update = max(dates) if dates else None
    return total, last_update


def _parse_bs4(xml_text: str) -> tuple:
    """
    BeautifulSoup fallback for malformed / namespace-broken XML.
    Returns (total_products: int, last_update: datetime | None).
    """
    soup = BeautifulSoup(xml_text, "lxml-xml")
    urls = soup.find_all("url")
    total = len(urls)
    dates = []

    for url_tag in urls:
        lastmod = url_tag.find("lastmod")
        if lastmod and lastmod.text:
            try:
                dt = datetime.fromisoformat(lastmod.text.strip())
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dates.append(dt)
            except ValueError:
                pass

    last_update = max(dates) if dates else None
    return total, last_update


def parse_sitemap(xml_text: str) -> tuple:
    """
    Try ElementTree first; fall back to BeautifulSoup on parse error.
    Returns (total_products: int, last_update: datetime | None).
    """
    try:
        return _parse_etree(xml_text)
    except ET.ParseError:
        if BS4_AVAILABLE:
            try:
                return _parse_bs4(xml_text)
            except Exception:
                pass
        return 0, None


# =============================================================================
# STEP 4 — TIER SCORING
# =============================================================================

def score_tier(total_products: int, last_update: Optional[datetime]) -> str:
    """
    Assign a competitive intelligence Tier based on sitemap metrics.

    Tier A  Big Fish  : > 30 products AND updated within last 30 days.
    Tier B  Average   : 1-29 products  OR  updated within last 6 months.
    Tier C  Discard   : 0 products, HTTP error, 404, or non-Shopify store.

    All datetime comparisons are UTC-aware to prevent naive/aware TypeError.
    """
    if total_products == 0:
        return "C"

    now = datetime.now(tz=timezone.utc)
    days_old = (now - last_update).days if last_update else None

    # Tier A — hot leads
    if (
        total_products > TIER_A_MIN_PROD
        and days_old is not None
        and days_old <= TIER_A_MAX_DAYS
    ):
        return "A"

    # Tier B — worth nurturing
    if 1 <= total_products <= TIER_A_MIN_PROD:
        return "B"

    if (
        total_products > TIER_A_MIN_PROD
        and days_old is not None
        and days_old <= TIER_B_MAX_DAYS
    ):
        return "B"

    # Tier C — everything else
    return "C"


# =============================================================================
# PIPELINE ORCHESTRATOR
# =============================================================================

def run_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Iterate every row, scrape its Shopify sitemap, parse, and score.
    Adds columns: Total_Products, Last_Update, Tier, Sitemap_URL.
    Preserves all original columns from the input CSV.
    """
    records = []
    total = len(df)

    print("-" * 60)
    print(f"  Running pipeline on {total:,} stores …")
    print("-" * 60)

    for i, row in enumerate(df.itertuples(index=False), start=1):
        domain = getattr(row, "TIENDA")
        clean = _clean_domain(domain)
        sitemap_url = SITEMAP_TEMPLATE.format(domain=clean)

        print(f"  [{i:>4}/{total}] {clean[:46]:<47}", end="", flush=True)

        xml_text = fetch_sitemap(domain)

        if xml_text is None:
            total_products = 0
            last_update = None
        else:
            total_products, last_update = parse_sitemap(xml_text)

        tier = score_tier(total_products, last_update)
        last_update_str = (
            last_update.strftime("%Y-%m-%d") if last_update else "N/A"
        )

        print(
            f"Tier {tier}  |  "
            f"Products: {total_products:>4}  |  "
            f"Updated: {last_update_str}"
        )

        record = row._asdict()
        record["Total_Products"] = total_products
        record["Last_Update"]    = last_update_str
        record["Tier"]           = tier
        record["Sitemap_URL"]    = sitemap_url
        records.append(record)

        time.sleep(REQUEST_DELAY)

    return pd.DataFrame(records)


# =============================================================================
# STEP 5 — EXPORT
# =============================================================================

def export_leads(df: pd.DataFrame, output_path: str = OUTPUT_FILE) -> Path:
    """
    Sort by Tier priority (A → B → C), then Total_Products descending.
    Writes a UTF-8-BOM CSV so Excel on Windows renders accents correctly.
    """
    df = df.copy()
    df["_sort"] = df["Tier"].map(TIER_SORT_KEY).fillna(99)
    df = df.sort_values(
        by=["_sort", "Total_Products"],
        ascending=[True, False],
    ).drop(columns=["_sort"])

    out = Path(output_path)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    return out.resolve()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main() -> None:
    """
    Usage:
        python antigravity_competitive_intel.py            # scans current dir
        python antigravity_competitive_intel.py ./my_data  # custom dir
    """
    search_dir = sys.argv[1] if len(sys.argv) > 1 else "."

    # Step 1 — Load CSV
    df_raw = interactive_csv_menu(search_dir)

    # Steps 2-4 — Scrape, Parse, Score
    df_enriched = run_pipeline(df_raw)

    # Print summary
    tier_counts = df_enriched["Tier"].value_counts().to_dict()
    tier_labels = {
        "A": "Big Fish   — Hot leads",
        "B": "Average    — Nurture",
        "C": "Discard    — Skip",
    }
    print("\n" + "=" * 60)
    print("  SCORING SUMMARY")
    print("-" * 60)
    for t in ["A", "B", "C"]:
        n = tier_counts.get(t, 0)
        print(f"  Tier {t}  {tier_labels[t]:<30}  {n:>5} stores")
    print("=" * 60)

    # Step 5 — Export
    out_path = export_leads(df_enriched)
    print(f"\n  ✔  Output saved → {out_path}\n")


if __name__ == "__main__":
    main()
