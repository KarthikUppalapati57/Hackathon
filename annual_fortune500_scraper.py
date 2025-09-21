"""
annual_fortune_scrape.py
Standalone scraper + local persist (prints exact output paths)

Usage:
  python annual_fortune_scrape.py       # uses UTC_current_year - 1
  python annual_fortune_scrape.py --year 2024  # force a year

Output:
  ./output/<year>/fortune500_<year>.json
  ./output/<year>/fortune500_<year>.csv
"""

import os
import sys
import json
import csv
import argparse
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

def compute_target_year(now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    return now.year - 1

def output_dir_for_year(year):
    return os.path.join(os.getcwd(), "output", str(year))

def already_processed(year):
    json_path = os.path.join(output_dir_for_year(year), f"fortune500_{year}.json")
    return os.path.exists(json_path)

def fetch_html(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; annual-fortune-scraper/1.0; +you@example.com)",
        "Accept": "text/html,application/xhtml+xml",
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.text

def scrape_fortune_india_year(year):
    base = "https://www.fortuneindia.com"
    url = f"{base}/rankings/fortune-500/{year}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    companies = []
    seen = set()

    # primary: anchors with /companies/
    anchors = soup.select('a[href*="/companies/"]')
    for a in anchors:
        name = a.get_text(strip=True)
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        companies.append({"rank": len(companies) + 1, "company_name": name})

    # fallback: row/text scanning
    if not companies:
        import re
        for el in soup.find_all(["tr", "li", "div"]):
            txt = " ".join(el.get_text(separator=" ").split()).strip()
            m = re.match(r"^\s*(\d{1,3})[\.\s-]*\s+(.{2,200})$", txt)
            if m:
                rank = int(m.group(1))
                name = m.group(2).strip()
                key = name.lower()
                if key not in seen:
                    seen.add(key)
                    companies.append({"rank": rank, "company_name": name})

    # normalize ranks to sequential order
    if companies:
        companies.sort(key=lambda x: x.get("rank", 0))
        for idx, c in enumerate(companies, start=1):
            c["rank"] = idx

    return companies

def ensure_dir(d):
    if not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def persist_local(year, companies):
    d = output_dir_for_year(year)
    ensure_dir(d)
    json_path = os.path.join(d, f"fortune500_{year}.json")
    csv_path = os.path.join(d, f"fortune500_{year}.csv")

    with open(json_path, "w", encoding="utf8") as fh:
        json.dump(companies, fh, ensure_ascii=False, indent=2)

    with open(csv_path, "w", encoding="utf8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["rank", "company_name"])
        for c in companies:
            writer.writerow([c.get("rank"), c.get("company_name")])

    return {"json_path": json_path, "csv_path": csv_path}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="force target year (overrides computed value)")
    args = parser.parse_args()

    target_year = args.year or compute_target_year()
    print(f"Trigger (UTC): {datetime.now(timezone.utc).isoformat()} -> targetYear={target_year}")

    if already_processed(target_year):
        print(f"SKIP: Output already exists at: {os.path.join(output_dir_for_year(target_year), f'fortune500_{target_year}.json')}")
        return

    try:
        print(f"Scraping Fortune India Fortune-500 for {target_year} ...")
        companies = scrape_fortune_india_year(target_year)
        if not companies:
            print(f"No companies scraped for {target_year}. Exiting.")
            return

        saved = persist_local(target_year, companies)
        print(f"Saved {len(companies)} companies for {target_year}:")
        print(f"  JSON -> {saved['json_path']}")
        print(f"  CSV  -> {saved['csv_path']}")
    except Exception as e:
        print("Error during scrape:", str(e))
        raise

if __name__ == "__main__":
    main()
