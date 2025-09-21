#!/usr/bin/env python3
"""
careerportal_enrich_by_year.py

What it does:
 - Computes targetYear = UTC_current_year - 1 (unless overridden by --year)
 - Loads ./output/<year>/fortune500_<year>.csv (columns: rank,company_name)
 - Queries DuckDuckGo to resolve a company's careers/jobs portal
 - Caches results in ./output/<year>/ddg_cache.json
 - Writes ./output/<year>/fortune500_<year>_with_careers.csv

Usage:
  # automatic (uses UTC now -> previous year)
  python careerportal_enrich_by_year.py

  # force a year
  python careerportal_enrich_by_year.py --year 2024

Requirements:
  pip install ddgs requests
"""

import os
import csv
import json
import time
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse

# Try ddgs first, fallback to duckduckgo_search
try:
    from ddgs import DDGS
except ImportError:
    try:
        from duckduckgo_search import DDGS
    except ImportError:
        DDGS = None

# Config / tuning
OUTPUT_ROOT = os.path.join(os.getcwd(), "output")
DELAY_BETWEEN_REQUESTS = 0.18
MAX_RESULTS_PER_QUERY = 10
PREFERRED_KEYWORDS = ("career", "careers", "job", "jobs", "vacancy", "join-us", "talent", "opportunities")

# === NEW: HELPER FUNCTIONS FOR CACHING ===
def load_cache(path):
    """Loads the cache file from the given path if it exists."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}

def save_cache(path, cache):
    """Saves the cache dictionary to the given path."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except IOError as e:
        print(f"[WARN] Could not save cache file to {path}: {e}")
# === END NEW HELPER FUNCTIONS ===


# Helpers: year logic
def compute_target_year(now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    return now.year - 1

def output_dir_for_year(year):
    return os.path.join(OUTPUT_ROOT, str(year))

def input_csv_for_year(year):
    return os.path.join(output_dir_for_year(year), f"fortune500_{year}.csv")

def cache_path_for_year(year):
    return os.path.join(output_dir_for_year(year), "ddg_cache.json")

def output_enriched_csv(year):
    return os.path.join(output_dir_for_year(year), f"fortune500_{year}_with_careers.csv")

# DDG search wrappers
def ddg_search_raw(query, max_results=MAX_RESULTS_PER_QUERY):
    if DDGS is None:
        raise RuntimeError("No DuckDuckGo client installed. Run: pip install ddgs")
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query, max_results=max_results))
    except Exception as e:
        print(f"[WARN] DuckDuckGo search failed for '{query}': {e}")
        return []

def normalize_href(r):
    return (r.get("href") or r.get("url") or "").strip()

def score_and_pick_best(results, company_name):
    if not results:
        return ""
    scored = []
    token = "".join(ch for ch in company_name if ch.isalnum()).lower()

    for idx, r in enumerate(results):
        href = normalize_href(r)
        title = (r.get("title") or "").lower()
        if not href:
            continue
        
        parsed = urlparse(href)
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        score = 0

        for kw in PREFERRED_KEYWORDS:
            if kw in href or kw in title:
                score += 60
        
        if token and token in netloc:
            score += 30

        if any(part in netloc for part in ("careers.", "jobs.", "talent.")):
            score += 40
            
        if any(dom in netloc for dom in ("wikipedia.org", "linkedin.com", "facebook.com")):
            score = 0 # Heavily penalize social/wiki sites

        score -= idx * 2 # Prioritize higher-ranked results
        scored.append((score, href))

    if not scored:
        return ""
        
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1] if scored[0][0] > 0 else ""

def find_career_link(company_name, cache):
    q = f"{company_name} careers site"
    if q in cache:
        return cache[q]
    
    results = ddg_search_raw(q, max_results=MAX_RESULTS_PER_QUERY)
    best = score_and_pick_best(results, company_name)
    
    cache[q] = best or ""
    return best or ""

# Main pipeline
def main():
    parser = argparse.ArgumentParser(description="Enrich Fortune500 CSV with careers links (DuckDuckGo).")
    parser.add_argument("--year", type=int, help="force year (overrides UTC-based target)")
    args = parser.parse_args()

    target_year = args.year or compute_target_year()
    in_csv = input_csv_for_year(target_year)
    out_csv = output_enriched_csv(target_year)
    cache_file = cache_path_for_year(target_year)

    print(f"Target Year: {target_year}")
    print(f"Input CSV: {in_csv}")

    if not os.path.exists(in_csv):
        print(f"[ERR] Input CSV not found. Please run the annual scraper first.")
        return

    if not DDGS:
        print("[ERR] No DuckDuckGo client found. Run: pip install ddgs")
        return

    with open(in_csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        companies = [row for row in reader if row.get("company_name")]

    cache = load_cache(cache_file)
    results = []

    try:
        for idx, row in enumerate(companies, start=1):
            name = row["company_name"]
            print(f"[{idx}/{len(companies)}] Resolving: {name}")
            link = find_career_link(name, cache)
            print("  ->", link or "(no match)")
            results.append({"rank": row.get("rank", ""), "company_name": name, "careers_link": link})
            time.sleep(DELAY_BETWEEN_REQUESTS)
    except KeyboardInterrupt:
        print("\nInterrupted. Flushing cache and partial output...")
    finally:
        save_cache(cache_file, cache)
        
        # Ensure output directory exists
        os.makedirs(output_dir_for_year(target_year), exist_ok=True)
        
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["rank", "company_name", "careers_link"])
            writer.writeheader()
            writer.writerows(results)
        print(f"\nSaved enriched CSV -> {out_csv}")

if __name__ == "__main__":
    main()

