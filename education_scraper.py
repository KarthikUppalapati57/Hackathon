#!/usr/bin/env python3
"""
company_education_improved.py

- Auto-targets previous UTC year (or use --year)
- Reads ./output/<year>/fortune500_<year>.csv (rank,company_name)
- Uses DuckDuckGo (ddgs preferred) to search and heuristically detect
  whether the company offers official courses/training/roadmaps/events
- Writes ./output/<year>/fortune500_<year>_education.csv
- Caches results in ./output/<year>/edu_cache.json and ./output/<year>/content_cache.json
"""

import os
import csv
import json
import time
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

# --- ddg client: ddgs preferred, fallback to duckduckgo_search ---
DDGS = None
provider = None
try:
    from ddgs import DDGS as _DDGS
    DDGS = _DDGS
    provider = "ddgs"
except Exception:
    try:
        from duckduckgo_search import DDGS as _DDGS2
        DDGS = _DDGS2
        provider = "duckduckgo_search"
    except Exception:
        DDGS = None
        provider = None

# ---------- config ----------
OUTPUT_ROOT = os.path.join(os.getcwd(), "output")
DELAY = 0.18
MAX_RESULTS = 10
TOP_FETCH = 3  # fetch content for top N candidates
FINAL_SCORE_THRESHOLD = 60  # >= => "Yes"
MIN_CONTENT_KEYWORD_MATCHES = 1  # at least 1 matching content keyword

# Keywords indicative of genuine learning/training resources
EDU_KEYWORDS = (
    "learn", "learning", "academy", "training", "course", "courses", "education",
    "skill", "skills", "bootcamp", "certification", "certifications", "path", "roadmap",
    "webinar", "workshop", "upskill", "upskilling", "developer", "developer training",
    "learning path", "learning paths", "study", "curriculum", "program", "programs",
)

# Domains we treat as noisy / third-party (filter these unless domain contains company token)
BLACKLIST_DOMAINS = (
    "medium.com", "forbes.com", "timesofindia", "indiatoday", "ndtv.com", "googleusercontent.com",
    "facebook.com", "linkedin.com", "twitter.com", "youtube.com", "reddit.com", "quora.com",
    "wordpress.com", "blogspot.com", "glassdoor.com", "indeed.com", "jooble.org", "jobsite",
    "news", "economictimes", "mint", "thehindu", "linkedin.", "razorpay.com"  # razorpay often shows tutorials
)

# explicit whitelist of learning subdomain patterns we trust (company-owned or common official platforms)
TRUSTED_LEARNING_DOMAINS_PARTS = (
    "academy", "learn", "learning", "skills", "cloudskillsboost", "training", "education", "developers", "campus", "university"
)

# ---------- helpers: paths & year logic ----------
def compute_target_year(now=None):
    if now is None:
        now = datetime.now(timezone.utc)
    return now.year - 1

def output_dir_for_year(year):
    return os.path.join(OUTPUT_ROOT, str(year))

def input_csv_for_year(year):
    return os.path.join(output_dir_for_year(year), f"fortune500_{year}.csv")

def output_education_csv(year):
    return os.path.join(output_dir_for_year(year), f"fortune500_{year}_education.csv")

def cache_path_for_year(year):
    return os.path.join(output_dir_for_year(year), "edu_cache.json")

def content_cache_path_for_year(year):
    return os.path.join(output_dir_for_year(year), "content_cache.json")

# ---------- ddg search ----------
def ddg_search(query, max_results=MAX_RESULTS):
    if DDGS is None:
        raise RuntimeError("No DuckDuckGo client found. Install ddgs or duckduckgo-search.")
    try:
        with DDGS() as ddgs:
            return list(ddgs.text(query, max_results=max_results))
    except Exception as e:
        print(f"[WARN] DuckDuckGo search failed for '{query}': {e}")
        return []

def normalize_href(res):
    return (res.get("href") or res.get("url") or "").strip()

# ---------- scoring helpers ----------
def domain_of(url):
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def prelim_score_from_result(res, company_token):
    href = normalize_href(res)
    title = (res.get("title") or "").lower()
    parsed = urlparse(href)
    netloc = parsed.netloc.lower()
    path = parsed.path.lower()
    score = 0

    # boost if any EDU keyword in title/path/url
    for kw in EDU_KEYWORDS:
        if kw in title or kw in path or kw in href.lower():
            score += 40

    # if domain contains company_token: likely official domain
    if company_token and company_token in netloc:
        score += 35

    # domain has learning-specific part
    if any(part in netloc for part in TRUSTED_LEARNING_DOMAINS_PARTS) or any(part in path for part in TRUSTED_LEARNING_DOMAINS_PARTS):
        score += 45

    # penalize known noisy sources
    if any(b in netloc for b in BLACKLIST_DOMAINS) and (not (company_token and company_token in netloc)):
        score -= 120

    # small ordering bonus not applied here (we rank before fetching)
    return score

def fetch_page_text(url, content_cache, timeout=8):
    if url in content_cache:
        return content_cache[url]
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; edu-detector/1.0; +you@example.com)"}
        r = requests.get(url, headers=headers, timeout=timeout)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True).lower()
    except Exception:
        text = ""
    # persist minimal snippet to cache
    content_cache[url] = text
    return text

def content_score_for_text(text, company_token):
    if not text:
        return 0, 0
    kw_matches = 0
    for kw in EDU_KEYWORDS:
        if kw in text:
            kw_matches += 1
    # company token presence (bonus)
    token_match = 1 if (company_token and company_token in text) else 0
    return kw_matches, token_match

def final_score(prelim, kw_matches, token_match, netloc):
    # content contribution: each keyword match * 18
    score = prelim + (kw_matches * 18) + (token_match * 20)
    # extra boost if netloc explicitly contains learning/zones
    if any(part in netloc for part in TRUSTED_LEARNING_DOMAINS_PARTS):
        score += 15
    # cap lower bound
    return score

# ---------- main detection per company ----------
def find_education_for_company(company_name, year, cache, content_cache):
    q = f"{company_name} learning academy training courses 'learning path' webinar workshop"
    if q in cache:
        return cache[q]

    # Prepare company token (simple heuristic)
    token = "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in company_name).lower().split()
    company_token = token[0] if token else ""

    results = ddg_search(q, max_results=MAX_RESULTS)
    if not results:
        cache[q] = {"offers": "No", "link": "", "title": "", "score": 0, "reason": "no_search_results"}
        return cache[q]

    # compute prelim scores
    prelim_list = []
    for idx, r in enumerate(results):
        href = normalize_href(r)
        if not href:
            continue
        netloc = domain_of(href)
        prelim = prelim_score_from_result(r, company_token)
        prelim_list.append((prelim, idx, href, r.get("title") or "", netloc))

    if not prelim_list:
        cache[q] = {"offers": "No", "link": "", "title": "", "score": 0, "reason": "no_candidates"}
        return cache[q]

    # rank by prelim and pick top N to fetch content
    prelim_list.sort(key=lambda x: x[0], reverse=True)
    top_candidates = prelim_list[:TOP_FETCH]

    scored_candidates = []
    for prelim, idx, href, title, netloc in top_candidates:
        # fetch text (cached)
        text = fetch_page_text(href, content_cache)
        kw_matches, token_match = content_score_for_text(text, company_token)
        final = final_score(prelim, kw_matches, token_match, netloc)
        scored_candidates.append((final, prelim, kw_matches, token_match, href, title, netloc))

    # If none of top candidates gave strong signals, optionally check the top-most result's domain-specific search
    if not scored_candidates or max(c[0] for c in scored_candidates) < 30:
        # domain-specific fallback using domain from first search result
        first_href = normalize_href(results[0])
        first_domain = domain_of(first_href)
        if first_domain:
            # search site:first_domain careers
            fallback_q = f"site:{first_domain} careers OR training OR academy OR learn"
            fb_results = ddg_search(fallback_q, max_results=6)
            for r in fb_results:
                href = normalize_href(r)
                if not href:
                    continue
                netloc = domain_of(href)
                text = fetch_page_text(href, content_cache)
                kw_matches, token_match = content_score_for_text(text, company_token)
                prelim = prelim_score_from_result(r, company_token)
                final = final_score(prelim, kw_matches, token_match, netloc)
                scored_candidates.append((final, prelim, kw_matches, token_match, href, r.get("title") or "", netloc))

    if not scored_candidates:
        cache[q] = {"offers": "No", "link": "", "title": "", "score": 0, "reason": "no_scored"}
        return cache[q]

    # pick best candidate
    scored_candidates.sort(key=lambda x: x[0], reverse=True)
    best = scored_candidates[0]
    best_score, best_prelim, best_kw_matches, best_token_match, best_href, best_title, best_netloc = best

    reason = []
    if best_token_match:
        reason.append("company_token_in_content")
    if best_kw_matches:
        reason.append(f"content_kw_matches={best_kw_matches}")
    if "academy" in best_netloc or "learn" in best_netloc or "skills" in best_netloc:
        reason.append("domain_edu_keyword")

    # blacklist safety: if domain blacklisted and company token not in domain -> reject unless very high score
    if any(b in best_netloc for b in BLACKLIST_DOMAINS) and (not (company_token and company_token in best_netloc)):
        if best_score < 80:
            cache[q] = {"offers": "No", "link": "", "title": "", "score": best_score, "reason": "blacklisted_domain_low_score"}
            return cache[q]
        else:
            reason.append("override_blacklist_by_score")

    offers = "Yes" if (best_score >= FINAL_SCORE_THRESHOLD and best_kw_matches >= MIN_CONTENT_KEYWORD_MATCHES) or (best_token_match and best_kw_matches >= 1) else "No"

    out = {
        "offers": offers,
        "link": best_href,
        "title": best_title,
        "score": int(best_score),
        "reason": ";".join(reason) if reason else "scored_candidate"
    }
    cache[q] = out
    return out

# ---------- pipeline ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="force year (defaults to UTC now - 1)")
    parser.add_argument("--only-yes", action="store_true", help="write only rows with offers_education=Yes")
    args = parser.parse_args()

    target_year = args.year or compute_target_year()
    input_csv = input_csv_for_year(target_year)
    out_csv = output_education_csv(target_year)
    cache_file = cache_path_for_year(target_year)
    content_cache_file = content_cache_path_for_year(target_year)

    print(f"Provider: {provider or 'none installed'} | targetYear={target_year}")
    print(f"Input CSV: {input_csv}")

    if not os.path.exists(input_csv):
        print(f"[ERR] Input CSV missing: {input_csv}")
        return

    if DDGS is None:
        print("[ERR] DuckDuckGo client not installed. pip install ddgs requests")
        return

    # load CSV
    companies = []
    with open(input_csv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = row.get("company_name") or row.get("Company") or row.get("name")
            rank = row.get("rank") or ""
            if name:
                companies.append({"rank": rank, "company_name": name.strip()})

    if not companies:
        print("[ERR] No rows in CSV.")
        return

    # load caches
    cache = {}
    content_cache = {}
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as fh:
                cache = json.load(fh)
        except Exception:
            cache = {}
    if os.path.exists(content_cache_file):
        try:
            with open(content_cache_file, "r", encoding="utf-8") as fh:
                content_cache = json.load(fh)
        except Exception:
            content_cache = {}

    results = []
    for idx, comp in enumerate(companies, start=1):
        name = comp["company_name"]
        rank = comp.get("rank", "")
        print(f"[{idx}/{len(companies)}] {name}")
        try:
            info = find_education_for_company(name, target_year, cache, content_cache)
        except Exception as e:
            print(f"  [ERR] search failed: {e}")
            info = {"offers": "No", "link": "", "title": "", "score": 0, "reason": "error"}

        print(f"   -> Offers: {info['offers']} | score={info.get('score')} | link={info.get('link') or '(none)'} | reason={info.get('reason')}")
        row = {
            "rank": rank,
            "company_name": name,
            "offers_education": info["offers"],
            "detected_link": info.get("link", ""),
            "detected_title": info.get("title", ""),
            "score": info.get("score", 0),
            "reason": info.get("reason", "")
        }
        if args.only_yes and info["offers"] != "Yes":
            # skip
            pass
        else:
            results.append(row)

        # periodic cache flush
        if idx % 20 == 0:
            os.makedirs(output_dir_for_year(target_year), exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as fh:
                json.dump(cache, fh, ensure_ascii=False, indent=2)
            with open(content_cache_file, "w", encoding="utf-8") as fh:
                json.dump(content_cache, fh, ensure_ascii=False, indent=2)

        time.sleep(DELAY)

    # final persist
    os.makedirs(output_dir_for_year(target_year), exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, ensure_ascii=False, indent=2)
    with open(content_cache_file, "w", encoding="utf-8") as fh:
        json.dump(content_cache, fh, ensure_ascii=False, indent=2)

    # write CSV
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        fieldnames = ["rank","company_name","offers_education","detected_link","detected_title","score","reason"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"Saved enriched CSV -> {out_csv}")
    print(f"Saved cache -> {cache_file}")
    print(f"Saved content cache -> {content_cache_file}")

if __name__ == "__main__":
    main()
