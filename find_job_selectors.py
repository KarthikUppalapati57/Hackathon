# find_job_selectors.py
# This script visits each career page and uses heuristics to guess the job title selector.

import os
import csv
from datetime import datetime
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# --- Configuration ---
YEAR = datetime.now().year - 1
INPUT_CSV = os.path.join("output", str(YEAR), f"fortune500_{YEAR}_with_careers.csv")
OUTPUT_CSV = os.path.join("output", str(YEAR), f"fortune500_fully_enriched.csv")

# Heuristics: Common tags and class keywords for job titles
COMMON_TAGS = ['h1', 'h2', 'h3', 'h4', 'a', 'div', 'span', 'p']
COMMON_KEYWORDS = ['job', 'title', 'position', 'opening', 'listing', 'heading']

def find_best_selector(url):
    """
    Visits a URL with Selenium and tries to find the best CSS selector for job titles.
    """
    if not url:
        return None

    print(f"  -> Analyzing URL: {url}")
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.get(url)
        time.sleep(3) # Wait for basic JS rendering
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        candidate_selectors = {}
        for tag in COMMON_TAGS:
            for element in soup.find_all(tag):
                class_list = element.get('class', [])
                for keyword in COMMON_KEYWORDS:
                    if any(keyword in c.lower() for c in class_list):
                        # Found a potential candidate
                        selector = f"{tag}.{'.'.join(class_list)}"
                        candidate_selectors[selector] = candidate_selectors.get(selector, 0) + 1
        
        if not candidate_selectors:
            print("  -> No strong selector candidates found.")
            return None

        # Return the selector that was found the most times
        best_selector = max(candidate_selectors, key=candidate_selectors.get)
        print(f"  -> Best guess for selector: {best_selector}")
        return best_selector

    except Exception as e:
        print(f"  -> Error analyzing URL {url}: {e}")
        return None
    finally:
        driver.quit()

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"ERROR: Input file not found at {INPUT_CSV}.")
        return

    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        companies = list(reader)

    enriched_results = []
    for i, row in enumerate(companies, start=1):
        name = row.get("company_name")
        link = row.get("careers_link")
        print(f"[{i}/{len(companies)}] Processing: {name}")

        selector = find_best_selector(link)
        row['jobTitleSelector'] = selector
        enriched_results.append(row)
        time.sleep(1) # Be polite

    # Write the final, fully enriched CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["rank", "company_name", "careers_link", "jobTitleSelector"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_results)
    
    print(f"\nDone — wrote fully enriched data to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
