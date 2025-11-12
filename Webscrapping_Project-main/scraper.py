import requests, json, time, logging, configparser
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from pathlib import Path

# --- config ---
config = configparser.ConfigParser()
config.read("config.ini")
BASE_URL = config["SCRAPER"]["base_url"]
DELAY = float(config["SCRAPER"]["delay"])
MAX_RETRIES = int(config["SCRAPER"]["max_retries"])
HEADERS = {"User-Agent": config["SCRAPER"]["user_agent"]}
OUTPUT_DIR = Path(config["SCRAPER"]["output_dir"])
LOG_FILE = config["SCRAPER"]["log_file"]

# --- logging ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_page(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            time.sleep(DELAY)
    logging.error(f"Failed after {MAX_RETRIES} retries for {url}")
    return None

def parse_quotes(html, page_number):
    soup = BeautifulSoup(html, "html.parser")
    quotes = []
    for q in soup.select(".quote"):
        quotes.append({
            "quote_text": q.select_one(".text").get_text(strip=True),
            "author": q.select_one(".author").get_text(strip=True),
            "tags": [t.get_text(strip=True) for t in q.select(".tags .tag")],
            "page_number": page_number,
            "scraped_at": datetime.now(timezone.utc).isoformat()  # fixed
        })
    has_next = bool(soup.select_one(".next > a"))
    return quotes, has_next

def save_json(data, page_number):
    file_path = OUTPUT_DIR / f"quotes_page_{page_number}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    page = 1
    while True:
        url = BASE_URL.format(page)
        html = fetch_page(url)
        if not html:
            break
        quotes, has_next = parse_quotes(html, page)
        save_json(quotes, page)
        logging.info(f"Saved {len(quotes)} quotes from page {page}")
        if not has_next:
            break
        page += 1
        time.sleep(DELAY)
    logging.info("Scraping completed successfully.")

if __name__ == "__main__":
    main()
