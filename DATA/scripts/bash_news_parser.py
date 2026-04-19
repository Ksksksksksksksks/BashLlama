import argparse
import json
import re
import requests
from bs4 import BeautifulSoup
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse

from neftsity_parser import clean_article_text, clean_text_garbage

BASE = "https://bash.news"
LIST_PATH = "/bash/news"
LAST_PAGE = 2347  # default upper bound when no --range is passed

output_file = "bash_news_articles.jsonl"
num_threads = 8
max_articles = None


def listing_url(page):
    # page 1: /bash/news ; page 2+: /bash/news/page/N
    if page == 1:
        return f"{BASE}{LIST_PATH}"
    return f"{BASE}{LIST_PATH}/page/{page}"


def article_id_from_path(path):
    m = re.search(r"/bash/news/(\d+)-", path)
    return int(m.group(1)) if m else None


def normalize_article_url(u: str) -> str:
    # Dedup key: same article link with/without trailing slash
    s = (u or "").strip()
    if not s:
        return ""
    p = urlparse(s)
    scheme = (p.scheme or "https").lower()
    netloc = p.netloc.lower()
    path = (p.path or "").rstrip("/")
    if p.query:
        return f"{scheme}://{netloc}{path}?{p.query}"
    return f"{scheme}://{netloc}{path}"


def load_existing_urls_from_jsonl(path: str) -> set:
    seen = set()
    try:
        with open(path, encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                u = obj.get("url")
                if u:
                    seen.add(normalize_article_url(u))
    except FileNotFoundError:
        pass
    return seen


def expand_page_ranges(ranges):
    # ranges: list of (first, last) inclusive; order within pair may be reversed
    pages = set()
    for a, b in ranges:
        lo, hi = (a, b) if a <= b else (b, a)
        pages.update(range(lo, hi + 1))
    return sorted(pages)


# collect (id, url) from given listing pages (dedupe by article id within this run)
def collect_article_urls(pages):
    seen = set()
    out = []
    total = len(pages)
    for i, page in enumerate(pages, 1):
        url = listing_url(page)
        print(f"Listing page {page} ({i}/{total})")
        try:
            res = requests.get(url, timeout=30)
            res.raise_for_status()
        except Exception as e:
            print(f"Skip listing {page}: {e}")
            continue
        soup = BeautifulSoup(res.text, "lxml")
        for a in soup.select(".news-min__title a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("/bash/news/"):
                continue
            full = urljoin(BASE, href)
            parsed = urlparse(full)
            aid = article_id_from_path(parsed.path)
            if aid is None or aid in seen:
                continue
            seen.add(aid)
            out.append((aid, full))
        sleep(0.2)
    return out


def parse_article(url):
    aid = article_id_from_path(urlparse(url).path)
    if aid is None:
        return None
    try:
        res = requests.get(url, timeout=30)
        if res.status_code != 200:
            return None
        soup = BeautifulSoup(res.text, "lxml")
        art = soup.find("article", class_="news-article")
        if not art:
            return None

        h1 = art.select_one(".news-article__title h1")
        title = h1.get_text(strip=True) if h1 else ""

        date = ""
        t_el = art.select_one("time.news-article__time")
        if t_el:
            date = (t_el.get("data-time-utc") or t_el.get_text(strip=True) or "").strip()
        if not date:
            meta = art.find("meta", itemprop="datePublished")
            if meta and meta.get("content"):
                date = meta["content"].strip()

        paras = art.select(".news-article__content .content-block--paragraph p")
        chunks = [re.sub(r"[ \t]+", " ", p.get_text(" ", strip=True)).strip() for p in paras]
        chunks = [c for c in chunks if c]
        if chunks:
            text = " ".join(chunks)
        else:
            content = art.select_one(".news-article__content")
            text = (
                re.sub(r"[ \t]+", " ", content.get_text(" ", strip=True)).strip()
                if content
                else ""
            )

        return {"id": aid, "url": url, "date": date, "title": title, "text": text}
    except Exception as e:
        print(f"Error id={aid}: {e}")
        return None


def parse_args():
    p = argparse.ArgumentParser(
        description="Scrape bash.news listing pages and append articles to JSONL."
    )
    p.add_argument(
        "--range",
        nargs=2,
        type=int,
        action="append",
        metavar=("FIRST", "LAST"),
        help="Inclusive page range; repeat for multiple ranges (e.g. --range 1 50 --range 100 120)",
    )
    p.add_argument(
        "--first-page",
        type=int,
        default=None,
        metavar="N",
        help="Shorthand: single range from N to --last-page or default last",
    )
    p.add_argument(
        "--last-page",
        type=int,
        default=None,
        metavar="N",
        help="Shorthand: use with --first-page (or alone is ignored without --first-page)",
    )
    p.add_argument("-o", "--output", default=output_file, help="JSONL output path")
    p.add_argument("--threads", type=int, default=num_threads)
    p.add_argument("--max-articles", type=int, default=max_articles)
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Truncate output file before writing (default: append)",
    )
    return p.parse_args()


def resolve_ranges(args):
    ranges = []
    if args.range:
        ranges.extend(args.range)
    if args.first_page is not None:
        last = args.last_page if args.last_page is not None else LAST_PAGE
        ranges.append((args.first_page, last))
    if not ranges:
        ranges = [(1, LAST_PAGE)]
    return ranges


def main():
    args = parse_args()
    ranges = resolve_ranges(args)
    pages = expand_page_ranges(ranges)
    if not pages:
        print("No pages to fetch.")
        return

    print(f"Page ranges: {ranges} -> {len(pages)} unique listing pages")
    pairs = collect_article_urls(pages)
    print(f"Found article URLs: {len(pairs)}")

    mode = "w" if args.overwrite else "a"
    existing_urls = set() if args.overwrite else load_existing_urls_from_jsonl(args.output)
    if existing_urls:
        before = len(pairs)
        pairs = [(aid, url) for aid, url in pairs if normalize_article_url(url) not in existing_urls]
        print(f"Skip {before - len(pairs)} URLs already in {args.output} (by link)")

    saved = 0
    skipped_write = 0
    with open(args.output, mode, encoding="utf-8") as f:
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = {executor.submit(parse_article, url): aid for aid, url in pairs}
            for future in as_completed(futures):
                article = future.result()
                if article:
                    key = normalize_article_url(article.get("url", ""))
                    if key in existing_urls:
                        skipped_write += 1
                        continue
                    article = clean_article_text(article)
                    if article.get("date"):
                        article["date"] = clean_text_garbage(article["date"])
                    f.write(json.dumps(article, ensure_ascii=False) + "\n")
                    f.flush()
                    existing_urls.add(key)
                    saved += 1
                    print(f"Saved: {article['id']}")
                if args.max_articles and saved >= args.max_articles:
                    print("Stop by limit")
                    return
                sleep(0.05)
    if skipped_write:
        print(f"Skipped duplicates at write: {skipped_write}")


if __name__ == "__main__":
    main()
