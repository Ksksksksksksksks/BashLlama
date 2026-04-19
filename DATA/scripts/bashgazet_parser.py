"""
bashgazet.ru/articles — SPA + anti-bot redirect; needs Playwright for listing and articles.
Install: pip install playwright && python -m playwright install chromium
"""

import json
import re
from time import sleep
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from neftsity_parser import clean_article_text, clean_text_garbage

BASE = "https://bashgazet.ru"
LIST_URL = f"{BASE}/articles"

output_file = "bashgazet_articles.jsonl"
max_articles = None

# Stop scrolling only after this many rounds in a row with no new unique URLs (lazy feed needs time)
SCROLL_STABLE_ROUNDS = 20
MAX_SCROLL_ROUNDS = 500
# Pause after each wheel so the list can request the next chunk
SCROLL_PAUSE_MS = 1200
# Every N rounds jump to page bottom (infinite lists often key off viewport end)
SCROLL_DEEP_EVERY = 8
SCROLL_DEEP_EXTRA_MS = 1500

# Progress logs (avoid spamming every line)
SCROLL_LOG_EVERY = 30  # every N scroll rounds
SCROLL_LOG_URL_DELTA = 50  # also log when unique URL count grew by this much
FETCH_LOG_EVERY = 25  # every N successfully saved articles
SKIP_LOG_MAX = 5  # print at most this many individual skip lines

CONTEXT_OPTIONS = {
    "locale": "ru-RU",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def article_id_from_url(url):
    path = urlparse(url).path.rstrip("/")
    m = re.search(r"-(\d+)$", path.split("/")[-1])
    return int(m.group(1)) if m else None


def date_from_article_path(url):
    m = re.search(r"/articles/[^/]+/(\d{4}-\d{2}-\d{2})/", urlparse(url).path)
    return m.group(1) if m else ""


def _matter_item_count(page):
    # Same count as in the browser DOM (Vue may update async; locator.count can lag)
    return page.evaluate(
        """() => document.querySelectorAll('.matter-grid a.item[href^="/articles/"]').length"""
    )


def _click_all_matters_in_page(page):
    # Exact node: sibling under the page root, not a random menu duplicate
    return page.evaluate(
        """() => {
        const el = document.querySelector('div.all-matters');
        if (!el) return 'missing';
        const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
        if (t !== 'Все материалы') return 'wrong-text:' + t.slice(0, 40);
        el.scrollIntoView({ block: 'center', inline: 'nearest' });
        const r = el.getBoundingClientRect();
        const cx = Math.floor(r.left + Math.max(1, r.width) / 2);
        const cy = Math.floor(r.top + Math.max(1, r.height) / 2);
        const ptr = {
          bubbles: true, cancelable: true, composed: true,
          clientX: cx, clientY: cy, view: window,
          pointerId: 1, pointerType: 'mouse', isPrimary: true, button: 0, buttons: 1
        };
        const mouseDown = {
          bubbles: true, cancelable: true, composed: true,
          clientX: cx, clientY: cy, view: window, button: 0, buttons: 1, detail: 1
        };
        const mouseUp = {
          bubbles: true, cancelable: true, composed: true,
          clientX: cx, clientY: cy, view: window, button: 0, buttons: 0, detail: 1
        };
        try { el.dispatchEvent(new PointerEvent('pointerdown', ptr)); } catch (e) {}
        el.dispatchEvent(new MouseEvent('mousedown', mouseDown));
        try { el.dispatchEvent(new PointerEvent('pointerup', { ...ptr, buttons: 0 })); } catch (e) {}
        el.dispatchEvent(new MouseEvent('mouseup', mouseUp));
        el.dispatchEvent(new MouseEvent('click', mouseDown));
        if (typeof el.click === 'function') el.click();
        return 'ok';
    }"""
    )


def _wait_grid_grew(page, before, timeout_ms):
    page.wait_for_function(
        "prev => document.querySelectorAll('.matter-grid a.item[href^=\"/articles/\"]').length > prev",
        arg=before,
        timeout=timeout_ms,
    )


def _activate_full_matters_feed(page):
    # One batch (~20) until "Все материалы"; then Vue appends more <a.item> (button -> .inf)
    for _ in range(4):
        if page.locator("div.all-matters").count():
            break
        page.mouse.wheel(0, 900)
        page.wait_for_timeout(400)

    try:
        page.locator("div.all-matters").first.wait_for(state="visible", timeout=25000)
    except Exception:
        pass

    before = _matter_item_count(page)
    if before == 0:
        return

    dom_res = _click_all_matters_in_page(page)
    print(f"All-matters click (DOM): {dom_res}")

    try:
        _wait_grid_grew(page, before, 90000)
    except Exception as e:
        print(f"All-matters wait grid grow: {e!s:.200}")
        try:
            page.locator("div.all-matters").first.click(timeout=15000, force=True)
            _wait_grid_grew(page, before, 45000)
        except Exception as e2:
            print(f"All-matters Playwright click fallback: {e2!s:.200}")
            return

    after = _matter_item_count(page)
    print(f"All-matters: grid {before} -> {after} cards")
    page.wait_for_timeout(2800)
    try:
        page.wait_for_load_state("networkidle", timeout=45000)
    except Exception:
        pass
    page.evaluate(
        "() => window.scrollTo(0, document.documentElement.scrollHeight)"
    )
    page.wait_for_timeout(800)


def collect_urls_from_list_page(page):
    page.goto(LIST_URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(4000)
    try:
        page.wait_for_selector(".matter-grid", timeout=120000)
    except Exception:
        pass

    if page.locator(".matter-grid a.item").count() < 3:
        for _ in range(5):
            for sel in (
                "button",
                '[role="button"]',
                "a.btn",
                ".cookie button",
                "[class*='cookie'] button",
            ):
                loc = page.locator(sel)
                if loc.count() == 0:
                    continue
                try:
                    loc.first.click(timeout=2000)
                    page.wait_for_timeout(2500)
                    break
                except Exception:
                    continue
            if page.locator(".matter-grid a.item").count() >= 3:
                break

    # "Все материалы" may sit below the first viewport
    page.mouse.wheel(0, 1200)
    page.wait_for_timeout(600)
    _activate_full_matters_feed(page)

    urls = []
    seen = set()
    stable = 0
    last_n = 0
    last_logged_urls = 0
    round_i = 0
    stopped_stable = False
    print(
        f"[scroll] start — max {MAX_SCROLL_ROUNDS} rounds, "
        f"exit after {SCROLL_STABLE_ROUNDS} rounds with no new links"
    )
    for round_i in range(1, MAX_SCROLL_ROUNDS + 1):
        nodes = page.query_selector_all('.matter-grid a.item[href^="/articles/"]')
        for n in nodes:
            h = n.get_attribute("href")
            if not h or h in seen:
                continue
            seen.add(h)
            urls.append(urljoin(BASE, h))

        cur = len(urls)
        if round_i % SCROLL_LOG_EVERY == 0 or cur - last_logged_urls >= SCROLL_LOG_URL_DELTA:
            print(
                f"[scroll] round {round_i}/{MAX_SCROLL_ROUNDS}, "
                f"unique URLs: {cur}, stable streak: {stable}/{SCROLL_STABLE_ROUNDS}"
            )
            last_logged_urls = cur

        if cur == last_n:
            stable += 1
            if stable >= SCROLL_STABLE_ROUNDS:
                stopped_stable = True
                break
        else:
            stable = 0
            last_n = cur

        page.mouse.wheel(0, 4500)
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        if round_i % SCROLL_DEEP_EVERY == 0:
            page.evaluate(
                "() => window.scrollTo(0, document.documentElement.scrollHeight)"
            )
            page.wait_for_timeout(400)
            page.keyboard.press("End")
            page.wait_for_timeout(SCROLL_DEEP_EXTRA_MS)

    reason = "no_new_links" if stopped_stable else "max_rounds"
    print(f"[scroll] done — rounds: {round_i}, unique URLs: {len(urls)}, exit: {reason}")
    return urls


def parse_article_html(html, page_url):
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.select_one("div.mb-4 h1") or soup.select_one("h1.h1") or soup.find("h1")
    title = h1.get_text(" ", strip=True) if h1 else ""

    lead_el = soup.select_one("div.mb-4 h2") or soup.select_one("h2[class*='bigLead']")
    lead = lead_el.get_text(" ", strip=True) if lead_el else ""

    paras = []
    for block in soup.select('div[class*="_block_"] div.serif-text'):
        for p in block.find_all("p"):
            t = p.get_text(" ", strip=True)
            if t:
                paras.append(re.sub(r"[ \t]+", " ", t).strip())
    if not paras:
        for p in soup.select("div.serif-text p"):
            t = p.get_text(" ", strip=True)
            if t:
                paras.append(re.sub(r"[ \t]+", " ", t).strip())

    text = " ".join(paras)
    date = date_from_article_path(page_url)
    aid = article_id_from_url(page_url)
    if aid is None:
        return None
    return {"id": aid, "url": page_url, "date": date, "title": title, "text": text, "_lead": lead}


def fetch_article_page(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)
    try:
        page.wait_for_selector("div.mb-4 h1, h1.h1, h1", timeout=60000)
    except Exception:
        pass
    return parse_article_html(page.content(), url)


def strip_lead_from_text(article):
    lead = article.pop("_lead", "") or ""
    if not lead:
        return article
    t = article.get("text", "")
    if t.startswith(lead):
        article["text"] = t[len(lead) :].strip()
    elif lead.strip() and t.lstrip().startswith(lead.strip()):
        article["text"] = t.lstrip()[len(lead.strip()) :].strip()
    return article


def main():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(**CONTEXT_OPTIONS)
        list_page = context.new_page()

        print("[list] collecting URLs (open + scroll)…")
        urls = collect_urls_from_list_page(list_page)
        list_page.close()
        print(f"[list] queue size: {len(urls)} URLs")
        if not urls:
            browser.close()
            print("No links — check Playwright install and site.")
            return

        art_page = context.new_page()
        total = len(urls)
        saved = 0
        skipped = 0
        skip_lines = 0
        print(f"[fetch] start — {total} URLs → {output_file}")
        with open(output_file, "w", encoding="utf-8") as f:
            for url in urls:
                try:
                    article = fetch_article_page(art_page, url)
                    if not article or not article.get("title"):
                        skipped += 1
                        if skip_lines < SKIP_LOG_MAX:
                            tail = url if len(url) <= 100 else "…" + url[-97:]
                            print(f"[fetch] skip (no article/title): {tail}")
                            skip_lines += 1
                        continue
                    article = strip_lead_from_text(article)
                    article = clean_article_text(article)
                    if article.get("date"):
                        article["date"] = clean_text_garbage(article["date"])
                    f.write(json.dumps(article, ensure_ascii=False) + "\n")
                    saved += 1
                    if saved % FETCH_LOG_EVERY == 0 or saved == total:
                        print(f"[fetch] saved {saved}/{total} (last id {article['id']})")
                except Exception as e:
                    skipped += 1
                    print(f"[fetch] error: {e!s:.120} — {url[:80]}")
                sleep(0.08)
                if max_articles and saved >= max_articles:
                    print(f"[fetch] stop — max_articles={max_articles}")
                    break
        print(f"[fetch] done — saved {saved}, skipped/errors {skipped}, queued {total}")
        art_page.close()
        browser.close()


if __name__ == "__main__":
    main()
