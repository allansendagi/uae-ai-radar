#!/usr/bin/env python3
"""
UAE AI Intelligence Radar — daily collect, classify, and deliver.

Runs standalone (independent of any Claude session) via macOS launchd.
Pipeline: collect (news + jobs via Apify) -> dedupe -> relevance/classify
(Claude API) -> compose briefing -> email (Gmail SMTP) -> archive locally.

Credentials come from .env in this same folder (see .env.example).
"""

import os
import sys
import json
import smtplib
import hashlib
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

import requests
from dotenv import load_dotenv
from anthropic import Anthropic

HERE = Path(__file__).parent
load_dotenv(HERE / ".env")

APIFY_TOKEN = os.environ.get("APIFY_API_TOKEN")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD")
DIGEST_TO = os.environ.get("DIGEST_TO_EMAIL", GMAIL_ADDRESS)
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY")  # optional — second news source, not a replacement for Apify

BRIEFINGS_DIR = HERE / "briefings"
LOGS_DIR = HERE / "logs"
BRIEFINGS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

SEEN_FILE = HERE / "seen_urls.json"

TODAY = datetime.now().strftime("%Y-%m-%d")
LOG_FILE = LOGS_DIR / f"{TODAY}.log"


_LOG_LOCK = threading.Lock()


def log(msg: str):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
    with _LOG_LOCK:
        print(line)
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")


def require_credentials():
    missing = [
        name for name, val in [
            ("APIFY_API_TOKEN", APIFY_TOKEN),
            ("ANTHROPIC_API_KEY", ANTHROPIC_API_KEY),
            ("GMAIL_ADDRESS", GMAIL_ADDRESS),
            ("GMAIL_APP_PASSWORD", GMAIL_APP_PASSWORD),
        ] if not val
    ]
    if missing:
        log(f"FATAL: missing credentials in .env: {', '.join(missing)}")
        sys.exit(1)


# ============================================================
# MARKETS — UAE and Qatar run as fully parallel markets: own news
# queries, own government/entity domains, own LinkedIn location.
# Every collected item is tagged "market" so the site can split them
# into separate tabs. Job-board domains are GCC-wide and shared —
# only the query terms differ per market.
# ============================================================

JOB_BOARD_DOMAINS = ["bayt.com", "gulftalent.com", "naukrigulf.com", "indeed.com", "ae.indeed.com"]

# Restricts NewsAPI's /everything search to actual Gulf-region outlets. Without this,
# `q=` does loose full-text matching — "UAE" and "AI" only need to appear somewhere in
# an article, anywhere in NewsAPI's global index, so broad queries return mostly unrelated
# crypto/geopolitics/market-report noise (confirmed live 2026-08-12: 0/27 candidates were
# real UAE/Qatar AI news once Apify's more precise Google-Search channel was unavailable —
# the same query restricted to these domains went from 100+ noisy hits to a handful of
# genuine regional matches). Not a fix for Apify being down — a real precision floor for
# the NewsAPI fallback specifically, so a capped Apify account doesn't mean a silently
# noise-only (and therefore always-empty) day.
NEWS_DOMAINS = (
    "thenationalnews.com,khaleejtimes.com,gulfnews.com,zawya.com,arabianbusiness.com,"
    "gulf-times.com,thepeninsulaqatar.com,dohanews.co,wam.ae,gulfbusiness.com"
)

MARKETS = {
    "UAE": {
        "linkedin_location": "United Arab Emirates",
        "news_queries": [
            "UAE artificial intelligence",
            "Dubai artificial intelligence",
            "Abu Dhabi artificial intelligence",
            "UAE AI government",
            "Dubai AI government",
            "UAE AI regulation",
            "UAE autonomous AI agents",
            "UAE AI governance",
            "UAE agentic AI",
            "UAE AI investment",
            "DHA AI",
            "RTA AI",
            "DEWA AI",
            "Digital Dubai AI",
            "Dubai Municipality AI",
            "KHDA AI",
            "DGE AI",
            "TDRA AI",
            "UAE Ministry AI",
        ],
        # CAIO22-style entity domains, reused for site-restricted career-page search.
        "entity_domains": [
            "digitaldubai.ae", "dewa.gov.ae", "dubaidet.ae", "dha.gov.ae",
            "dubaipolice.gov.ae", "rta.ae", "hbmsu.ac.ae", "dm.gov.ae",
            "dubaicustoms.gov.ae", "khda.gov.ae", "tdra.gov.ae", "dof.gov.ae",
        ],
    },
    "Qatar": {
        "linkedin_location": "Qatar",
        "news_queries": [
            "Qatar artificial intelligence",
            "Doha artificial intelligence",
            "Qatar AI government",
            "Qatar AI regulation",
            "Qatar autonomous AI agents",
            "Qatar AI governance",
            "Qatar agentic AI",
            "Qatar AI investment",
            "MOI Qatar AI",
            "Qatar Central Bank AI",
            "Ashghal AI",
            "Kahramaa AI",
            "Hamad Medical AI",
            "Qatar Foundation AI",
            "MCIT Qatar AI",
            "Qatar Airways AI",
            "CRA Qatar AI",
            "QIA AI",
            "Qatar Ministry AI",
        ],
        "entity_domains": [
            "mcit.gov.qa", "qcb.gov.qa", "qfc.qa", "moi.gov.qa", "hamad.qa",
            "qf.org.qa", "qatarairways.com", "ashghal.gov.qa", "km.qa",
            "qia.qa", "cra.gov.qa", "motc.gov.qa",
        ],
    },
}


# ============================================================
# APIFY (standalone REST calls — no MCP session available here)
# ============================================================

def run_apify_actor(actor_id: str, input_payload: dict, timeout=240) -> list:
    """Runs an Apify actor synchronously and returns its dataset items."""
    url = f"https://api.apify.com/v2/acts/{actor_id}/run-sync-get-dataset-items"
    try:
        resp = requests.post(
            url,
            params={"token": APIFY_TOKEN},
            json=input_payload,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log(f"WARNING: Apify actor {actor_id} failed: {e}")
        return []


def collect_news(market: str, news_queries: list) -> list:
    log(f"[{market}] Collecting news via Google Search...")
    # NOTE: countryCode="ae" was tested live and silently returns zero results —
    # it routes requests to google.ae, which appears to get bot-blocked. Default
    # (no countryCode, uses google.com) returns real, relevant results, since the
    # query terms themselves ("UAE", "Dubai", "Qatar", "Doha") already do the
    # geo-targeting.
    items = run_apify_actor(
        "apify~google-search-scraper",
        {
            "queries": "\n".join(news_queries),
            "maxPagesPerQuery": 1,
        },
    )
    results = []
    for page in items:
        for r in page.get("organicResults", []):
            results.append({
                "type": "news",
                "market": market,
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("description", ""),
                "source_query": page.get("searchQuery", {}).get("term", ""),
                # No published_date: confirmed via a live raw-response check that Google's
                # organicResults carry no date field at all — not omitted by oversight.
                "published_date": None,
            })
    log(f"  {len(results)} raw news results")
    return results


def collect_news_newsapi(market: str, news_queries: list) -> list:
    """Second, independent news source — official API, not scraping. Additive to
    collect_news(), not a replacement: Google's scraped results still catch obscure
    government pages a news-specific index misses. Skips silently if no key is set."""
    if not NEWSAPI_KEY:
        return []
    log(f"[{market}] Collecting news via NewsAPI.org...")
    results = []
    for q in news_queries:
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": q,
                    "domains": NEWS_DOMAINS,
                    "apiKey": NEWSAPI_KEY,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 10,
                },
                timeout=30,
            )
            resp.raise_for_status()
            for a in resp.json().get("articles", []):
                published = a.get("publishedAt") or None  # real field, ISO 8601
                results.append({
                    "type": "news",
                    "market": market,
                    "title": a.get("title", "") or "",
                    "url": a.get("url", ""),
                    "snippet": a.get("description", "") or "",
                    "source_query": q,
                    "published_date": published[:10] if published else None,
                })
        except Exception as e:
            log(f"WARNING: NewsAPI query '{q}' failed: {e}")
    log(f"  {len(results)} raw news results from NewsAPI")
    return results


def collect_job_boards(market: str, entity_domains: list, location_terms: str) -> list:
    log(f"[{market}] Collecting job board / career page postings via site-restricted search...")
    queries = []
    for domain in JOB_BOARD_DOMAINS:
        queries.append(f"site:{domain} AI {location_terms}")
        queries.append(f"site:{domain} \"artificial intelligence\" jobs")
    for domain in entity_domains:
        queries.append(f"site:{domain} careers AI")
        queries.append(f"site:{domain} jobs \"artificial intelligence\"")

    items = run_apify_actor(
        "apify~google-search-scraper",
        {
            "queries": "\n".join(queries),
            "maxPagesPerQuery": 1,
        },
    )
    results = []
    for page in items:
        term = page.get("searchQuery", {}).get("term", "")
        # Root-cause fix, not a workaround: checked real results and confirmed
        # entity-career-domain queries (tdra.gov.ae, dof.gov.ae, etc.) don't
        # actually surface individual job listings — Google returns whatever
        # ranks on those low-traffic gov domains (PR pages, awards, even a bond
        # prospectus), not real postings. Dedicated job boards (bayt/gulftalent/
        # naukrigulf/indeed) do return genuine job content. Tag each accordingly
        # instead of mislabeling all of it "job_posting".
        is_real_job_board = any(d in term for d in JOB_BOARD_DOMAINS)
        for r in page.get("organicResults", []):
            results.append({
                "type": "job_posting" if is_real_job_board else "news",
                "market": market,
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("description", ""),
                "source_query": term,
                "published_date": None,  # Google organicResults carry no date field
            })
    log(f"  {len(results)} raw job-board/career-page results")
    return results


def collect_linkedin_jobs(market: str, linkedin_location: str) -> list:
    log(f"[{market}] Collecting LinkedIn Jobs...")
    items = run_apify_actor(
        "curious_coder~linkedin-jobs-scraper",
        {
            "keywords": "artificial intelligence OR AI governance OR chief AI officer OR head of AI",
            "location": linkedin_location,
            "datePosted": "past24Hours",  # actor's real enum: anyTime|past24Hours|pastWeek|pastMonth
            "limitPerSource": 30,
        },
    )
    results = []
    for r in items:
        results.append({
            "type": "job_posting",
            "market": market,
            "title": r.get("title", ""),
            "url": r.get("link") or r.get("jobUrl", ""),
            "snippet": f"{r.get('companyName', '')} — {r.get('location', '')}",
            "source_query": "linkedin_jobs",
            "published_date": r.get("postedAt") or None,  # real field, confirmed present (YYYY-MM-DD)
        })
    log(f"  {len(results)} raw LinkedIn job results")
    return results


# ============================================================
# DEDUPE
# ============================================================

def normalize_url(url: str) -> str:
    return re.sub(r"[?#].*$", "", url or "").rstrip("/").lower()


def dedupe(items: list) -> list:
    seen = set()
    out = []
    for it in items:
        key = normalize_url(it.get("url", "")) or hashlib.md5(
            it.get("title", "").lower().encode()
        ).hexdigest()
        if key in seen or not it.get("title"):
            continue
        seen.add(key)
        out.append(it)
    log(f"Deduped (within today's run): {len(items)} -> {len(out)}")
    return out


# ============================================================
# SEEN-BEFORE STORE (cross-day "what's new since last time")
# ============================================================
# Google's own date-restriction (tbs=qdr:d) was tested live and starves
# results for this topic/geography — most relevant pages don't carry a
# strong freshness signal Google will honor, even when the underlying
# content is genuinely new to us. Tracking what we've already shown
# across all previous runs gives a real "what's new" filter without that
# problem: Day 1 surfaces a healthy batch, Day 2+ only shows URLs never
# seen in any prior digest, so nothing repeats regardless of how Google
# dates the page.

def load_seen() -> dict:
    if SEEN_FILE.exists():
        try:
            return json.loads(SEEN_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_seen(seen: dict):
    SEEN_FILE.write_text(json.dumps(seen, indent=2))


def filter_unseen(items: list, seen: dict) -> list:
    out = [it for it in items if normalize_url(it.get("url", "")) not in seen]
    log(f"Filtered against {len(seen)} previously-seen URLs: {len(items)} -> {len(out)}")
    return out


def mark_seen(items: list, seen: dict):
    for it in items:
        key = normalize_url(it.get("url", ""))
        if key:
            seen[key] = TODAY


# ============================================================
# RELEVANCE + CLASSIFICATION (Claude API)
# ============================================================

CLASSIFY_PROMPT = """You are screening a batch of raw search results (news articles and job postings) for a UAE- and Qatar-focused AI governance consultancy called NOMOS. NOMOS sells AI governance verification (turning institutional policy into machine-executable, auditable artifacts) to UAE and Qatar government entities and enterprises.

For EACH item below, decide:
1. is_relevant: true only if it's genuinely about AI in the UAE/Dubai/Abu Dhabi or Qatar/Doha context (government AI, AI regulation, agentic AI, AI governance, AI deployments, AI investment, AI startups, AI infrastructure, or an AI-related job posting at a UAE or Qatar entity). Discard generic/irrelevant results (unrelated jobs, non-UAE/non-Qatar content, spam).
2. category: one of [Government AI, Regulation/Policy, Agentic AI, AI Governance, AI Deployment, AI Investment, AI Startups, AI Infrastructure, Job Posting]
3. why_it_matters: one sentence, concrete, no fluff.
4. nomos_relevance: "High", "Medium", or "Low" — plus a one-line reason. High = a real, specific opening for a governance/verification pitch (new autonomous decision-making system, new AI-related executive hire at a named entity, new regulation creating an audit/accountability requirement). Do not inflate — most items should be Medium or Low.
5. suggested_contact: if a specific organisation or role is named, suggest who at that org would be the right NOMOS contact (role/title only, not a fabricated name). Null if not applicable.

Be honest and conservative — this digest is only useful if the "High" relevance flags are genuinely rare and correct, not everything tagged high to seem important.

Return ONLY a JSON array, one object per input item, in the same order, with fields: is_relevant, category, why_it_matters, nomos_relevance, nomos_reason, suggested_contact. No text before or after the array, no explanation, no markdown fence.

ITEMS:
{items_json}
"""


def classify_batch(client: Anthropic, items: list) -> list:
    if not items:
        return []
    batch_input = [
        {"title": it["title"], "snippet": it["snippet"], "url": it["url"], "type": it["type"]}
        for it in items
    ]
    # Persisted BEFORE classification so a zero/low-relevant day is auditable after the
    # fact — without this, there was no way to tell "genuinely no relevant news" apart
    # from "the classifier silently dropped items" (found 2026-08-12: a zero-relevant
    # day left nothing to inspect once the run finished).
    with open(LOGS_DIR / f"{TODAY}_candidates.jsonl", "a") as f:
        for it in batch_input:
            f.write(json.dumps(it) + "\n")
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4000,
            messages=[{
                "role": "user",
                "content": CLASSIFY_PROMPT.format(items_json=json.dumps(batch_input, indent=2)),
            }],
        )
        text = resp.content[0].text.strip()
        # Extract just the JSON array — tolerate any prose/fences/trailing notes
        # the model adds around it rather than assuming the whole response is pure JSON.
        start, end = text.find("["), text.rfind("]")
        if start == -1 or end == -1 or end < start:
            raise ValueError(f"No JSON array found in response: {text[:200]!r}")
        classifications = json.loads(text[start:end + 1])
    except Exception as e:
        log(f"WARNING: classification failed: {e}")
        return []

    if len(classifications) != len(items):
        # zip() would otherwise silently pair only as many as the shorter list and drop
        # the rest with no error at all — e.g. a response truncated at max_tokens still
        # parses as valid (partial) JSON, so the try/except above wouldn't catch it.
        log(f"WARNING: classification returned {len(classifications)} results for "
            f"{len(items)} items — likely truncated. Items beyond the response length "
            f"were NOT evaluated and are being treated as not-relevant this run.")

    enriched = []
    for it, cls in zip(items, classifications):
        if cls.get("is_relevant"):
            enriched.append({**it, **cls})
    log(f"Classified: {len(items)} -> {len(enriched)} relevant")
    return enriched


# ============================================================
# COMPOSE BRIEFING
# ============================================================

RELEVANCE_ORDER = {"High": 0, "Medium": 1, "Low": 2}


def compose_briefing(items: list) -> str:
    date_str = datetime.now().strftime("%d %B %Y")
    items_sorted = sorted(items, key=lambda x: RELEVANCE_ORDER.get(x.get("nomos_relevance", "Low"), 3))

    lines = [f"# UAE AI Intelligence — {date_str}", ""]

    high = [i for i in items_sorted if i.get("nomos_relevance") == "High"]
    if high:
        lines.append("## 🔥 High NOMOS Relevance")
        lines.append("")
        for it in high:
            lines.append(f"### {it['title']}")
            lines.append(f"[{it['url']}]({it['url']})")
            lines.append("")
            lines.append(f"**Why it matters:** {it['why_it_matters']}")
            lines.append(f"**NOMOS relevance:** High — {it.get('nomos_reason', '')}")
            if it.get("suggested_contact"):
                lines.append(f"**Suggested contact:** {it['suggested_contact']}")
            lines.append(f"**Category:** {it['category']}")
            lines.append("")

    by_category = {}
    for it in items_sorted:
        if it.get("nomos_relevance") == "High":
            continue
        by_category.setdefault(it["category"], []).append(it)

    for cat, its in sorted(by_category.items()):
        lines.append(f"## {cat}")
        lines.append("")
        for it in its:
            lines.append(f"- **{it['title']}** — {it['why_it_matters']} "
                         f"([link]({it['url']})) — NOMOS relevance: {it.get('nomos_relevance', 'Low')}"
                         + (f" — contact: {it['suggested_contact']}" if it.get("suggested_contact") else ""))
        lines.append("")

    if not items:
        lines.append("_No relevant items found today._")

    return "\n".join(lines)


def compose_email_summary(items: list) -> str:
    """Short digest for the inbox — full detail lives in the archived .md/.json
    and the website, which is what compose_briefing() is for. This is deliberately
    terse: an inbox isn't the right place for 250 dense entries."""
    date_str = datetime.now().strftime("%d %B %Y")
    high = [i for i in items if i.get("nomos_relevance") == "High"]
    medium = [i for i in items if i.get("nomos_relevance") == "Medium"]
    low = [i for i in items if i.get("nomos_relevance") == "Low"]
    news_count = sum(1 for i in items if i.get("type") != "job_posting")
    job_count = sum(1 for i in items if i.get("type") == "job_posting")

    lines = [
        f"UAE AI Intelligence — {date_str}",
        "",
        f"{len(items)} new items today ({news_count} news, {job_count} jobs) — "
        f"{len(high)} High, {len(medium)} Medium, {len(low)} lower relevance.",
        "",
    ]

    if not items:
        lines.append("Nothing new today — everything collected was already shown in a previous run.")
        return "\n".join(lines)

    if high:
        lines.append(f"HIGH RELEVANCE ({len(high)})")
        for it in high:
            lines.append(f"- {it['title']} — {it['why_it_matters']}")
            lines.append(f"  {it['url']}")
        lines.append("")

    if medium:
        lines.append(f"MEDIUM RELEVANCE ({len(medium)}) — titles only, full detail on the website:")
        for it in medium:
            lines.append(f"- {it['title']}")
        lines.append("")

    if low:
        lines.append(f"{len(low)} lower-relevance items also collected — not listed here, see the website for the full archive.")
        lines.append("")

    lines.append("Full detail, jobs tab, and search: see index.html (local) or the hosted site once Vercel is live.")

    return "\n".join(lines)


# ============================================================
# DELIVER
# ============================================================

def send_email(markdown_body: str):
    date_str = datetime.now().strftime("%d %b %Y")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"UAE AI Intelligence — {date_str}"
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = DIGEST_TO

    text_part = MIMEText(markdown_body, "plain")
    msg.attach(text_part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, [DIGEST_TO], msg.as_string())
        log(f"Email sent to {DIGEST_TO}")
    except Exception as e:
        log(f"ERROR: failed to send email: {e}")


def archive_briefing(markdown_body: str):
    path = BRIEFINGS_DIR / f"{TODAY}.md"
    path.write_text(markdown_body)
    log(f"Archived to {path}")


def archive_briefing_json(items: list, counts: dict, provenance: str = "automated", date_str: str = None):
    """Merges into any existing archive for the same date instead of overwriting
    it. Running the pipeline twice in one day (a retry, a manual re-test) used to
    silently destroy the first run's data — real items were lost this way once
    already. Dedup by URL; counts are summed, not replaced."""
    date_str = date_str or TODAY
    path = BRIEFINGS_DIR / f"{date_str}.json"

    existing_items, existing_counts = [], {}
    if path.exists():
        try:
            prior = json.loads(path.read_text())
            existing_items = prior.get("items", []) or []
            existing_counts = prior.get("counts", {}) or {}
        except Exception as e:
            log(f"WARNING: could not read existing archive to merge, overwriting: {e}")

    seen_urls = set()
    merged_items = []
    for it in items + existing_items:
        u = it.get("url", "")
        if u and u in seen_urls:
            continue
        if u:
            seen_urls.add(u)
        merged_items.append(it)

    merged_counts = {
        k: (counts.get(k) or 0) + (existing_counts.get(k) or 0)
        for k in set(counts) | set(existing_counts)
        if k != "relevant"
    }
    merged_counts["relevant"] = len(merged_items)

    data = {
        "date": date_str,
        "provenance": provenance,
        "counts": merged_counts,
        "items": merged_items,
    }
    path.write_text(json.dumps(data, indent=2))
    log(f"Archived structured data to {path} ({len(merged_items)} total after merge)")


# ============================================================
# FRONTEND (static, self-contained, regenerated from archived JSON —
# no server, no build step, opens directly as a file in any browser)
# ============================================================

RELEVANCE_COLOR = {"High": "#c0392b", "Medium": "#b8860b", "Low": "#6b7280"}


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _item_html(it: dict) -> str:
    rel = it.get("nomos_relevance", "Low")
    color = RELEVANCE_COLOR.get(rel, "#6b7280")
    contact = f' &middot; <span class="contact">contact: {_esc(it["suggested_contact"])}</span>' if it.get("suggested_contact") else ""
    reason = it.get("nomos_reason", "")
    pub_date = it.get("published_date")
    # Real date only where the source actually provides one (LinkedIn jobs, NewsAPI) —
    # Google-search-sourced items have no date field, so no line is shown for those
    # rather than fabricating one.
    date_bit = f' &middot; <span class="pubdate">{_esc(pub_date)}</span>' if pub_date else ""
    return f"""
    <div class="item">
      <div class="item-title"><a href="{_esc(it.get('url',''))}" target="_blank" rel="noopener">{_esc(it.get('title',''))}</a></div>
      <div class="item-meta">
        <span class="badge" style="background:{color}">{_esc(rel)}</span>
        <span class="category">{_esc(it.get('category',''))}</span>{contact}{date_bit}
      </div>
      <div class="why">{_esc(it.get('why_it_matters',''))}</div>
      {f'<div class="reason">{_esc(reason)}</div>' if reason else ''}
    </div>"""


def _render_group(group_items: list, empty_msg: str) -> str:
    high = [i for i in group_items if i.get("nomos_relevance") == "High"]
    medium = [i for i in group_items if i.get("nomos_relevance") == "Medium"]
    low_count = sum(1 for i in group_items if i.get("nomos_relevance") == "Low")
    by_cat = {}
    for it in medium:
        by_cat.setdefault(it.get("category", "Other"), []).append(it)

    if not high and not by_cat and not low_count:
        return f'<p class="empty">{empty_msg}</p>'

    sections = []
    if high:
        sections.append('<h3 class="section-high">High NOMOS Relevance</h3>' + "".join(_item_html(i) for i in high))
    for cat in sorted(by_cat):
        sections.append(f'<h3>{_esc(cat)}</h3>' + "".join(_item_html(i) for i in by_cat[cat]))
    if low_count:
        sections.append(
            f'<p class="lowcount">{low_count} lower-relevance item{"s" if low_count != 1 else ""} '
            f'also collected but not shown here — full data in the archived JSON.</p>'
        )
    return "".join(sections)


def _market_items(day: dict, market: str) -> list:
    """Items missing a 'market' field are from before markets existed (UAE-only
    era) — default them to UAE rather than dropping them, so old archives don't
    silently vanish from the site."""
    return [i for i in (day.get("items") or []) if i.get("market", "UAE") == market]


def _day_html(day: dict, open_attr: str, market: str) -> str:
    """News only — jobs live in the separate persistent Jobs board, not nested
    per-day, since a job posting shouldn't get buried under a new day's block
    the moment it's no longer 'new'. News is naturally a daily feed; jobs are
    naturally a standing board."""
    date_str = day.get("date", "unknown")
    provenance = day.get("provenance", "automated")
    counts = day.get("counts", {}) or {}
    market_items = _market_items(day, market)
    news_items = [i for i in market_items if i.get("type") != "job_posting"]

    provenance_badge = ""
    if provenance != "automated":
        provenance_badge = f'<span class="badge" style="background:#8e44ad">MANUAL TEST DATA — {_esc(provenance)}</span>'

    count_bits = " &middot; ".join(
        f"{k}: {v}" for k, v in counts.items() if v is not None
    )

    body = _render_group(news_items, "No new news today — everything collected was already shown in a previous run.")
    anchor_id = f"day-{market.lower()}-{re.sub(r'[^a-zA-Z0-9]', '', date_str)}"

    return f"""
  <details {open_attr} id="{anchor_id}">
    <summary>
      <span class="date">{_esc(date_str)}</span>
      <span class="summary-meta">{count_bits}</span>
      {provenance_badge}
    </summary>
    <div class="day-body">
      <p class="dateflag">Arranged by the day each item was first seen (new-to-us), not by original publication date.</p>
      {body}
    </div>
  </details>"""


def _jobs_board_html(days: list, market: str) -> str:
    """Persistent, cross-day jobs board — every job ever collected, deduped by
    URL, until it's explicitly worth pruning. Unlike news, a job posting stays
    relevant as long as it's open, so it shouldn't disappear into a collapsed
    day-block the day after it's first seen. Grouped by date — real
    published_date where the source has one (LinkedIn, NewsAPI), otherwise the
    date we first saw it — both to show *when*, and as the filter: collapsed
    date sections double as a way to jump to/skip a given day without JS.
    """
    seen_urls = set()
    by_date = {}
    for day in days:  # days is newest-first
        for it in _market_items(day, market):
            if it.get("type") != "job_posting":
                continue
            url = it.get("url", "")
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            display_date = it.get("published_date") or day.get("date", "unknown")
            by_date.setdefault(display_date, []).append(it)

    if not by_date:
        return '<p class="empty">No jobs collected yet.</p>'

    sections = []
    for date_str in sorted(by_date, reverse=True):
        jobs = by_date[date_str]
        sections.append(f"""
      <details class="date-filter" open>
        <summary><span class="date">{_esc(date_str)}</span> <span class="summary-meta">{len(jobs)} job{"s" if len(jobs) != 1 else ""}</span></summary>
        <div class="day-body">{_render_group(jobs, "Nothing here.")}</div>
      </details>""")
    return "".join(sections)


def _market_tab_html(days: list, market: str) -> str:
    """News | Jobs sub-tabs for one market (UAE or Qatar)."""
    m = market.lower()
    news_blocks = [_day_html(d, "open" if i == 0 else "", market) for i, d in enumerate(days)]
    jobs_block = _jobs_board_html(days, market)
    total_jobs = len({
        it.get("url") for d in days for it in _market_items(d, market)
        if it.get("type") == "job_posting"
    })
    date_picker = "".join(
        f'<a href="#day-{m}-{re.sub(r"[^a-zA-Z0-9]", "", d.get("date",""))}" class="date-pill">{_esc(d.get("date",""))}</a>'
        for d in days
    )
    return f"""
  <div class="toptabs">
    <input type="radio" name="toptabs-{m}" id="toptab-news-{m}" class="tab-input" checked>
    <label for="toptab-news-{m}" class="tab-label">News</label>
    <input type="radio" name="toptabs-{m}" id="toptab-jobs-{m}" class="tab-input">
    <label for="toptab-jobs-{m}" class="tab-label">Jobs ({total_jobs})</label>
    <div class="tab-panel">
      <div class="date-picker">Jump to: {date_picker}</div>
      {"".join(news_blocks)}
    </div>
    <div class="tab-panel"><p class="dateflag">Every job ever collected, newest first, until seen — not reset daily.</p>{jobs_block}</div>
  </div>"""


def render_index_html(days: list) -> str:
    if days:
        blocks = f"""
  <div class="markettabs">
    <input type="radio" name="markettabs" id="markettab-uae" class="market-input" checked>
    <label for="markettab-uae" class="market-label">UAE</label>
    <input type="radio" name="markettabs" id="markettab-qatar" class="market-input">
    <label for="markettab-qatar" class="market-label">Qatar</label>
    <div class="market-panel">{_market_tab_html(days, "UAE")}</div>
    <div class="market-panel">{_market_tab_html(days, "Qatar")}</div>
  </div>"""
    else:
        blocks = '<p class="empty">No briefings archived yet. Run the pipeline once (or add the seed test day) to populate this page.</p>'

    generated = datetime.now().strftime("%d %b %Y, %H:%M")

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>UAE AI Intelligence Radar</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 820px; margin: 0 auto; padding: 32px 20px 80px; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 22px; margin-bottom: 2px; }}
  .subtitle {{ color: #666; font-size: 13px; margin-top: 0; margin-bottom: 28px; }}
  details {{ background: #fff; border: 1px solid #e2e2e2; border-radius: 8px; margin-bottom: 12px; padding: 0 16px; }}
  summary {{ cursor: pointer; padding: 14px 0; font-weight: 600; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; list-style: none; }}
  summary::-webkit-details-marker {{ display: none; }}
  summary::before {{ content: "▸"; color: #999; }}
  details[open] summary::before {{ content: "▾"; }}
  .date {{ font-size: 15px; }}
  .summary-meta {{ font-weight: 400; color: #888; font-size: 12px; }}
  .day-body {{ padding: 4px 0 18px; border-top: 1px solid #eee; }}
  .dateflag {{ font-size: 11px; color: #999; font-style: italic; margin: 10px 0 16px; }}
  h3 {{ font-size: 13px; text-transform: uppercase; letter-spacing: 0.04em; color: #555; margin: 20px 0 8px; }}
  h3.section-high {{ color: #c0392b; }}
  .item {{ padding: 10px 0; border-bottom: 1px solid #f0f0f0; }}
  .item:last-child {{ border-bottom: none; }}
  .item-title a {{ font-weight: 600; color: #14213d; text-decoration: none; }}
  .item-title a:hover {{ text-decoration: underline; }}
  .item-meta {{ font-size: 11px; margin: 4px 0; color: #777; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
  .badge {{ color: #fff; font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 10px; letter-spacing: 0.03em; }}
  .category {{ text-transform: uppercase; font-size: 10px; letter-spacing: 0.03em; }}
  .contact {{ color: #555; }}
  .pubdate {{ color: #999; }}
  .why {{ font-size: 13px; margin-top: 4px; }}
  .reason {{ font-size: 12px; color: #888; margin-top: 2px; }}
  .empty {{ color: #888; font-style: italic; padding: 16px 0; }}
  .lowcount {{ font-size: 11px; color: #999; font-style: italic; margin-top: 16px; }}
  .date-picker {{ font-size: 12px; color: #888; margin: 4px 0 16px; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }}
  .date-pill {{ display: inline-block; padding: 3px 10px; border-radius: 12px; background: #f0f0f0; color: #14213d; text-decoration: none; font-size: 12px; font-weight: 600; }}
  .date-pill:hover {{ background: #14213d; color: #fff; }}
  details:target {{ outline: 2px solid #14213d; outline-offset: 4px; }}
  .markettabs {{ margin-bottom: 8px; }}
  .markettabs input.market-input {{ display: none; }}
  .markettabs label.market-label {{ display: inline-block; padding: 10px 28px; margin-right: 6px; border-radius: 8px 8px 0 0; background: #e8e4da; color: #555; font-size: 15px; font-weight: 800; letter-spacing: 0.02em; cursor: pointer; }}
  .markettabs input.market-input:first-of-type:checked ~ label.market-label:first-of-type,
  .markettabs input.market-input:last-of-type:checked ~ label.market-label:last-of-type {{ background: #b8451f; color: #fff; }}
  .markettabs .market-panel {{ display: none; }}
  .markettabs input.market-input:first-of-type:checked ~ .market-panel:first-of-type,
  .markettabs input.market-input:last-of-type:checked ~ .market-panel:last-of-type {{ display: block; }}
  .toptabs {{ margin-top: 4px; }}
  .toptabs input.tab-input {{ display: none; }}
  .toptabs label.tab-label {{ display: inline-block; padding: 8px 20px; margin-right: 4px; border-radius: 6px 6px 0 0; background: #eee; color: #666; font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.03em; cursor: pointer; }}
  .toptabs input.tab-input:first-of-type:checked ~ label.tab-label:first-of-type,
  .toptabs input.tab-input:last-of-type:checked ~ label.tab-label:last-of-type {{ background: #14213d; color: #fff; }}
  .toptabs .tab-panel {{ display: none; border-top: 3px solid #14213d; padding-top: 16px; }}
  .toptabs input.tab-input:first-of-type:checked ~ .tab-panel:first-of-type,
  .toptabs input.tab-input:last-of-type:checked ~ .tab-panel:last-of-type {{ display: block; }}
  details.date-filter {{ background: transparent; border: none; padding: 0 0 0 4px; margin-bottom: 8px; }}
  details.date-filter summary {{ padding: 8px 0; font-size: 13px; }}
  details.date-filter .day-body {{ padding-left: 8px; border-top: none; }}
  footer {{ margin-top: 30px; font-size: 11px; color: #aaa; text-align: center; }}
</style>
</head>
<body>
  <h1>UAE AI Intelligence Radar</h1>
  <p class="subtitle">Daily archive, newest first. Regenerated automatically after each run.</p>
  {"".join(blocks)}
  <footer>Generated {generated}</footer>
</body>
</html>"""


def build_index_html():
    log("Building index.html from all archived briefings...")
    json_files = sorted(BRIEFINGS_DIR.glob("*.json"), reverse=True)
    days = []
    for jf in json_files:
        try:
            days.append(json.loads(jf.read_text()))
        except Exception as e:
            log(f"WARNING: failed to parse {jf}: {e}")
    html = render_index_html(days)
    out_path = HERE / "index.html"
    out_path.write_text(html)
    log(f"Wrote {out_path} ({len(days)} day(s))")
    return out_path


def push_to_github():
    """Commits the regenerated site + archives and pushes, so the Vercel-hosted
    copy actually updates daily instead of only reflecting the last manual push.
    Non-fatal by design: a git/network failure here shouldn't take down email
    delivery or the local site, which have already succeeded by this point."""
    import subprocess

    def run(*args):
        return subprocess.run(args, cwd=HERE, capture_output=True, text=True)

    if not (HERE / ".git").exists():
        log("No .git directory — skipping push (repo not initialized here).")
        return

    try:
        run("git", "add", "index.html", "briefings/", "seen_urls.json")
        status = run("git", "status", "--porcelain")
        if not status.stdout.strip():
            log("No changes to push today.")
            return
        commit = run("git", "commit", "-m", f"Daily update — {TODAY}")
        if commit.returncode != 0:
            log(f"WARNING: git commit failed: {commit.stderr.strip()}")
            return
        push = run("git", "push")
        if push.returncode != 0:
            log(f"WARNING: git push failed: {push.stderr.strip()}")
        else:
            log("Pushed daily update to GitHub.")
    except Exception as e:
        log(f"WARNING: push_to_github failed: {e}")


# ============================================================
# MAIN
# ============================================================

def main():
    require_credentials()
    log("=== UAE AI Radar run starting ===")

    # All 8 collection calls (4 sources x 2 markets) are independent network I/O —
    # run them concurrently instead of sequentially. Sequential was taking ~20min
    # once Qatar doubled the source count; concurrent brings it back down to
    # roughly the time of the single slowest call (~90s-2min), not the sum of all 8.
    raw_items = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for market, cfg in MARKETS.items():
            futures.append(pool.submit(collect_news, market, cfg["news_queries"]))
            futures.append(pool.submit(collect_news_newsapi, market, cfg["news_queries"]))
            futures.append(pool.submit(collect_job_boards, market, cfg["entity_domains"], market))
            futures.append(pool.submit(collect_linkedin_jobs, market, cfg["linkedin_location"]))
        for future in as_completed(futures):
            try:
                raw_items += future.result()
            except Exception as e:
                log(f"WARNING: a collection task raised an exception: {e}")

    if not raw_items:
        log("No items collected at all — likely an Apify credential/connectivity issue. Aborting without sending.")
        sys.exit(1)

    deduped = dedupe(raw_items)

    seen = load_seen()
    unseen = filter_unseen(deduped, seen)

    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    # Classify in batches of 25, run concurrently — same rationale as collection:
    # independent network calls, no reason to wait on them one at a time. Capped
    # at 5 concurrent to stay well clear of API rate limits.
    batches = [unseen[i:i + 25] for i in range(0, len(unseen), 25)]
    relevant = []
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(classify_batch, client, b) for b in batches]
        for future in as_completed(futures):
            try:
                relevant += future.result()
            except Exception as e:
                log(f"WARNING: a classification batch raised an exception: {e}")

    briefing = compose_briefing(relevant)
    archive_briefing(briefing)
    archive_briefing_json(relevant, {
        "raw": len(raw_items),
        "deduped": len(deduped),
        "unseen": len(unseen),
        "relevant": len(relevant),
    })
    send_email(compose_email_summary(relevant))
    build_index_html()
    push_to_github()

    # Mark EVERYTHING collected today as seen (not just what was relevant) —
    # an irrelevant result shouldn't keep resurfacing for reclassification either.
    mark_seen(deduped, seen)
    save_seen(seen)

    log(f"=== Run complete: {len(raw_items)} raw -> {len(deduped)} deduped -> "
        f"{len(unseen)} new (not seen before) -> {len(relevant)} relevant ===")


if __name__ == "__main__":
    if "--rebuild-index" in sys.argv:
        build_index_html()
    else:
        main()
