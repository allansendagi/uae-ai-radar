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


def log(msg: str):
    line = f"[{datetime.now().isoformat(timespec='seconds')}] {msg}"
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
# SEARCH QUERIES
# ============================================================

NEWS_QUERIES = [
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
]

# CAIO22-style entity domains, reused for site-restricted career-page search.
ENTITY_CAREER_DOMAINS = [
    "digitaldubai.ae", "dewa.gov.ae", "dubaidet.ae", "dha.gov.ae",
    "dubaipolice.gov.ae", "rta.ae", "hbmsu.ac.ae", "dm.gov.ae",
    "dubaicustoms.gov.ae", "khda.gov.ae", "tdra.gov.ae", "dof.gov.ae",
]

JOB_BOARD_DOMAINS = ["bayt.com", "gulftalent.com", "naukrigulf.com", "indeed.com", "ae.indeed.com"]


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


def collect_news() -> list:
    log("Collecting news via Google Search...")
    # NOTE: countryCode="ae" was tested live and silently returns zero results —
    # it routes requests to google.ae, which appears to get bot-blocked. Default
    # (no countryCode, uses google.com) returns real, relevant results, since the
    # query terms themselves ("UAE", "Dubai") already do the geo-targeting.
    items = run_apify_actor(
        "apify~google-search-scraper",
        {
            "queries": "\n".join(NEWS_QUERIES),
            "maxPagesPerQuery": 1,
        },
    )
    results = []
    for page in items:
        for r in page.get("organicResults", []):
            results.append({
                "type": "news",
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


def collect_news_newsapi() -> list:
    """Second, independent news source — official API, not scraping. Additive to
    collect_news(), not a replacement: Google's scraped results still catch obscure
    government pages a news-specific index misses. Skips silently if no key is set."""
    if not NEWSAPI_KEY:
        return []
    log("Collecting news via NewsAPI.org...")
    results = []
    for q in NEWS_QUERIES:
        try:
            resp = requests.get(
                "https://newsapi.org/v2/everything",
                params={
                    "q": q,
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


def collect_job_boards() -> list:
    log("Collecting job board / career page postings via site-restricted search...")
    queries = []
    for domain in JOB_BOARD_DOMAINS:
        queries.append(f"site:{domain} AI Dubai OR UAE")
        queries.append(f"site:{domain} \"artificial intelligence\" jobs")
    for domain in ENTITY_CAREER_DOMAINS:
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
        for r in page.get("organicResults", []):
            results.append({
                "type": "job_posting",
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "snippet": r.get("description", ""),
                "source_query": page.get("searchQuery", {}).get("term", ""),
                "published_date": None,  # Google organicResults carry no date field
            })
    log(f"  {len(results)} raw job-board/career-page results")
    return results


def collect_linkedin_jobs() -> list:
    log("Collecting LinkedIn Jobs...")
    items = run_apify_actor(
        "curious_coder~linkedin-jobs-scraper",
        {
            "keywords": "artificial intelligence OR AI governance OR chief AI officer OR head of AI",
            "location": "United Arab Emirates",
            "datePosted": "past24Hours",  # actor's real enum: anyTime|past24Hours|pastWeek|pastMonth
            "limitPerSource": 30,
        },
    )
    results = []
    for r in items:
        results.append({
            "type": "job_posting",
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

CLASSIFY_PROMPT = """You are screening a batch of raw search results (news articles and job postings) for a UAE-focused AI governance consultancy called NOMOS. NOMOS sells AI governance verification (turning institutional policy into machine-executable, auditable artifacts) to UAE government entities and enterprises.

For EACH item below, decide:
1. is_relevant: true only if it's genuinely about AI in the UAE/Dubai/Abu Dhabi context (government AI, AI regulation, agentic AI, AI governance, AI deployments, AI investment, AI startups, AI infrastructure, or an AI-related job posting at a UAE entity). Discard generic/irrelevant results (unrelated jobs, non-UAE content, spam).
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
    date_str = date_str or TODAY
    path = BRIEFINGS_DIR / f"{date_str}.json"
    data = {
        "date": date_str,
        "provenance": provenance,
        "counts": counts,
        "items": items,
    }
    path.write_text(json.dumps(data, indent=2))
    log(f"Archived structured data to {path}")


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


def _day_html(day: dict, open_attr: str) -> str:
    date_str = day.get("date", "unknown")
    provenance = day.get("provenance", "automated")
    counts = day.get("counts", {}) or {}
    items = day.get("items", []) or []

    provenance_badge = ""
    if provenance != "automated":
        provenance_badge = f'<span class="badge" style="background:#8e44ad">MANUAL TEST DATA — {_esc(provenance)}</span>'

    count_bits = " &middot; ".join(
        f"{k}: {v}" for k, v in counts.items() if v is not None
    )

    if not items:
        body = '<p class="empty">No new items today — everything collected was already shown in a previous run.</p>'
    else:
        high = [i for i in items if i.get("nomos_relevance") == "High"]
        medium = [i for i in items if i.get("nomos_relevance") == "Medium"]
        low_count = sum(1 for i in items if i.get("nomos_relevance") == "Low")
        by_cat = {}
        for it in medium:
            by_cat.setdefault(it.get("category", "Other"), []).append(it)

        sections = []
        if high:
            sections.append('<h3 class="section-high">High NOMOS Relevance</h3>' + "".join(_item_html(i) for i in high))
        for cat in sorted(by_cat):
            sections.append(f'<h3>{_esc(cat)}</h3>' + "".join(_item_html(i) for i in by_cat[cat]))
        if low_count:
            sections.append(
                f'<p class="lowcount">{low_count} lower-relevance item{"s" if low_count != 1 else ""} '
                f'also collected today but not shown here — full data in the archived JSON.</p>'
            )
        body = "".join(sections)

    return f"""
  <details {open_attr}>
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


def render_index_html(days: list) -> str:
    if days:
        blocks = [_day_html(d, "open" if i == 0 else "") for i, d in enumerate(days)]
    else:
        blocks = ['<p class="empty">No briefings archived yet. Run the pipeline once (or add the seed test day) to populate this page.</p>']

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


# ============================================================
# MAIN
# ============================================================

def main():
    require_credentials()
    log("=== UAE AI Radar run starting ===")

    raw_items = []
    raw_items += collect_news()
    raw_items += collect_news_newsapi()
    raw_items += collect_job_boards()
    raw_items += collect_linkedin_jobs()

    if not raw_items:
        log("No items collected at all — likely an Apify credential/connectivity issue. Aborting without sending.")
        sys.exit(1)

    deduped = dedupe(raw_items)

    seen = load_seen()
    unseen = filter_unseen(deduped, seen)

    client = Anthropic(api_key=ANTHROPIC_API_KEY)
    # Classify in batches of 25 to keep prompts manageable.
    relevant = []
    for i in range(0, len(unseen), 25):
        relevant += classify_batch(client, unseen[i:i + 25])

    briefing = compose_briefing(relevant)
    archive_briefing(briefing)
    archive_briefing_json(relevant, {
        "raw": len(raw_items),
        "deduped": len(deduped),
        "unseen": len(unseen),
        "relevant": len(relevant),
    })
    send_email(briefing)
    build_index_html()

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
