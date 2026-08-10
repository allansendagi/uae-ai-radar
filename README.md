# UAE AI Intelligence Radar

Runs daily at **6:47 AM**, independent of Claude Code or any browser session — a macOS `launchd` job triggers a standalone Python script. Pipeline: collect (UAE AI news + job postings across LinkedIn, Bayt, GulfTalent, NaukriGulf, and CAIO22-entity career pages) → dedupe → classify/score relevance via Claude → compose briefing → email it to you → archive a local copy → regenerate a browsable `index.html`.

**Status: installed and scheduled, but will not run successfully until you add credentials below.** The scheduler is already active (`launchctl load` has been run) — it just needs `.env` filled in.

## One-time setup — 3 credentials, all filled in by you directly

Claude does not see, handle, or enter these. Copy `.env.example` to `.env` in this same folder and fill in:

1. **`APIFY_API_TOKEN`** — from [console.apify.com/settings/integrations](https://console.apify.com/settings/integrations). This is the account already used for outreach sourcing this session, so if you have a token saved from that, it's the same one.
2. **`ANTHROPIC_API_KEY`** — from [console.anthropic.com/settings/keys](https://console.anthropic.com/settings/keys). Note: this is billed separately from your Claude subscription — it's pay-per-use API access. Cost is small (roughly a few cents per day at this volume) but real.
3. **`GMAIL_APP_PASSWORD`** — requires 2-Step Verification enabled on the Google account first, then generate at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords). This is **not** your normal Gmail password — Google blocks normal-password SMTP login for security.

## Test it manually before trusting the schedule

```
cd "/Users/allansendagi/Desktop/Creativity/active/uae-ai-radar"
python3 uae_ai_radar.py
```

Check `logs/` for what happened and `briefings/YYYY-MM-DD.md` for the output, and check your inbox. Fix any credential issues here before relying on the 6:47 AM automatic run.

## What it actually does each run

1. **Collect** — Apify's Google Search actor for ~19 UAE/Dubai AI news queries + site-restricted searches against Bayt, GulfTalent, NaukriGulf, and 12 CAIO22-entity career-page domains; plus a dedicated LinkedIn Jobs scraper for AI-titled roles in the UAE.
2. **Dedupe** — drops repeat URLs/titles within today's run.
3. **Filter against `seen_urls.json`** — drops anything already shown in a *previous* day's digest. This, not a Google date filter, is what keeps the digest from repeating itself. (Tested live: Google's own "past 24/48 hours" restriction starves this topic/geography down to almost nothing — most relevant pages don't carry a freshness signal Google will honor. Tracking what's already been shown gives a real "what's new since last time" filter without that problem.) First run will surface a full batch; every run after only shows genuinely new URLs.
4. **Classify** — one Claude call per batch: category, one-line "why it matters," a **High/Medium/Low** NOMOS-relevance flag with reasoning (deliberately not a numeric score — see the reasoning behind that call in this session's history), and a suggested contact role where applicable. Irrelevant results are dropped here, not just deprioritized.
5. **Compose** — High-relevance items surfaced first, everything else grouped by category underneath.
6. **Deliver** — emailed to `allansendagi@gmail.com`, and archived locally in `briefings/`. Every URL collected today (relevant or not) gets added to `seen_urls.json` so it never resurfaces.

## Browsing the archive — `index.html`

Every run writes `briefings/YYYY-MM-DD.json` (the structured data behind that day's email) and then regenerates `index.html` in this folder from *all* archived days — newest first, one collapsible section per day, same High/Medium/Low grouping and contact suggestions as the email. It's a single self-contained static file: no server, nothing to install, just open it —

```
open "/Users/allansendagi/Desktop/Creativity/active/uae-ai-radar/index.html"
```

Two things worth knowing about what the dates mean:

- **Each day's section is "new to us that day," not "published that day."** The collector doesn't get reliable publication dates back from search results — that's the same reason `seen_urls.json` exists instead of a date filter (see below). So the archive is arranged by *when the radar first surfaced it*, not by the article's actual publish date. The page says this explicitly under each day's header.
- **A day with zero items is expected, not a failure.** Once the seen-URL filter is warmed up, quiet days will show "No new items today" rather than being silently skipped — a missing date would look like the pipeline broke; an explicit empty state means it just didn't find anything new.

To rebuild the page without running the full pipeline (no Apify or Anthropic calls, free):

```
python3 uae_ai_radar.py --rebuild-index
```

The very first entry, `2026-08-04`, is seed/test data from the manual pipeline test done before the schedule went live — it's tagged with a purple "MANUAL TEST DATA" badge on the page and in its JSON's `provenance` field so it's never mistaken for a real automated run. Once real daily runs accumulate, that entry stays at the bottom of the list where it belongs chronologically.

## Known limitations (MVP, by design)

- No institutional memory yet — each day is independent; the system won't yet connect "Monday's story" to "Wednesday's follow-up," beyond simply not repeating the same URL. That's the deferred Option-B database layer, not built here.
- Relevance classification is a judgment call by Claude, not a certified score — treat "High" as "worth a human look," not "definitely act on this."
- If Apify or the classification step fails outright, the script logs the failure and does not send a broken/empty digest — check `logs/` if a day's email doesn't arrive.
- `seen_urls.json` grows forever with no pruning yet — fine for a long time at this volume, but worth revisiting eventually.

## Managing the schedule

```
# Turn off (stops future runs, keeps everything else intact)
launchctl unload ~/Library/LaunchAgents/com.allan.uaeairadar.plist

# Turn back on
launchctl load ~/Library/LaunchAgents/com.allan.uaeairadar.plist

# Change the time — edit com.allan.uaeairadar.plist's Hour/Minute, then:
cp com.allan.uaeairadar.plist ~/Library/LaunchAgents/com.allan.uaeairadar.plist
launchctl unload ~/Library/LaunchAgents/com.allan.uaeairadar.plist
launchctl load ~/Library/LaunchAgents/com.allan.uaeairadar.plist
```
