"""
test_sources.py — Probe alternative stat sources for Roch Cholowsky (UCLA).
Prints HTTP status, page title, and a body snippet for each URL tested.
No DB, no integration — diagnostic only.

Usage:
    python test_sources.py
"""

import textwrap
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta

yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
PLAYER = "Roch Cholowsky (UCLA)"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
})


def probe(label: str, url: str, note: str = "", **kwargs):
    print(f"\n{'='*65}")
    print(f"SOURCE : {label}")
    print(f"URL    : {url}")
    if note:
        print(f"NOTE   : {note}")
    print("-" * 65)
    try:
        r = SESSION.get(url, timeout=20, **kwargs)
        soup = BeautifulSoup(r.text, "html.parser")
        title = soup.find("title")
        h1 = soup.find("h1")
        tables = soup.find_all("table")
        table_ids = [t.get("id", "(no id)") for t in tables]

        print(f"STATUS : {r.status_code}")
        print(f"TITLE  : {title.text.strip()[:80] if title else 'none'}")
        print(f"H1     : {h1.text.strip()[:80] if h1 else 'none'}")
        print(f"TABLES : {len(tables)} — {table_ids}")

        snippet = r.text.replace("\n", " ").replace("\r", "")
        print("BODY   :", textwrap.fill(snippet[:500], width=80))
    except Exception as e:
        print(f"ERROR  : {e}")
    print("=" * 65)


# ---------------------------------------------------------------------------
# 1. Baseball Reference — Register (confirmed: correct player page, 200 OK)
#    BUT only has season totals — no game-by-game log.
# ---------------------------------------------------------------------------

probe(
    "BBRef Register — season totals page (CONFIRMED PLAYER, NO GAME LOG)",
    "https://www.baseball-reference.com/register/player.fcgi?id=cholow000roc",
    note="Returns 200 and correct player. Season totals only — no game log table.",
)

# BBRef Stathead game log — requires subscription, included for completeness
probe(
    "BBRef Stathead — game log finder",
    "https://stathead.com/baseball/player-batting-season-finder.cgi",
    params={"player_id": "cholow000roc", "year_min": "2026"},
    note="Stathead — requires paid subscription.",
)

# ---------------------------------------------------------------------------
# 2. ESPN — college baseball player page
#    Player ID 5084498 returned 404 earlier. Try search API to find correct ID.
# ---------------------------------------------------------------------------

probe(
    "ESPN — player search API",
    "https://site.api.espn.com/apis/common/v3/search",
    params={"query": "Roch Cholowsky", "limit": 5, "type": "player"},
    note="Looking for correct ESPN player ID.",
)

probe(
    "ESPN — college baseball scoreboard (yesterday)",
    f"https://www.espn.com/college-sports/baseball/scoreboard/_/date/{yesterday.replace('-','')}",
    note="Check if ESPN has yesterday's college baseball scores at all.",
)

# ---------------------------------------------------------------------------
# 3. D1Baseball.com — try both 2025 and 2026 season URLs
# ---------------------------------------------------------------------------

probe(
    "D1Baseball — player page (2026 season)",
    "https://d1baseball.com/players/2026/roch-cholowsky/",
)

probe(
    "D1Baseball — player page (2025 season)",
    "https://d1baseball.com/players/2025/roch-cholowsky/",
)

# ---------------------------------------------------------------------------
# 4. UCLA Athletics — official team stats page
# ---------------------------------------------------------------------------

probe(
    "UCLA Athletics — baseball stats",
    "https://uclabruins.com/sports/baseball/stats",
    note="Official team page — may have box score links.",
)

probe(
    "UCLA Athletics — baseball schedule/results",
    "https://uclabruins.com/sports/baseball/schedule",
)
