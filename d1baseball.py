"""
d1baseball.py — Season stats fetcher from D1Baseball.com.

Fetches the standard (free) hitting and pitching stats tables for a given school,
fuzzy-matches the player by name, and returns season stats for inclusion in
nightly emails.

Available stats (free tier):
  Hitters:  BA, OBP, SLG, OPS, HR, RBI, BB, K, SB, GP
  Pitchers: ERA, W, L, IP, K, BB, H, APP, GS, SV, WHIP (computed)

Advanced stats (K%, BB%, FIP, wRC+, BABIP, etc.) are subscription-gated on
D1Baseball.com and return placeholder values in the HTML — only the standard
tables are used here.

URL pattern: https://d1baseball.com/team/{slug}/stats/
"""

from __future__ import annotations

import difflib
import logging
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------
# Keyed by team slug.  Invalidated by clear_cache() at the start of each
# nightly job so stats are always fresh within a given run.

_CACHE: dict[str, dict] = {}
_CACHE_TTL = 21600  # 6 hours — safety net for any mid-day manual runs


# ---------------------------------------------------------------------------
# Team slug mapping
# ---------------------------------------------------------------------------
# D1baseball.com uses its own abbreviated slug scheme — NOT simply hyphenated
# school names.  This map was built from the live /teams/ listing page.
# Entries are keyed by lowercased, normalized school name.

_SLUG_OVERRIDES: dict[str, str] = {
    # ── SEC ──────────────────────────────────────────────────────────────
    "ole miss": "olemiss",
    "mississippi": "olemiss",
    "mississippi state": "missst",
    "vanderbilt": "vandy",
    "texas a&m": "texasam",
    "south carolina": "scarolina",
    "georgia southern": "georgiasou",

    # ── ACC ──────────────────────────────────────────────────────────────
    "florida state": "floridast",
    "georgia tech": "gatech",
    "nc state": "ncstate",
    "north carolina": "unc",
    "virginia tech": "vatech",
    "wake forest": "wake",
    "boston college": "bostoncoll",
    "pittsburgh": "pittsburgh",  # same as default but explicit
    "notre dame": "notredame",
    "miami": "miamifl",
    "miami (fl)": "miamifl",
    "miami (ohio)": "miamioh",

    # ── Big 12 ───────────────────────────────────────────────────────────
    "texas christian": "tcu",
    "texas tech": "txtech",
    "oklahoma state": "okstate",
    "kansas state": "kansasst",
    "west virginia": "wvirginia",
    "arizona state": "arizonast",

    # ── Big Ten ───────────────────────────────────────────────────────────
    "ohio state": "ohiost",
    "penn state": "pennst",
    "michigan state": "michiganst",
    "northwestern": "nwestern",
    "central michigan": "cmichigan",
    "eastern michigan": "emichigan",
    "western michigan": "wmichigan",
    "illinois-chicago": "illchicago",

    # ── Other multi-word schools ─────────────────────────────────────────
    "florida atlantic": "flatlantic",
    "south florida": "sflorida",
    "east carolina": "ecarolina",
    "coastal carolina": "coastcar",
    "long beach state": "longbeach",
    "cal state fullerton": "calstfull",
    "cal poly": "calpoly",
    "cal poly slo": "calpoly",
    "san diego state": "sandiegost",
    "san jose state": "sanjosest",
    "new mexico state": "nmstate",
    "new mexico": "nmexico",
    "dallas baptist": "dallasbapt",
    "air force": "airforce",
    "brigham young": "byu",
    "oral roberts": "oralrob",
    "oregon state": "oregonst",
    "north carolina a&t": "ncat",
    "california": "california",
    "uc santa barbara": "ucsb",
    "uc irvine": "ucirvine",
    "uc davis": "ucdavis",
    "uc san diego": "ucsandiego",
    "uc riverside": "ucriver",
    "william & mary": "willmary",
    "james madison": "jamesmad",
    "wright state": "wrightst",
    "georgia state": "georgiast",
    "kennesaw state": "kennesawst",
    "morehead state": "morehead",
    "murray state": "murrayst",
    "northern illinois": "nillinois",
    "southern illinois": "sillinois",
    "eastern illinois": "eillinois",
    "eastern kentucky": "ekentucky",
    "eastern tennessee state": "etennst",
    "east tennessee state": "etennst",
    "middle tennessee": "mtennst",
    "north dakota state": "ndakotast",
    "south dakota state": "sdakotast",
    "southern": "southernu",
    "louisiana": "ulala",
    "louisiana-lafayette": "ulala",
    "louisiana-monroe": "ulamo",
    "incarnate word": "incarnword",
    "texas state": "txstate",
    "utsa": "utsa",
    "ut arlington": "utarl",
    "ut rio grande valley": "utrio",
    "utah valley": "utvalley",
    "sacramento state": "sacstate",
    "cal state northridge": "calstnorth",
    "cal state bakersfield": "calstbaker",
    "grand canyon": "gcanyon",
    "sam houston": "samhouston",
    "sam houston state": "samhouston",
    "stephen f. austin": "sfaustin",
    "florida gulf coast": "flgulfcst",
    "florida international": "flinternat",
    "florida a&m": "floridaam",
    "alabama a&m": "alabamaam",
    "alabama state": "alabamast",
    "prairie view": "prairview",
    "southern a&m": "salabama",
    "grambling state": "grambling",
    "jackson state": "jacksonst",
    "mississippi valley state": "missvalley",
    "alcorn state": "alcornst",
    "southern miss": "smiss",
    "southern mississippi": "smiss",
    "houston baptist": "houstnbapt",
    "houston christian": "houstnbapt",
    "appalachian state": "appalst",
    "georgia mason": "georgemas",
    "george washington": "georgewash",
    "george mason": "georgemas",
    "incarnate word": "incarnword",
    "central connecticut state": "cconnst",
    "central arkansas": "carkansas",
    "arkansas state": "arkansasst",
    "arkansas-pine bluff": "arkansaspb",
    "little rock": "arkansaslr",
    "arkansas-little rock": "arkansaslr",
    "north florida": "nflorida",
    "kennesaw state": "kennesawst",
    "longwood": "longwood",
    "bowling green": "bowlgreen",
    "bowling green state": "bowlgreen",
    "ball state": "ballst",
    "kent state": "kentst",
    "youngstown state": "youngst",
    "wichita state": "wichitast",
    "valparaiso": "valpo",
    "loyola marymount": "loyolamary",
    "santa clara": "santaclara",
    "saint peter's": "stpeters",
    "st. peter's": "stpeters",
    "seton hall": "setonhall",
    "st. john's": "stjohns",
    "st. joseph's": "stjosephs",
    "st. louis": "stlouis",
    "st. mary's": "stmarysca",
    "st. mary's (ca)": "stmarysca",
    "st. bonaventure": "stbonny",
    "stony brook": "stonybrook",
    "binghamton": "sunybing",
    "texas a&m-corpus christi": "tamucc",
    "tarleton state": "tarletonst",
    "north carolina asheville": "uncashe",
    "unc asheville": "uncashe",
    "unc greensboro": "uncgreen",
    "unc wilmington": "uncwilm",
    "uscupstate": "uscupstate",
    "usc upstate": "uscupstate",
    "queens": "queens-nc",
    "queens (nc)": "queens-nc",
    "le moyne": "le-moyne",
    "new haven": "new-haven",
    "utah tech": "utah-tech",
    "dixie state": "utah-tech",
    "west georgia": "west-georgia",
    "lindenwood": "lindenwood",
    "southern indiana": "southern-indiana",
    "bellarmine": "bellarmine",
    "stonehill": "stonehill",
    "merrimack": "merrimack",
}


def _school_to_slug(school: str) -> str:
    """Convert a school name to its D1baseball.com team slug."""
    normalized = school.lower().strip()
    # Strip "university of / at / the" prefixes that might come from player data
    for prefix in ("university of ", "the university of ", "the ", "college of "):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break
    for suffix in (" university", " college", " state university", " a&m university"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break

    if normalized in _SLUG_OVERRIDES:
        return _SLUG_OVERRIDES[normalized]

    # Generic: keep only a–z, 0–9, spaces/hyphens; collapse spaces to hyphens
    slug = re.sub(r"[^a-z0-9\s\-]", "", normalized)
    slug = re.sub(r"\s+", "-", slug.strip())
    return slug


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://d1baseball.com/",
})


def _fetch_stats_page(slug: str) -> Optional[str]:
    """
    Fetch the /stats/ page for a team slug.  Returns HTML or None on failure.

    D1baseball.com sometimes redirects an alias slug to a canonical team URL
    WITHOUT preserving the /stats/ path (e.g. team/mississippi-state/stats/ →
    team/missst/).  We detect this and re-request the /stats/ sub-path on the
    canonical URL so we always land on the stats page.
    """
    url = f"https://d1baseball.com/team/{slug}/stats/"
    try:
        resp = _SESSION.get(url, timeout=20, allow_redirects=True)
        resp.raise_for_status()

        # If we were redirected away from a /stats/ URL, re-fetch stats on canonical slug
        final_url = str(resp.url).rstrip("/")
        if not final_url.endswith("/stats"):
            stats_url = final_url + "/stats/"
            logger.info("d1baseball: redirect detected — re-fetching %s", stats_url)
            resp2 = _SESSION.get(stats_url, timeout=20)
            resp2.raise_for_status()
            return resp2.text

        return resp.text
    except Exception as exc:
        logger.warning("d1baseball: failed to fetch '%s': %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Table parsing
# ---------------------------------------------------------------------------

def _parse_table(soup: BeautifulSoup, table_id: str) -> list[dict]:
    """
    Parse a stats table by its HTML id attribute.

    Returns a list of row-dicts where each key is the column header text (lowercased)
    and "player_name" holds the extracted player name.

    Rows marked sub-required (subscription placeholder values) are still parsed —
    the caller decides whether the data is real.  In practice only the standard
    batting/pitching tables contain real values; the advanced tables return
    garbage placeholder data (12.3, .123, etc.) for non-subscribers.
    """
    table = soup.find("table", {"id": table_id})
    if not table:
        return []

    # Build header list — tooltip divs take precedence over raw th text
    headers: list[str] = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            tooltip = th.find("div", class_="tooltip")
            text = (tooltip or th).get_text(strip=True).lower()
            headers.append(text)

    tbody = table.find("tbody")
    if not tbody:
        return []

    rows: list[dict] = []
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue

        row: dict = {}
        for td, col in zip(cells, headers):
            if col == "qual.":
                continue
            if col == "player":
                link = td.find("a")
                span = td.find("span", class_=re.compile(r"fake-link|player"))
                if link:
                    row["player_name"] = link.get_text(strip=True)
                elif span:
                    row["player_name"] = span.get_text(strip=True)
                else:
                    row["player_name"] = td.get_text(strip=True)
            elif col in ("team", "class"):
                pass  # skip — not needed
            else:
                row[col] = td.get_text(strip=True)

        if row.get("player_name"):
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Team stats fetch + cache
# ---------------------------------------------------------------------------

def _fetch_team_stats(slug: str) -> Optional[dict]:
    """
    Return parsed batting and pitching rows for a team slug.

    Caches results in _CACHE for _CACHE_TTL seconds to avoid refetching
    for every player on the same team in the same nightly run.
    """
    now = time.time()
    cached = _CACHE.get(slug)
    if cached and (now - cached["fetched_at"]) < _CACHE_TTL:
        return cached

    html = _fetch_stats_page(slug)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    batting = _parse_table(soup, "batting-stats")
    pitching = _parse_table(soup, "pitching-stats")

    result = {
        "batting": batting,
        "pitching": pitching,
        "fetched_at": now,
    }
    _CACHE[slug] = result
    logger.info(
        "d1baseball: cached %s — %d hitters, %d pitchers",
        slug, len(batting), len(pitching),
    )
    return result


# ---------------------------------------------------------------------------
# Player name matching
# ---------------------------------------------------------------------------

def _fuzzy_match_player(rows: list[dict], player_name: str) -> Optional[dict]:
    """
    Find the best-matching row for a given player name.

    Match priority:
      1. Exact match (case-insensitive)
      2. difflib fuzzy match (≥ 0.75 similarity)
      3. Last-name + first-initial match (handles nickname differences)
    """
    if not rows:
        return None

    name_lower = player_name.lower().strip()
    names_lower = [r.get("player_name", "").lower() for r in rows]

    # 1. Exact
    for row in rows:
        if row.get("player_name", "").lower() == name_lower:
            return row

    # 2. Fuzzy
    matches = difflib.get_close_matches(name_lower, names_lower, n=1, cutoff=0.75)
    if matches:
        for row in rows:
            if row.get("player_name", "").lower() == matches[0]:
                logger.debug(
                    "d1baseball: fuzzy matched '%s' → '%s'", player_name, row["player_name"]
                )
                return row

    # 3. Last-name + first-initial
    parts = name_lower.split()
    if len(parts) >= 2:
        first_init = parts[0][0]
        last = parts[-1]
        for row in rows:
            rparts = row.get("player_name", "").lower().split()
            if rparts and rparts[-1] == last and len(rparts) >= 2 and rparts[0][0] == first_init:
                logger.debug(
                    "d1baseball: initial+last matched '%s' → '%s'", player_name, row["player_name"]
                )
                return row

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_season_stats(player_name: str, school: str, position: str) -> Optional[dict]:
    """
    Return season stats from D1Baseball.com for one player.

    Returns a dict on success, or None if the school/player can't be found.

    Hitter keys:  ba, obp, slg, ops, hr, rbi, bb, k, sb, gp
    Pitcher keys: era, w, l, ip, k, bb, h, app, gs, sv, whip
    All values are strings (as scraped from the page).
    """
    slug = _school_to_slug(school)
    team_data = _fetch_team_stats(slug)
    if not team_data:
        logger.info("d1baseball: no data for '%s' (slug='%s')", school, slug)
        return None

    if position == "pitcher":
        row = _fuzzy_match_player(team_data["pitching"], player_name)
        if row is None:
            logger.info("d1baseball: pitcher '%s' not found for '%s'", player_name, school)
            return None

        # Compute WHIP from raw counting stats (not in the standard table)
        try:
            ip_val = row.get("ip", "0").rstrip("f").rstrip("r") or "0"  # strip footnote chars
            ip = float(ip_val) if ip_val else 0.0
            h = int(row.get("h", 0) or 0)
            bb = int(row.get("bb", 0) or 0)
            whip = f"{(h + bb) / ip:.2f}" if ip > 0 else "—"
        except (ValueError, ZeroDivisionError):
            whip = "—"

        return {
            "source": "d1baseball",
            "era":  row.get("era", "—"),
            "w":    row.get("w",   "—"),
            "l":    row.get("l",   "—"),
            "ip":   row.get("ip",  "—"),
            "k":    row.get("k",   "—"),
            "bb":   row.get("bb",  "—"),
            "h":    row.get("h",   "—"),
            "app":  row.get("app", "—"),
            "gs":   row.get("gs",  "—"),
            "sv":   row.get("sv",  "—"),
            "whip": whip,
        }

    else:  # hitter
        row = _fuzzy_match_player(team_data["batting"], player_name)
        if row is None:
            logger.info("d1baseball: hitter '%s' not found for '%s'", player_name, school)
            return None

        return {
            "source": "d1baseball",
            "ba":   row.get("ba",  "—"),
            "obp":  row.get("obp", "—"),
            "slg":  row.get("slg", "—"),
            "ops":  row.get("ops", "—"),
            "hr":   row.get("hr",  "—"),
            "rbi":  row.get("rbi", "—"),
            "bb":   row.get("bb",  "—"),
            "k":    row.get("k",   "—"),
            "sb":   row.get("sb",  "—"),
            "gp":   row.get("gp",  "—"),
        }


def clear_cache() -> None:
    """Clear the in-memory cache.  Called at the start of each nightly job."""
    _CACHE.clear()
    logger.debug("d1baseball: cache cleared")
