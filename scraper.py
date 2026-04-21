"""
scraper.py — Modular player stat scraper.

Architecture:
  - BasePlayerScraper   — abstract interface every data source must implement
  - NCAAScraper         — scrapes stats.ncaa.org game logs
  - (future) HSBCScraper, MaxPreps, etc. follow the same interface

The nightly job calls `scrape_all_players()` which routes each player to
the correct scraper based on a source tag stored in the player record
(defaults to "ncaa" for all current players).
"""

import logging
import re
import time
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Optional

import os
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import urlencode, urlparse

load_dotenv()

import database as db

logger = logging.getLogger(__name__)

NCAA_BASE = "https://stats.ncaa.org"

# ---------------------------------------------------------------------------
# HTTP session — routes all stats.ncaa.org requests through ScraperAPI
# ---------------------------------------------------------------------------
# stats.ncaa.org is protected by Akamai. ScraperAPI's render=true mode
# handles the JS challenge and returns the rendered HTML.
# Set SCRAPERAPI_KEY in .env to enable automatic scraping.
# Without it, use manual stat entry in the web UI instead.
# ---------------------------------------------------------------------------

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY", "")
SCRAPERAPI_ENDPOINT = "http://api.scraperapi.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
})

REQUEST_DELAY = 2.0  # seconds between requests


SCRAPERAPI_ULTRA = os.getenv("SCRAPERAPI_ULTRA", "false").lower() == "true"


def _scraperapi_url(target_url: str) -> str:
    """Wrap a target URL for delivery through ScraperAPI with JS rendering.

    Set SCRAPERAPI_ULTRA=true in .env to enable Ultra Premium mode, which
    is required for Akamai-protected sites like stats.ncaa.org.
    """
    params = {
        "api_key": SCRAPERAPI_KEY,
        "render": "true",
        "url": target_url,
    }
    if SCRAPERAPI_ULTRA:
        params["ultra_premium"] = "true"
    return f"{SCRAPERAPI_ENDPOINT}?{urlencode(params)}"


def _get(url: str, retries: int = 2, **kwargs):
    """Fetch url via ScraperAPI (if key configured) or directly.
    Retries up to `retries` times on 500 errors (ScraperAPI transient failures)."""
    time.sleep(REQUEST_DELAY)
    if SCRAPERAPI_KEY:
        fetch_url = _scraperapi_url(url)
    else:
        logger.warning(
            "SCRAPERAPI_KEY not set — requesting %s directly. "
            "Expect 403 from stats.ncaa.org (Akamai protection).",
            url,
        )
        fetch_url = url

    last_exc = None
    for attempt in range(1 + retries):
        try:
            resp = SESSION.get(fetch_url, timeout=90, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            last_exc = exc
            if exc.response is not None and exc.response.status_code == 500 and attempt < retries:
                wait = 10 * (attempt + 1)
                logger.warning("ScraperAPI 500 for %s — retrying in %ds (attempt %d/%d)", url, wait, attempt + 1, retries)
                time.sleep(wait)
                continue
            raise
    raise last_exc


def _direct_get(url: str, **kwargs):
    """Fetch url directly, bypassing ScraperAPI. For static HTML sites."""
    time.sleep(REQUEST_DELAY)
    resp = SESSION.get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Base interface
# ---------------------------------------------------------------------------


class BasePlayerScraper(ABC):
    """
    Every data source subclasses this.  Implement `fetch_latest_game` and
    `source_name`.  The rest of the pipeline (DB writes, email) is source-
    agnostic.
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable label, e.g. 'NCAA Stats'."""

    @abstractmethod
    def fetch_latest_game(self, player: dict) -> Optional[dict]:
        """
        Fetch the most recent game for *player* (a DB row dict).

        Returns a stats dict on success, or None if the player had no game
        yesterday (or data is unavailable).

        The returned dict must include at minimum:
          - 'game_date'  : str  "YYYY-MM-DD"
          - 'opponent'   : str
          - 'team_score' : int  (player's team final score)
          - 'opp_score'  : int
          - 'team_name'  : str

        Hitter additional keys:  ab, h, hr, r, rbi, bb, k
        Pitcher additional keys: ip, h, r, er, bb, hbp, k
        """


# ---------------------------------------------------------------------------
# NCAA Stats scraper
# ---------------------------------------------------------------------------

# Game-log URL pattern:
# https://stats.ncaa.org/players/{ncaa_player_id}/game_log_stats?game_sport_year_ctl_id=...
# We build the game log URL from the player profile page.

HITTER_STAT_COLS = {
    "date": None,
    "opponent": None,
    "result": None,
    "ab": None,
    "r": None,
    "h": None,
    "rbi": None,
    "bb": None,
    "k": None,
    "hr": None,
}

PITCHER_STAT_COLS = {
    "date": None,
    "opponent": None,
    "result": None,
    "ip": None,
    "h": None,
    "r": None,
    "er": None,
    "bb": None,
    "hbp": None,
    "k": None,
}


def _safe_float(val: str) -> float:
    try:
        return float(val.strip())
    except (ValueError, AttributeError):
        return 0.0


def _safe_int(val: str) -> int:
    try:
        return int(str(val).strip().split(".")[0])
    except (ValueError, AttributeError):
        return 0


def _parse_result(result_str: str) -> tuple[int, int]:
    """
    Parse a result string like 'W 7-3' or 'L 2-10' into
    (team_score, opp_score).  Returns (0, 0) on failure.
    """
    match = re.search(r"(\d+)-(\d+)", result_str or "")
    if not match:
        return 0, 0
    a, b = int(match.group(1)), int(match.group(2))
    # NCAA format: winner score first regardless of home/away
    if result_str.upper().startswith("W"):
        return a, b
    else:
        return b, a


def _normalize_ncaa_date(raw_date: str) -> Optional[str]:
    """
    Convert NCAA date strings like '03/15/2025', 'Mar 15', or
    '05/16/2026 04:00 PM' (scheduled games) to YYYY-MM-DD.
    Returns None if unparseable.
    """
    raw_date = raw_date.strip()
    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y", "%m/%d/%y", "%b %d", "%B %d"):
        try:
            dt = datetime.strptime(raw_date, fmt)
            if fmt in ("%b %d", "%B %d"):
                # No year — assume current year, or last year if date is future
                today = date.today()
                dt = dt.replace(year=today.year)
                if dt.date() > today:
                    dt = dt.replace(year=today.year - 1)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _normalize_sidearm_date(raw_date: str) -> Optional[str]:
    """
    Convert Sidearm date strings to YYYY-MM-DD.
    Handles formats like '4/18/2025', '04/18/25', 'Apr. 18', 'Apr 18, 2025'.
    Falls back to _normalize_ncaa_date for shared formats.
    """
    raw_date = raw_date.strip()
    # Strip trailing periods from abbreviated months: "Apr." -> "Apr"
    cleaned = re.sub(r"\b([A-Za-z]+)\.", r"\1", raw_date).strip()
    # Remove ordinal suffixes: "18th" -> "18"
    cleaned = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", cleaned)
    # Try Sidearm-specific formats first, then fall back to NCAA helper
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%m/%d/%y", "%b %d", "%B %d"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            if fmt in ("%b %d", "%B %d"):
                today = date.today()
                dt = dt.replace(year=today.year)
                if dt.date() > today:
                    dt = dt.replace(year=today.year - 1)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class NCAAScraper(BasePlayerScraper):
    """Scrapes game logs from stats.ncaa.org."""

    @property
    def source_name(self) -> str:
        return "NCAA Stats"

    def fetch_latest_game(self, player: dict) -> Optional[dict]:
        ncaa_player_id = player.get("ncaa_player_id")
        if not ncaa_player_id:
            logger.warning(
                "Player %s has no ncaa_player_id — skipping", player["name"]
            )
            return None

        try:
            return self._scrape_game_log(player, ncaa_player_id)
        except requests.HTTPError as exc:
            logger.error(
                "HTTP error fetching stats for %s: %s", player["name"], exc
            )
        except Exception as exc:
            logger.exception(
                "Unexpected error fetching stats for %s: %s", player["name"], exc
            )
        return None

    def _scrape_game_log(self, player: dict, ncaa_player_id: str) -> Optional[dict]:
        # Step 1 — Load the player profile page
        profile_url = f"{NCAA_BASE}/players/{ncaa_player_id}"
        logger.info("Fetching NCAA profile: %s", profile_url)
        resp = _get(profile_url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Step 2 — Try parsing game log directly from the profile page.
        # stats.ncaa.org renders the full game-by-game table on the profile
        # page itself (visible in the "Game By Game" section), so we can often
        # skip the second ScraperAPI call entirely.
        result = self._parse_most_recent_game(soup, player)
        if result is not None:
            logger.info("Parsed game log from profile page for %s", player["name"])
            return result

        # Step 3 — Profile page didn't have the table; find and follow the
        # dedicated game-log tab URL.
        game_log_url = self._find_game_log_url(soup, ncaa_player_id)

        if not game_log_url:
            ctl_id = self._discover_baseball_ctl_id()
            if ctl_id:
                # Use the standard /player/game_log?... format
                game_log_url = (
                    f"{NCAA_BASE}/player/game_log"
                    f"?game_sport_year_ctl_id={ctl_id}&player_id={ncaa_player_id}"
                )
                logger.info("Using discovered ctl_id=%s for %s", ctl_id, player["name"])

        if not game_log_url:
            logger.warning(
                "Could not find game log URL for %s (id=%s)",
                player["name"],
                ncaa_player_id,
            )
            return None

        logger.info("Fetching game log page: %s", game_log_url)
        resp = _get(game_log_url)
        soup = BeautifulSoup(resp.text, "html.parser")
        return self._parse_most_recent_game(soup, player)

    # Cache so we only spend one ScraperAPI credit discovering this per process lifetime
    _cached_ctl_id: Optional[str] = None

    def _discover_baseball_ctl_id(self) -> Optional[str]:
        """
        Fetch the NCAA D1 baseball team-stats index and pull out the current
        game_sport_year_ctl_id.  Result is cached for the lifetime of the process.
        """
        if NCAAScraper._cached_ctl_id:
            return NCAAScraper._cached_ctl_id
        try:
            resp = _get(f"{NCAA_BASE}/rankings/national_team_statistics")
            raw = str(BeautifulSoup(resp.text, "html.parser"))
            for pattern in [
                r"game_sport_year_ctl_id[^0-9]+(\d{4,6})",
                r"sport_year_ctl_id[^0-9]+(\d{4,6})",
            ]:
                match = re.search(pattern, raw)
                if match:
                    NCAAScraper._cached_ctl_id = match.group(1)
                    logger.info("Discovered D1 baseball ctl_id=%s", NCAAScraper._cached_ctl_id)
                    return NCAAScraper._cached_ctl_id
        except Exception as exc:
            logger.warning("Could not discover baseball ctl_id: %s", exc)
        return None

    def _find_game_log_url(self, soup: BeautifulSoup, ncaa_player_id: str) -> Optional[str]:
        """Find the game-log link on the player profile page."""
        # 1. Direct game_log href in anchor tags
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "game_log" in href.lower():
                return href if href.startswith("http") else NCAA_BASE + href

        # 2. game_sport_year_ctl_id in any anchor href — use href as-is
        for a in soup.find_all("a", href=True):
            if "game_sport_year_ctl_id" in a["href"]:
                href = a["href"]
                # If the link already points somewhere useful, follow it
                if "game_log" in href.lower():
                    return href if href.startswith("http") else NCAA_BASE + href
                # Otherwise extract the ctl_id and build the canonical URL
                match = re.search(r"game_sport_year_ctl_id=(\d+)", href)
                if match:
                    ctl_id = match.group(1)
                    return (
                        f"{NCAA_BASE}/player/game_log"
                        f"?game_sport_year_ctl_id={ctl_id}&player_id={ncaa_player_id}"
                    )

        # 3. Search the entire raw HTML — covers JS variables, data attrs, JSON blobs
        raw = str(soup)
        for pattern in [
            r"game_sport_year_ctl_id[^0-9]+(\d{4,6})",
            r"sport_year_ctl_id[^0-9]+(\d{4,6})",
            r"\"ctl_id\"\s*:\s*(\d{4,6})",
            r"ctl_id=(\d{4,6})",
        ]:
            match = re.search(pattern, raw)
            if match:
                ctl_id = match.group(1)
                return (
                    f"{NCAA_BASE}/player/game_log"
                    f"?game_sport_year_ctl_id={ctl_id}&player_id={ncaa_player_id}"
                )

        # 4. Check form fields / select options whose name suggests a sport-year selector
        for el in soup.find_all(["input", "option"]):
            val = el.get("value", "")
            if not re.fullmatch(r"\d{4,6}", str(val)):
                continue
            name_attr = el.get("name", "") or (el.parent.get("name", "") if el.parent else "")
            if re.search(r"sport.year|ctl.id|year.ctl", name_attr, re.I):
                return (
                    f"{NCAA_BASE}/players/{ncaa_player_id}"
                    f"/game_log_stats?game_sport_year_ctl_id={val}"
                )

        return None

    def _parse_most_recent_game(
        self, soup: BeautifulSoup, player: dict
    ) -> Optional[dict]:
        """
        Parse the game log HTML table and return stats for the most recent
        game, or None if the player had no game yesterday.
        """
        table = soup.find("table", id=re.compile(r"game_log", re.I))

        if table is None:
            position = player.get("position", "hitter")
            # Position-specific column to prefer: pitchers need "ip", hitters need "ab"
            position_key = "ip" if position == "pitcher" else "ab"

            all_tables = soup.find_all("table")
            candidates = []  # tables that have date/opponent headers

            for t in all_tables:
                ths = t.find_all("th")
                th_texts = [th.get_text(strip=True).lower() for th in ths]
                has_date_opp = any(re.search(r"\bdate\b|\bopponent\b", h) for h in th_texts)

                if not has_date_opp:
                    # Also check first-row <td>s (some tables use td for headers)
                    first_row = t.find("tr")
                    if first_row:
                        td_texts = [td.get_text(strip=True).lower() for td in first_row.find_all("td")]
                        has_date_opp = any(re.search(r"\bdate\b|\bopponent\b", h) for h in td_texts)
                        th_texts = th_texts or td_texts

                if has_date_opp:
                    candidates.append((t, th_texts))

            # Prefer the candidate whose headers include the position-specific key
            for t, th_texts in candidates:
                if position_key in th_texts:
                    table = t
                    break
            # Fall back to first candidate
            if table is None and candidates:
                table = candidates[0][0]

        if table is None:
            all_tables = soup.find_all("table")
            logger.warning(
                "No game log table found for %s — %d table(s) on page; "
                "headers seen: %s",
                player["name"],
                len(all_tables),
                [
                    [th.get_text(strip=True) for th in t.find_all("th")][:6]
                    for t in all_tables[:3]
                ],
            )
            return None

        headers = [
            th.get_text(strip=True).lower()
            for th in table.find_all("th")
        ]
        # Fallback: headers from first row <td>s if no <th> found
        if not headers:
            first_row = table.find("tr")
            if first_row:
                headers = [td.get_text(strip=True).lower() for td in first_row.find_all("td")]

        rows = table.find_all("tr")
        # Filter to data rows (skip header rows)
        data_rows = [
            r
            for r in rows
            if r.find("td") and not r.find("th")
        ]
        if not data_rows:
            return None

        yesterday = (date.today() - timedelta(days=1)).isoformat()

        # Scan backward through rows looking only for yesterday's game.
        # Skips future/today games, unparseable dates, and totals rows.
        # Stops as soon as it passes yesterday (player didn't play yesterday).
        col_map = None
        for row in reversed(data_rows):
            cells = row.find_all("td")
            if not cells:
                continue
            first_cell_text = cells[0].get_text(strip=True).lower()
            if any(kw in first_cell_text for kw in ("total", "avg", "average", "career")):
                continue

            cell_values = [c.get_text(strip=True) for c in cells]
            row_map = {}
            for i, header in enumerate(headers):
                if i < len(cell_values):
                    row_map[header] = cell_values[i]

            raw_date = row_map.get("date", "")
            game_date_str = _normalize_ncaa_date(raw_date)
            if not game_date_str:
                continue  # unparseable date — skip (e.g. blank scheduled row)

            if game_date_str == yesterday:
                col_map = row_map
                col_map["_game_date"] = game_date_str
                break

            if game_date_str < yesterday:
                # Went past yesterday — player didn't play yesterday
                logger.info(
                    "%s last played on %s — no game to report",
                    player["name"],
                    game_date_str,
                )
                return None
            # game_date_str > yesterday (today or future) — keep scanning back

        if col_map is None:
            logger.info("%s — no game found for %s", player["name"], yesterday)
            return None

        game_date_str = col_map.pop("_game_date")

        # Parse opponent and result
        opponent = col_map.get("opponent", "").strip()
        result_str = col_map.get("result", col_map.get("score", ""))
        team_score, opp_score = _parse_result(result_str)

        base = {
            "game_date": game_date_str,
            "opponent": opponent,
            "team_name": player.get("school", ""),
            "team_score": team_score,
            "opp_score": opp_score,
            "result": result_str,
        }

        if player["position"] == "pitcher":
            return {
                **base,
                "ip": _safe_float(col_map.get("ip", "0")),
                "h": _safe_int(col_map.get("h", "0")),
                "r": _safe_int(col_map.get("r", "0")),
                "er": _safe_int(col_map.get("er", "0")),
                "bb": _safe_int(col_map.get("bb", "0")),
                "hbp": _safe_int(col_map.get("hbp", "0")),
                "k": _safe_int(
                    col_map.get("so", col_map.get("k", col_map.get("ks", "0")))
                ),
            }
        else:  # hitter
            return {
                **base,
                "ab": _safe_int(col_map.get("ab", "0")),
                "h": _safe_int(col_map.get("h", "0")),
                "hr": _safe_int(col_map.get("hr", "0")),
                "r": _safe_int(col_map.get("r", "0")),
                "rbi": _safe_int(col_map.get("rbi", "0")),
                "bb": _safe_int(col_map.get("bb", "0")),
                "k": _safe_int(
                    col_map.get("so", col_map.get("k", col_map.get("ks", "0")))
                ),
            }


# ---------------------------------------------------------------------------
# Sidearm Sports scraper
# ---------------------------------------------------------------------------
# Sidearm Sports (Nextgen/Nuxt platform) powers many Power-5 athletics
# sites (UCLA, etc.).  Player game logs are NOT in the HTML tables — the
# Nuxt shell is server-rendered but stats are loaded via the JSON API:
#
#   GET {host}/api/v2/stats/bio
#       ?rosterPlayerId={id}&sport={shortName}&year={YYYY}
#
# Scores are not in the stats API response.  We stream just the first
# ~30 KB of the player page (the scoreboard section) and parse the W/L
# score from there.
#
# WMT Digital sites (e.g. ASU) return 404 on the stats API → None.
#
# Player dict must include:
#   sidearm_url : str  — URL to the player's profile page
#                        e.g. https://uclabruins.com/sports/baseball/roster/roch-cholowsky/15523
# ---------------------------------------------------------------------------

# Sidearm pitching stats field names (from the Nuxt bundle)
_SIDEARM_PITCH_FIELDS = {
    "ip":  "inningsPitched",
    "h":   "hitsAllowed",
    "r":   "runsAllowed",
    "er":  "earnedRunsAllowed",
    "bb":  "walksAllowed",
    "hbp": "hitBatters",
    "k":   "strikeouts",
}

# Sidearm hitting stats field names (confirmed from live API response)
_SIDEARM_HIT_FIELDS = {
    "ab":  "atBats",
    "h":   "hits",
    "hr":  "homeRuns",
    "r":   "runsScored",
    "rbi": "runsBattedIn",
    "bb":  "walks",
    "k":   "strikeouts",
}


class SidearmScraper(BasePlayerScraper):
    """Scrapes game logs from Sidearm Sports (Nextgen) college athletics sites."""

    @property
    def source_name(self) -> str:
        return "Sidearm Sports"

    def fetch_latest_game(self, player: dict) -> Optional[dict]:
        url = player.get("sidearm_schedule_url") or player.get("sidearm_url")
        if not url:
            logger.warning("Player %s has no sidearm_schedule_url — skipping", player["name"])
            return None
        try:
            return self._scrape(player, url, bypass_date_gate=False)
        except requests.HTTPError as exc:
            logger.error("HTTP error for %s: %s", player["name"], exc)
        except Exception as exc:
            logger.exception("Unexpected error for %s: %s", player["name"], exc)
        return None

    def debug_fetch(self, player: dict) -> Optional[dict]:
        """
        Like fetch_latest_game but bypasses the date gate.
        Use this in tests to verify parsing without needing yesterday's game.
        """
        url = player.get("sidearm_schedule_url") or player.get("sidearm_url")
        if not url:
            return None
        try:
            return self._scrape(player, url, bypass_date_gate=True)
        except Exception as exc:
            logger.exception("debug_fetch failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Internal implementation
    # ------------------------------------------------------------------

    def _scrape(
        self, player: dict, url: str, *, bypass_date_gate: bool
    ) -> Optional[dict]:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        # Extract sport shortname and roster player ID from the URL path.
        # Path pattern: /sports/{sport}/roster/{slug}/{id}
        path_parts = [p for p in parsed.path.split("/") if p]
        sport = path_parts[1] if len(path_parts) > 1 else "baseball"
        roster_player_id = path_parts[-1] if path_parts and path_parts[-1].isdigit() else None

        if not roster_player_id:
            logger.warning("Could not extract roster player ID from %s", url)
            return None

        year = date.today().year

        # 1. Fetch game-by-game stats from the JSON API
        stats_data = self._fetch_player_stats(base_url, roster_player_id, sport, year)
        if stats_data is None:
            return None

        # 2. Pick the most recent game entry (hitter vs pitcher)
        position = player.get("position", "hitter")
        game_entry = self._latest_game_entry(stats_data, position)
        if game_entry is None:
            logger.info("No game entries in stats API response for %s", player["name"])
            return None

        # 3. Parse and gate by date
        game_date_str = self._parse_stats_date(game_entry.get("date", ""))
        if not game_date_str:
            logger.warning(
                "Could not parse stats API date '%s' for %s",
                game_entry.get("date"), player["name"],
            )
            return None

        if not bypass_date_gate:
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            today_str = date.today().isoformat()
            if game_date_str not in (yesterday, today_str):
                logger.info(
                    "%s last played on %s — no game to report",
                    player["name"], game_date_str,
                )
                return None

        # 4. Get team/opp score from the Sidearm scoreboard API
        opponent = game_entry.get("opponent", "").strip()
        event_id = self._extract_event_id(game_entry.get("boxscoreUrl", ""))
        team_score, opp_score = self._fetch_scoreboard_score(base_url, event_id)

        result_str = game_entry.get("result", "")  # "W" or "L"

        base = {
            "game_date": game_date_str,
            "opponent": opponent,
            "team_name": player.get("school", ""),
            "team_score": team_score,
            "opp_score": opp_score,
            "result": result_str,
        }

        if position == "pitcher":
            return {
                **base,
                **{
                    k: _safe_float(game_entry.get(api_field, "0"))
                    if k == "ip"
                    else _safe_int(game_entry.get(api_field, "0"))
                    for k, api_field in _SIDEARM_PITCH_FIELDS.items()
                },
            }
        else:
            return {
                **base,
                **{
                    k: _safe_int(game_entry.get(api_field, "0"))
                    for k, api_field in _SIDEARM_HIT_FIELDS.items()
                },
            }

    def _fetch_player_stats(
        self, base_url: str, roster_player_id: str, sport: str, year: int
    ) -> Optional[dict]:
        """Call the Sidearm JSON stats API. Returns None on 404 (WMT Digital)."""
        api_url = f"{base_url}/api/v2/stats/bio"
        try:
            resp = _direct_get(
                api_url,
                params={
                    "rosterPlayerId": roster_player_id,
                    "sport": sport,
                    "year": year,
                },
            )
            return resp.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                logger.info(
                    "Sidearm stats API 404 for %s/%s — likely WMT Digital, skipping",
                    sport, roster_player_id,
                )
                return None
            raise

    def _latest_game_entry(
        self, stats_data: dict, position: str
    ) -> Optional[dict]:
        """Return the most recent actual game entry (skip totals rows with date=null)."""
        current = stats_data.get("currentStats", {}) or {}

        if position == "pitcher":
            games = current.get("pitchingStats") or []
        else:
            games = current.get("hittingStats") or []

        # Actual game rows always have a date; totals/averages rows have date=null
        actual = [g for g in games if g.get("date")]
        if not actual:
            return None

        # The API returns games in chronological order; last entry is most recent
        return actual[-1]

    @staticmethod
    def _parse_stats_date(raw: str) -> Optional[str]:
        """
        Parse a Sidearm stats API date like '4/19/2026 12:00:00\u202fPM' → 'YYYY-MM-DD'.
        """
        if not raw:
            return None
        # Date part is before the first whitespace character (including narrow NNBSP)
        date_part = re.split(r"[\s\u202f\u00a0]", raw.strip())[0]
        try:
            return datetime.strptime(date_part, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return _normalize_sidearm_date(raw)

    @staticmethod
    def _extract_event_id(boxscore_url: str) -> Optional[int]:
        """
        Extract the numeric event ID from a Sidearm boxscoreUrl.
        e.g. '/sports/baseball/stats/2026/minnesota/boxscore/34556' → 34556
        """
        m = re.search(r"/(\d+)/?$", boxscore_url or "")
        return int(m.group(1)) if m else None

    def _fetch_scoreboard_score(
        self, base_url: str, event_id: Optional[int]
    ) -> tuple[int, int]:
        """
        Call the Sidearm scoreboard API and return (team_score, opp_score)
        for the given event ID.  Falls back to (0, 0) on any error.

        The scoreboard endpoint returns recent + upcoming events across all
        sports (sport_id is omitted so we see all). We match by event ID.
        """
        if event_id is None:
            return (0, 0)
        try:
            resp = _direct_get(
                f"{base_url}/api/v2.1/EventsResults/scoreboard",
            )
            data = resp.json()
            for item in data.get("items", []):
                if item.get("id") == event_id:
                    result = item.get("result") or {}
                    ts = _safe_int(result.get("teamScore", "0"))
                    os_ = _safe_int(result.get("opponentScore", "0"))
                    return (ts, os_)
            logger.warning(
                "Event ID %d not found in scoreboard response (%d items)",
                event_id, len(data.get("items", [])),
            )
        except Exception as exc:
            logger.warning("Could not fetch scoreboard score for event %s: %s", event_id, exc)
        return (0, 0)


# ---------------------------------------------------------------------------
# Scraper registry — add new sources here as they are implemented
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# SidearmLegacyScraper — older static-HTML Sidearm sites
# ---------------------------------------------------------------------------
# These sites pre-date Sidearm Nextgen and do NOT have the /api/v2/ JSON API.
# Stats are served as pre-rendered HTML tables via:
#   GET /services/responsive-roster-bio.ashx?type=stats&rp_id={id}&path={sport}&year={year}&player_id=0
#
# The HTML response contains <section> blocks with <table> game log rows.
# Pitching columns: Date, Opponent, W/L, IP, H, R, ER, BB, SO, ..., HBP, ..., SCORE, ...
# Hitting columns:  Date, Opponent, W/L, AB, R, H, RBI, 2B, 3B, HR, BB, HBP, K, SB, ...
# SCORE column format: "team_score-opp_score" (e.g. "5-3" = we scored 5, they scored 3)
# ---------------------------------------------------------------------------

_LEGACY_PITCH_COLS = ["date", "opponent", "wl", "ip", "h", "r", "er", "bb", "so",
                      "2b", "3b", "hr", "wp", "bk", "hbp", "bf", "np", "score",
                      "w", "l", "sv", "g-era", "s-era"]
_LEGACY_HIT_COLS   = ["date", "opponent", "wl", "ab", "r", "h", "2b", "3b", "hr",
                      "rbi", "bb", "hbp", "k", "sb", "cs", "tb", "slg", "obp",
                      "score", "gdp", "go", "fo", "np", "pa"]


class SidearmLegacyScraper(BasePlayerScraper):
    """Scrapes game logs from older static-HTML Sidearm sites via responsive-roster-bio.ashx."""

    @property
    def source_name(self) -> str:
        return "Sidearm Legacy"

    def fetch_latest_game(self, player: dict) -> Optional[dict]:
        return self._scrape(player, bypass_date_gate=False)

    def debug_fetch(self, player: dict) -> Optional[dict]:
        return self._scrape(player, bypass_date_gate=True)

    def _scrape(self, player: dict, *, bypass_date_gate: bool) -> Optional[dict]:
        url = player.get("sidearm_schedule_url") or player.get("sidearm_url")
        if not url:
            logger.warning("Player %s has no sidearm_schedule_url", player["name"])
            return None

        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path_parts = [p for p in parsed.path.split("/") if p]
        sport = path_parts[1] if len(path_parts) > 1 else "baseball"
        rp_id = path_parts[-1] if path_parts and path_parts[-1].isdigit() else None
        if not rp_id:
            logger.warning("Could not extract rp_id from %s", url)
            return None

        year = date.today().year
        api_url = f"{base_url}/services/responsive-roster-bio.ashx"
        try:
            resp = _direct_get(api_url, params={
                "type": "stats", "rp_id": rp_id, "path": sport,
                "year": year, "player_id": 0,
            })
            data = resp.json()
        except Exception as exc:
            logger.error("Could not fetch legacy stats for %s: %s", player["name"], exc)
            return None

        html = data.get("current_stats", "")
        if not html:
            logger.info("No current_stats HTML for %s", player["name"])
            return None

        position = player.get("position", "hitter")
        row = self._latest_row(html, position)
        if row is None:
            logger.info("No game rows found for %s (position=%s)", player["name"], position)
            return None

        game_date_str = self._parse_date(row.get("date", ""))
        if not game_date_str:
            logger.warning("Could not parse date '%s' for %s", row.get("date"), player["name"])
            return None

        if not bypass_date_gate:
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            today_str = date.today().isoformat()
            if game_date_str not in (yesterday, today_str):
                logger.info("%s last played on %s — no game to report", player["name"], game_date_str)
                return None

        team_score, opp_score = self._parse_score(row.get("score", ""))
        base = {
            "game_date": game_date_str,
            "opponent": row.get("opponent", "").strip(),
            "team_name": player.get("school", ""),
            "team_score": team_score,
            "opp_score": opp_score,
            "result": row.get("w/l", row.get("wl", "")).strip(),
        }

        if position == "pitcher":
            return {
                **base,
                "ip":  _safe_float(row.get("ip", "0")),
                "h":   _safe_int(row.get("h", "0")),
                "r":   _safe_int(row.get("r", "0")),
                "er":  _safe_int(row.get("er", "0")),
                "bb":  _safe_int(row.get("bb", "0")),
                "hbp": _safe_int(row.get("hbp", "0")),
                "k":   _safe_int(row.get("so", "0")),
            }
        else:
            return {
                **base,
                "ab":  _safe_int(row.get("ab", "0")),
                "h":   _safe_int(row.get("h", "0")),
                "hr":  _safe_int(row.get("hr", "0")),
                "r":   _safe_int(row.get("r", "0")),
                "rbi": _safe_int(row.get("rbi", "0")),
                "bb":  _safe_int(row.get("bb", "0")),
                "k":   _safe_int(row.get("k", "0")),
            }

    def _latest_row(self, html: str, position: str) -> Optional[dict]:
        """Parse the HTML stats table and return the last game row as a dict."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        target_heading = "Pitching Statistics" if position == "pitcher" else "Hitting Statistics"
        for section in soup.find_all("section"):
            h5 = section.find("h5")
            if not h5 or target_heading.lower() not in h5.get_text().lower():
                continue
            table = section.find("table")
            if not table:
                continue
            headers = [th.get_text(strip=True).lower() for th in table.select("thead th")]
            rows = table.select("tbody tr")
            if not rows:
                return None
            last = rows[-1]
            cells = [td.get_text(strip=True) for td in last.find_all("td")]
            return dict(zip(headers, cells))

        return None

    @staticmethod
    def _parse_date(raw: str) -> Optional[str]:
        raw = raw.strip()
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return _normalize_sidearm_date(raw)

    @staticmethod
    def _parse_score(score_str: str) -> tuple[int, int]:
        """Parse 'team-opp' score string, e.g. '5-3' → (5, 3)."""
        m = re.match(r"(\d+)-(\d+)", score_str.strip())
        if m:
            return int(m.group(1)), int(m.group(2))
        return 0, 0


_SCRAPER_REGISTRY: dict[str, BasePlayerScraper] = {
    "ncaa": NCAAScraper(),
    "sidearm": SidearmScraper(),
    "sidearm_legacy": SidearmLegacyScraper(),
    # "maxpreps": MaxPrepsScraper(),   # future
    # "perfect_game": PerfectGameScraper(),  # future high school source
}


def get_scraper(source: str = "ncaa") -> BasePlayerScraper:
    scraper = _SCRAPER_REGISTRY.get(source)
    if scraper is None:
        raise ValueError(
            f"Unknown scraper source '{source}'. "
            f"Available: {list(_SCRAPER_REGISTRY)}"
        )
    return scraper


# ---------------------------------------------------------------------------
# Player connectivity test — used by "Check now" / verify flow in the UI
# ---------------------------------------------------------------------------


def _test_sidearm_connectivity(player: dict, *, legacy: bool = False) -> tuple[bool, str]:
    """Check whether the Sidearm stats API endpoint is reachable for this player."""
    url = player.get("sidearm_schedule_url") or player.get("sidearm_url")
    if not url:
        return False, "No Sidearm URL configured"
    try:
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        path_parts = [p for p in parsed.path.split("/") if p]
        sport = path_parts[1] if len(path_parts) > 1 else "baseball"
        pid = path_parts[-1] if path_parts and path_parts[-1].isdigit() else None
        if not pid:
            return False, "Could not extract player ID from Sidearm URL"
        year = date.today().year
        if legacy:
            _direct_get(
                f"{base_url}/services/responsive-roster-bio.ashx",
                params={"type": "stats", "rp_id": pid, "path": sport,
                        "year": year, "player_id": 0},
            )
        else:
            _direct_get(
                f"{base_url}/api/v2/stats/bio",
                params={"rosterPlayerId": pid, "sport": sport, "year": year},
            )
        return True, ""
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response else "?"
        if code == 404:
            return (
                False,
                "This school uses the WMT Games platform — Sidearm stats API not "
                "available. Enter the player's NCAA Stats ID manually using Fix.",
            )
        return False, f"HTTP {code}"
    except Exception as exc:
        return False, str(exc)


def _test_ncaa_connectivity(player: dict) -> tuple[bool, str]:
    """Check whether the NCAA stats profile page loads for this player."""
    ncaa_id = player.get("ncaa_player_id")
    if not ncaa_id:
        return False, "No NCAA player ID — use Fix to add one manually"
    try:
        _get(f"{NCAA_BASE}/players/{ncaa_id}")
        return True, ""
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response else "?"
        return False, f"NCAA profile returned HTTP {code}"
    except Exception as exc:
        return False, str(exc)


def test_player_connectivity(player: dict) -> tuple[bool, str]:
    """
    Verify that we have what we need to scrape this player.

    For Sidearm: makes a direct (non-ScraperAPI) call to the stats API to confirm
    the endpoint is reachable.  Fast — no credit cost.
    For NCAA: just confirms an ncaa_player_id is configured (the nightly job proves
    the real connectivity when it runs).

    Returns (success, error_message).
    """
    source = player.get("source", "ncaa")

    if source in ("sidearm", "sidearm_legacy"):
        ok, err = _test_sidearm_connectivity(player, legacy=(source == "sidearm_legacy"))
        if ok:
            return True, ""
        # Sidearm failed — but if we have an NCAA fallback ID, we're still good
        if player.get("ncaa_player_id"):
            return True, ""
        return False, err

    # NCAA source — just check that we have a player ID
    if player.get("ncaa_player_id"):
        return True, ""
    return False, "No NCAA player ID configured — use Fix to add one manually"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def _scrape_player_with_fallback(player: dict) -> tuple[Optional[dict], str]:
    """
    Try primary scraper, fall back to NCAA via ScraperAPI if it fails.
    Returns (stats_or_None, error_message).
    """
    source = player.get("source", "ncaa")
    name = player["name"]
    school = player["school"]
    ncaa_id = player.get("ncaa_player_id", "")

    # Try primary scraper
    primary_error = ""
    try:
        scraper = get_scraper(source)
        logger.info("Scraping %s [%s] via %s ...", name, school, scraper.source_name)
        stats = scraper.fetch_latest_game(player)
        if stats is not None:
            return stats, ""
    except ValueError:
        primary_error = f"No scraper for source '{source}'"
        logger.error(primary_error + " (player: %s)", name)
    except Exception as exc:
        primary_error = str(exc)
        logger.error("Primary scraper error for %s: %s", name, exc)

    # Fall back to NCAA scraper if we have a player ID and primary source is not NCAA
    if ncaa_id and source != "ncaa":
        logger.info("Trying NCAA fallback for %s (id=%s)", name, ncaa_id)
        try:
            stats = NCAAScraper().fetch_latest_game(player)
            if stats is not None:
                return stats, ""
        except Exception as exc:
            logger.error("NCAA fallback also failed for %s: %s", name, exc)
            return None, f"Primary: {primary_error or 'no game'} | NCAA fallback: {exc}"

    return None, primary_error


def scrape_all_players() -> int:
    """
    Scrape the latest game for every player in the DB.
    Writes results to games_log. Updates scrape_status on persistent failures.
    Returns the number of new game entries saved.
    """
    players = db.get_all_players()
    if not players:
        logger.info("No players in database — nothing to scrape")
        return 0

    saved = 0
    for player in players:
        stats, error = _scrape_player_with_fallback(player)
        if stats is None:
            if error:
                db.update_player_scrape_status(player["id"], "failed", error)
                logger.info("  -> Scrape failed for %s: %s", player["name"], error)
            else:
                logger.info("  -> No game to report for %s", player["name"])
            continue

        game_date = stats.get("game_date", date.today().isoformat())
        db.upsert_game_log(player["id"], game_date, stats)
        db.update_player_scrape_status(player["id"], "verified", "")
        logger.info("  -> Saved stats for %s on %s", player["name"], game_date)
        saved += 1

    logger.info("Scraping complete. %d new/updated game entries saved.", saved)
    return saved


# ---------------------------------------------------------------------------
# Manual test — python scraper.py
# ---------------------------------------------------------------------------
# Tests SidearmScraper against Roch Cholowsky (UCLA pitcher).
# Uses debug_fetch() which bypasses the date gate so it works any day.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")

    # Roch Cholowsky — UCLA (Sidearm Sports / Nextgen)
    # He plays as a hitter/infielder at UCLA (transferred from Charlotte).
    cholowsky = {
        "name": "Roch Cholowsky",
        "school": "UCLA",
        "position": "hitter",  # position player at UCLA
        "source": "sidearm",
        "sidearm_url": "https://uclabruins.com/sports/baseball/roster/roch-cholowsky/15523",
    }

    scraper = SidearmScraper()
    print("\n--- UCLA (Sidearm Nextgen / JSON API) ---")
    result = scraper.debug_fetch(cholowsky)
    if result:
        print(json.dumps(result, indent=2))
    else:
        print("No result returned.")
        sys.exit(1)
