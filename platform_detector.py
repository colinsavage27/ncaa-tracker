"""
platform_detector.py — Automatic school athletics platform detection.

Given a player name and school name, this module:
  1. Finds the school's athletics website URL
  2. Fingerprints which CMS platform it runs on
  3. Finds the player's specific roster/stats page URL
  4. Returns everything needed to populate the players table

Supported platforms:
  sidearm       — Sidearm Sports Nextgen (Nuxt.js SSR, JSON API)
  sidearm_legacy — Older Sidearm (static HTML tables)
  wmt_digital   — WMT Digital (React SPA, JS-only — fallback to NCAA scraper)
  prestosports  — PrestoSports CMS
  ncaa          — Unknown/fallback; use NCAA stats scraper
"""

from __future__ import annotations

import difflib
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote_plus, urlparse

import requests

logger = logging.getLogger(__name__)

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
})

REQUEST_DELAY = 1.0  # seconds between requests


def _fetch(url: str, params: dict | None = None, timeout: int = 20) -> requests.Response:
    time.sleep(REQUEST_DELAY)
    resp = _SESSION.get(url, params=params, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp


# ---------------------------------------------------------------------------
# Curated school → athletics base URL map
# Each key is a lowercased, normalized school name.
# ---------------------------------------------------------------------------

SCHOOL_ATHLETICS_MAP: dict[str, str] = {
    # ACC
    "clemson": "https://clemsontigers.com",
    "duke": "https://goduke.com",
    "florida state": "https://seminoles.com",
    "georgia tech": "https://ramblinwreck.com",
    "louisville": "https://gocards.com",
    "miami": "https://hurricanesports.com",
    "nc state": "https://gopack.com",
    "north carolina": "https://tarheels.com",
    "notre dame": "https://und.com",
    "virginia": "https://virginiaathletics.com",
    "wake forest": "https://godeacs.com",
    "boston college": "https://bceagles.com",
    "pittsburgh": "https://pittsburghpanthers.com",
    "syracuse": "https://cuse.com",
    "virginia tech": "https://hokiesports.com",

    # SEC
    "alabama": "https://rolltide.com",
    "arkansas": "https://arkansasrazorbacks.com",
    "auburn": "https://auburntigers.com",
    "florida": "https://floridagators.com",
    "georgia": "https://georgiadogs.com",
    "kentucky": "https://ukathletics.com",
    "lsu": "https://lsusports.net",
    "mississippi state": "https://hailstate.com",
    "ole miss": "https://olemisssports.com",
    "missouri": "https://mutigers.com",
    "south carolina": "https://gamecocksonline.com",
    "tennessee": "https://utsports.com",
    "texas a&m": "https://12thman.com",
    "vanderbilt": "https://vucommodores.com",

    # Big 12
    "arizona": "https://arizonawildcats.com",
    "arizona state": "https://thesundevils.com",
    "baylor": "https://baylorbears.com",
    "houston": "https://uhcougars.com",
    "kansas": "https://kuathletics.com",
    "kansas state": "https://kstatesports.com",
    "oklahoma state": "https://okstate.com",
    "tcu": "https://gofrogs.com",
    "texas": "https://texassports.com",
    "texas tech": "https://texastech.com",
    "ucf": "https://ucfknights.com",
    "utah": "https://utahutes.com",
    "west virginia": "https://wvusports.com",

    # Pac-12 / Big Ten migrations
    "cal": "https://calbears.com",
    "oregon state": "https://osubeavers.com",
    "stanford": "https://gostanford.com",
    "ucla": "https://uclabruins.com",
    "usc": "https://usctrojans.com",
    "washington": "https://gohuskies.com",

    # Big Ten
    "illinois": "https://fightingillini.com",
    "indiana": "https://iuhoosiers.com",
    "maryland": "https://umterps.com",
    "michigan": "https://mgoblue.com",
    "michigan state": "https://msuspartans.com",
    "minnesota": "https://gophersports.com",
    "nebraska": "https://huskers.com",
    "northwestern": "https://nusports.com",
    "ohio state": "https://ohiostatebuckeyes.com",
    "penn state": "https://gopsusports.com",
    "purdue": "https://purduesports.com",
    "rutgers": "https://scarletknights.com",

    # American / Sun Belt / CUSA / other prominent programs
    "cal state fullerton": "https://fullertontitans.com",
    "dallas baptist": "https://dbupatriots.com",
    "coastal carolina": "https://goccusports.com",
    "east carolina": "https://ecupirates.com",
    "florida atlantic": "https://fausports.com",
    "long beach state": "https://longbeachstate.com",
    "memphis": "https://gotigersgo.com",
    "mississippi valley state": "https://mvsuathletics.com",
    "new mexico": "https://golobos.com",
    "oral roberts": "https://orualengolden.com",
    "san diego": "https://toreroathletics.com",
    "seton hall": "https://shupirates.com",
    "south florida": "https://gousfbulls.com",
    "tulane": "https://tulanegreenwave.com",
    "wright state": "https://wsuraiders.com",
}

# Common school name aliases / abbreviations
_ALIASES: dict[str, str] = {
    "unc": "north carolina",
    "uva": "virginia",
    "vt": "virginia tech",
    "osu": "ohio state",
    "msstate": "mississippi state",
    "miss state": "mississippi state",
    "miss. state": "mississippi state",
    "a&m": "texas a&m",
    "texas am": "texas a&m",
    "fsu": "florida state",
    "asu": "arizona state",
    "ucla": "ucla",
    "usc": "usc",
    "gt": "georgia tech",
    "wfu": "wake forest",
    "ncsu": "nc state",
    "uf": "florida",
    "uga": "georgia",
}


# ---------------------------------------------------------------------------
# DetectionResult
# ---------------------------------------------------------------------------

@dataclass
class DetectionResult:
    school_name: str
    player_name: str
    athletics_url: str = ""
    platform: str = "unknown"       # sidearm | sidearm_legacy | unknown
    player_url: str = ""            # full URL for the player's roster/stats page
    source: str = "ncaa"            # maps to DB `source` column → which scraper to use
    ncaa_player_id: str = ""        # populated only when source='ncaa'
    success: bool = False
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# School name normalisation & athletics URL discovery
# ---------------------------------------------------------------------------

def _normalize_school_name(name: str) -> str:
    """Lowercase, remove 'university of / at / the', common suffixes, extra spaces."""
    s = name.lower().strip()
    # Resolve aliases first
    if s in _ALIASES:
        return _ALIASES[s]
    for alias, canonical in _ALIASES.items():
        if s == alias:
            return canonical
    # Strip generic prefixes / suffixes
    prefixes = ["university of ", "the university of ", "the ", "college of "]
    for prefix in prefixes:
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    suffixes = [
        " university", " college", " state university", " a&m university",
        " institute of technology", " polytechnic", " tech",
    ]
    for suffix in suffixes:
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    return s.strip()


def _fuzzy_match_school(normalized: str) -> Optional[str]:
    """Fuzzy match a normalized school name against the curated map. Returns base URL or None."""
    keys = list(SCHOOL_ATHLETICS_MAP.keys())
    matches = difflib.get_close_matches(normalized, keys, n=1, cutoff=0.80)
    if matches:
        logger.debug("Fuzzy matched '%s' → '%s'", normalized, matches[0])
        return SCHOOL_ATHLETICS_MAP[matches[0]]
    # Also try partial containment
    for key in keys:
        if normalized in key or key in normalized:
            logger.debug("Partial match '%s' → '%s'", normalized, key)
            return SCHOOL_ATHLETICS_MAP[key]
    return None


def _duckduckgo_athletics_url(school_name: str, sport: str = "baseball") -> Optional[str]:
    """Fall back to DuckDuckGo HTML search to find the school's athletics site."""
    query = f"{school_name} {sport} official athletics site"
    ddg_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        resp = _fetch(ddg_url, timeout=15)
        html = resp.text
        # DuckDuckGo result links are in <a class="result__url" href="..."> or rel="nofollow"
        # The actual result URLs appear as href attributes in result links
        href_pattern = re.compile(
            r'<a[^>]+class="result__a"[^>]+href="([^"]+)"', re.IGNORECASE
        )
        # Also try the redirect links which contain uddg= param
        uddg_pattern = re.compile(r'uddg=([^&"]+)', re.IGNORECASE)

        candidates: list[str] = []
        for m in href_pattern.finditer(html):
            href = m.group(1)
            if href.startswith("http"):
                candidates.append(href)

        for m in uddg_pattern.finditer(html):
            from urllib.parse import unquote
            url = unquote(m.group(1))
            if url.startswith("http"):
                candidates.append(url)

        # Filter to likely athletics domains (avoid Wikipedia, generic news, etc.)
        bad_domains = {"wikipedia.org", "espn.com", "cbssports.com", "247sports.com",
                       "rivals.com", "on3.com", "twitter.com", "facebook.com",
                       "instagram.com", "youtube.com", "ncaa.com", "ncaa.org"}
        for url in candidates:
            parsed = urlparse(url)
            domain = parsed.netloc.lstrip("www.")
            if not any(bad in domain for bad in bad_domains):
                base = f"{parsed.scheme}://{parsed.netloc}"
                logger.info("DuckDuckGo found athletics URL for '%s': %s", school_name, base)
                return base
    except Exception as exc:
        logger.warning("DuckDuckGo search failed for '%s': %s", school_name, exc)
    return None


def discover_athletics_url(school_name: str, sport: str = "baseball") -> Optional[str]:
    """
    Find the school's athletics base URL.
    Order: curated map → fuzzy map match → DuckDuckGo search.
    Returns base URL like 'https://uclabruins.com' or None.
    """
    normalized = _normalize_school_name(school_name)
    logger.debug("discover_athletics_url: normalized='%s'", normalized)

    # 1. Exact match
    if normalized in SCHOOL_ATHLETICS_MAP:
        return SCHOOL_ATHLETICS_MAP[normalized]

    # 2. Fuzzy match
    url = _fuzzy_match_school(normalized)
    if url:
        return url

    # 3. Web search fallback
    return _duckduckgo_athletics_url(school_name, sport)


# ---------------------------------------------------------------------------
# Platform fingerprinting
# ---------------------------------------------------------------------------

def fingerprint_platform(base_url: str, sport: str = "baseball") -> str:
    """
    Fetch the school's sport roster page and detect the CMS platform.

    In practice, essentially all D1 baseball programs use Sidearm Sports in one
    of two generations:

      sidearm        — Sidearm Nextgen (Nuxt.js SSR).  Marker: __NUXT_DATA__ blob.
                       Uses the /api/v2/stats/bio JSON API.
      sidearm_legacy — Older static-HTML Sidearm.  Markers: sidearmsports.com CDN
                       domain or the responsive-roster-bio.ashx service path.
                       Uses the /services/responsive-roster-bio.ashx HTML-table API.

    Returns one of: sidearm | sidearm_legacy | unknown
    """
    roster_url = f"{base_url}/sports/{sport}/roster"
    try:
        resp = _fetch(roster_url, timeout=20)
        html = resp.text
    except requests.HTTPError as exc:
        logger.warning("fingerprint_platform: HTTP %s for %s",
                       exc.response.status_code if exc.response else "?", roster_url)
        return "unknown"
    except Exception as exc:
        logger.warning("fingerprint_platform: failed to fetch %s: %s", roster_url, exc)
        return "unknown"

    # Sidearm Nextgen — Nuxt.js SSR, contains a large __NUXT_DATA__ JSON blob
    if "__NUXT_DATA__" in html:
        logger.info("Platform: sidearm (Nextgen/Nuxt) at %s", base_url)
        return "sidearm"

    # Sidearm Legacy — older static-HTML Sidearm identified by its CDN domain,
    # the responsive-roster-bio service path, or the Sidearm consent-manager UUID
    _SIDEARM_LEGACY_MARKERS = (
        "sidearmsports.com",
        "sidearm.nextgen.sites",          # CDN path used by some legacy installs
        "responsive-roster-bio.ashx",
        "30fbff84-b0e3-4e26-9084-0b5158fdb1ed",  # Sidearm Transcend consent UUID
    )
    if any(m in html for m in _SIDEARM_LEGACY_MARKERS):
        logger.info("Platform: sidearm_legacy at %s", base_url)
        return "sidearm_legacy"

    logger.info("Platform: unknown at %s", base_url)
    return "unknown"


# ---------------------------------------------------------------------------
# Player URL discovery — Sidearm
# ---------------------------------------------------------------------------

def find_player_url_sidearm(
    base_url: str,
    player_name: str,
    sport: str = "baseball",
) -> Optional[str]:
    """
    Scrape the Sidearm roster page and return the full URL for the given player.

    Two strategies:
    1. HTML link regex — works for Sidearm legacy where links are server-rendered.
       Links look like: href="/sports/baseball/roster/firstname-lastname/12345"
    2. NUXT_DATA blob parsing — used when Sidearm Nextgen renders the list client-side
       and no player links appear in the static HTML.
    """
    roster_url = f"{base_url}/sports/{sport}/roster"
    try:
        resp = _fetch(roster_url, timeout=20)
        html = resp.text
    except Exception as exc:
        logger.warning("Could not fetch Sidearm roster at %s: %s", roster_url, exc)
        return None

    # Strategy 1: HTML anchor links
    path = _find_player_via_html_links(html, base_url, player_name, sport)
    if path:
        return path

    # Strategy 2: __NUXT_DATA__ blob (Sidearm Nextgen)
    if "__NUXT_DATA__" in html:
        path = _find_player_via_nuxt_blob(html, base_url, player_name, sport)
        if path:
            return path

    logger.warning("Could not find '%s' in roster at %s", player_name, roster_url)
    return None


def _find_player_via_html_links(
    html: str, base_url: str, player_name: str, sport: str
) -> Optional[str]:
    """Parse static HTML anchor links for player roster URLs."""
    pattern = re.compile(
        r'href="(/sports/' + re.escape(sport) + r'/roster/([^/\"]+)/(\d+))"',
        re.IGNORECASE,
    )
    candidates: list[tuple[str, str]] = []  # (path, slug)
    seen_ids: set[str] = set()
    for m in pattern.finditer(html):
        path_, slug, pid = m.group(1), m.group(2), m.group(3)
        if pid not in seen_ids:
            seen_ids.add(pid)
            candidates.append((path_, slug))

    if not candidates:
        return None

    logger.debug("HTML link strategy: found %d candidates", len(candidates))
    return _best_slug_match(candidates, player_name, base_url)


def _find_player_via_nuxt_blob(
    html: str, base_url: str, player_name: str, sport: str
) -> Optional[str]:
    """Parse the __NUXT_DATA__ JSON blob to extract player ids and slugs."""
    try:
        m = re.search(
            r'<script[^>]+id="__NUXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if not m:
            return None
        blob = json.loads(m.group(1))
    except Exception as exc:
        logger.debug("NUXT blob parse error: %s", exc)
        return None

    # Each player record is a dict object in the blob with fields stored as
    # index references: {'id': 136, 'first_name': 137, 'last_name': 138, 'slug': 141, ...}
    # The actual values live at blob[136], blob[137], etc.
    seen_ids: set[int] = set()
    candidates: list[tuple[str, str]] = []  # (path, slug)
    for item in blob:
        if not (isinstance(item, dict) and "first_name" in item
                and "last_name" in item and "slug" in item and "id" in item):
            continue
        try:
            pid = blob[item["id"]]
            slug = blob[item["slug"]]
        except (IndexError, TypeError):
            continue
        if not (isinstance(pid, int) and isinstance(slug, str) and "-" in slug):
            continue
        if pid in seen_ids:
            continue
        seen_ids.add(pid)
        path_ = f"/sports/{sport}/roster/{slug}/{pid}"
        candidates.append((path_, slug))

    if not candidates:
        logger.debug("NUXT blob strategy: no player candidates found")
        return None

    logger.debug("NUXT blob strategy: found %d candidates", len(candidates))
    return _best_slug_match(candidates, player_name, base_url)


def _best_slug_match(
    candidates: list[tuple[str, str]], player_name: str, base_url: str
) -> Optional[str]:
    """Pick the best matching candidate path by fuzzy-comparing slugs to the player name."""
    name_slug = _name_to_slug(player_name)
    name_parts = player_name.lower().split()

    best_path: Optional[str] = None
    best_score: float = 0.0
    for path_, slug in candidates:
        score = difflib.SequenceMatcher(None, name_slug, slug).ratio()
        if all(part in slug for part in name_parts):
            score = max(score, 0.95)
        if score > best_score:
            best_score = score
            best_path = path_

    threshold = 0.60
    if best_score >= threshold and best_path:
        full_url = f"{base_url}{best_path}"
        logger.info("Matched player '%s' → %s (score=%.2f)", player_name, full_url, best_score)
        return full_url

    logger.debug(
        "No slug match above threshold for '%s' (best=%.2f)", player_name, best_score
    )
    return None


def _name_to_slug(name: str) -> str:
    """Convert 'Roch Cholowsky' → 'roch-cholowsky' for slug comparison."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    s = re.sub(r"\s+", "-", s)
    return s


# ---------------------------------------------------------------------------
# Main auto-detect pipeline
# ---------------------------------------------------------------------------

def auto_detect(
    player_name: str,
    school_name: str,
    sport: str = "baseball",
) -> DetectionResult:
    """
    Full detection pipeline. Returns a DetectionResult.

    On success:
      result.success = True
      result.source  = 'sidearm' | 'ncaa' etc.
      result.player_url = full URL for the player's roster/stats page (if found)
    On failure:
      result.success = False
      result.notes contains human-readable explanation
    """
    result = DetectionResult(school_name=school_name, player_name=player_name)

    # Step 1 — find athletics URL
    logger.info("auto_detect: finding athletics URL for '%s'", school_name)
    athletics_url = discover_athletics_url(school_name, sport)
    if not athletics_url:
        result.notes.append(f"Could not find athletics website for '{school_name}'.")
        return result
    result.athletics_url = athletics_url
    result.notes.append(f"Athletics site: {athletics_url}")

    # Step 2 — fingerprint platform
    logger.info("auto_detect: fingerprinting platform at %s", athletics_url)
    platform = fingerprint_platform(athletics_url, sport)
    result.platform = platform
    result.notes.append(f"Platform detected: {platform}")

    # Step 3 — find player URL based on platform
    if platform in ("sidearm", "sidearm_legacy"):
        result.source = platform  # "sidearm" or "sidearm_legacy"
        player_url = find_player_url_sidearm(athletics_url, player_name, sport)
        if player_url:
            result.player_url = player_url
            result.success = True
            result.notes.append(f"Player roster URL: {player_url}")
        else:
            result.notes.append(
                f"Could not find '{player_name}' on the Sidearm roster at {athletics_url}. "
                "You may need to enter the URL manually."
            )
            result.success = False

    else:  # unknown — all major D1 programs are on Sidearm; reaching here is unusual
        result.source = "ncaa"
        result.notes.append(
            f"{school_name}'s athletics platform could not be identified as Sidearm. "
            "Falling back to NCAA Stats scraper. Enter the NCAA Stats player ID manually."
        )
        result.success = False

    return result


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(name)s: %(message)s")

    test_cases = [
        ("Cameron Flukey", "Coastal Carolina", "baseball"),
    ]

    for pname, school, sport_ in test_cases:
        print(f"\n{'='*60}")
        print(f"Detecting: {pname} @ {school}")
        r = auto_detect(pname, school, sport_)
        print(f"  athletics_url : {r.athletics_url}")
        print(f"  platform      : {r.platform}")
        print(f"  source        : {r.source}")
        print(f"  player_url    : {r.player_url}")
        print(f"  success       : {r.success}")
        for note in r.notes:
            print(f"  note: {note}")
