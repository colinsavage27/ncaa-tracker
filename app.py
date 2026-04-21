"""
app.py — Flask web UI for managing agents and players.

Pages:
  /               Roster view grouped by agent
  /agents         Add/remove agents
  /players        Add/remove players
  /players/add    Add player form
  /logs           Recent game log entries (debugging)
"""

import logging
import os
import re
import sys
import threading
import time
from urllib.parse import urlparse, parse_qs

import schedule
from flask import Flask, redirect, render_template, request, flash, url_for
from dotenv import load_dotenv
import platform_detector as pd

load_dotenv()

# ---------------------------------------------------------------------------
# Logging — stdout always; file only when LOG_FILE env var is set
# (Railway captures stdout; no persistent file needed in production)
# ---------------------------------------------------------------------------
_log_handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
_log_file = os.environ.get("LOG_FILE")
if _log_file:
    _log_handlers.append(logging.FileHandler(_log_file))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=_log_handlers,
)
logger = logging.getLogger(__name__)

import database as db

db.init_db()

app = Flask(__name__)
# SECRET_KEY must be set in Railway environment variables.
# A missing key still works (flash messages won't persist across restarts).
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-insecure-key")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_ncaa_ids(url_or_id: str) -> tuple[str, str]:
    """
    Accept either:
      - A raw player ID: "6152438"
      - A full NCAA Stats URL: https://stats.ncaa.org/players/6152438
        or https://stats.ncaa.org/player/game_log?game_sport_year_ctl_id=...&player_id=...

    Returns (ncaa_player_id, ncaa_team_id).  team_id may be empty string.
    """
    url_or_id = url_or_id.strip()

    # Plain numeric ID
    if re.fullmatch(r"\d+", url_or_id):
        return url_or_id, ""

    # URL form
    try:
        parsed = urlparse(url_or_id)
        qs = parse_qs(parsed.query)

        # /players/{id} path format
        path_match = re.search(r"/players/(\d+)", parsed.path)
        if path_match:
            player_id = path_match.group(1)
            team_id = qs.get("org_id", [""])[0]
            return player_id, team_id

        # Query-string format: player_id=...
        player_id = qs.get("player_id", [""])[0]
        team_id = qs.get("org_id", qs.get("team_id", [""]))[0]
        if player_id:
            return player_id, team_id
    except Exception:
        pass

    # Return as-is if we couldn't parse it — let the user fix it
    return url_or_id, ""


def _background_ncaa_lookup(player_id: int, name: str, school: str):
    """Background thread: find NCAA player ID via search and store it (ID only, no status change)."""
    try:
        ncaa_id = pd.search_ncaa_player_id(name, school)
        if ncaa_id:
            db.update_player_ncaa_id(player_id, ncaa_id)
            logger.info("Background NCAA ID lookup: found %s for %s", ncaa_id, name)
        else:
            logger.info("Background NCAA ID lookup: no ID found for %s", name)
    except Exception as exc:
        logger.warning("Background NCAA ID lookup failed for %s: %s", name, exc)


def _background_verify_player(player_id: int, name: str, school: str):
    """
    Background thread: find NCAA player ID if missing, then test connectivity
    and update scrape_status to 'verified' or 'failed'.
    """
    import scraper as sc
    try:
        player = db.get_player(player_id)
        if not player:
            return

        # Step 1: find NCAA ID if missing (needed for fallback)
        if not player.get("ncaa_player_id"):
            ncaa_id = pd.search_ncaa_player_id(name, school)
            if ncaa_id:
                db.update_player_ncaa_id(player_id, ncaa_id)
                player = db.get_player(player_id)  # refresh with new ID
                logger.info("Found NCAA ID %s for %s", ncaa_id, name)
            else:
                logger.info("No NCAA ID found for %s via search", name)

        # Step 2: test connectivity and update status
        success, error = sc.test_player_connectivity(player)
        if success:
            db.update_player_scrape_status(player_id, "verified", "")
            logger.info("Player %s verified — stats source reachable", name)
        else:
            db.update_player_scrape_status(player_id, "failed", error or "Could not reach stats")
            logger.warning("Player %s verification failed: %s", name, error)

    except Exception as exc:
        logger.warning("Background verify failed for player_id=%d (%s): %s", player_id, name, exc)
        try:
            db.update_player_scrape_status(player_id, "failed", str(exc))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Routes — Roster
# ---------------------------------------------------------------------------


@app.route("/")
def roster():
    agents = db.get_all_agents()
    players = db.get_all_players()

    # Group players by agent
    by_agent: dict = {}
    unassigned = []
    player_map = {a["id"]: [] for a in agents}
    for p in players:
        aid = p.get("assigned_agent_id")
        if aid and aid in player_map:
            player_map[aid].append(p)
        else:
            unassigned.append(p)

    return render_template(
        "roster.html",
        agents=agents,
        player_map=player_map,
        unassigned=unassigned,
    )


# ---------------------------------------------------------------------------
# Routes — Agents
# ---------------------------------------------------------------------------


@app.route("/agents")
def agents():
    all_agents = db.get_all_agents()
    return render_template("agents.html", agents=all_agents)


@app.route("/agents/add", methods=["POST"])
def add_agent():
    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip().lower()

    if not name or not email:
        flash("Name and email are required.", "error")
        return redirect(url_for("agents"))

    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("agents"))

    try:
        db.add_agent(name, email)
        flash(f"Agent '{name}' added successfully.", "success")
    except Exception as exc:
        logger.error("Error adding agent: %s", exc)
        flash(f"Could not add agent: {exc}", "error")

    return redirect(url_for("agents"))


@app.route("/agents/remove/<int:agent_id>", methods=["POST"])
def remove_agent(agent_id: int):
    agent = db.get_agent(agent_id)
    if not agent:
        flash("Agent not found.", "error")
        return redirect(url_for("agents"))

    db.remove_agent(agent_id)
    flash(f"Agent '{agent['name']}' removed.", "success")
    return redirect(url_for("agents"))


# ---------------------------------------------------------------------------
# Routes — Players
# ---------------------------------------------------------------------------


@app.route("/players")
def players():
    all_players = db.get_all_players()
    all_agents = db.get_all_agents()
    return render_template("players.html", players=all_players, agents=all_agents)


@app.route("/players/add", methods=["GET", "POST"])
def add_player():
    all_agents = db.get_all_agents()

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        school = request.form.get("school", "").strip()
        ncaa_url = request.form.get("ncaa_url", "").strip()
        position = request.form.get("position", "").strip().lower()
        agent_id_raw = request.form.get("assigned_agent_id", "").strip()

        errors = []
        if not name:
            errors.append("Player name is required.")
        if not school:
            errors.append("School is required.")
        if position not in ("hitter", "pitcher"):
            errors.append("Position must be 'hitter' or 'pitcher'.")

        if errors:
            for e in errors:
                flash(e, "error")
            return render_template("add_player.html", agents=all_agents, form=request.form)

        assigned_agent_id = int(agent_id_raw) if agent_id_raw.isdigit() else None

        # --- If an NCAA URL was provided, skip auto-detection and use it directly ---
        if ncaa_url:
            ncaa_player_id, ncaa_team_id = _extract_ncaa_ids(ncaa_url)
            try:
                player_db_id = db.add_player(
                    name=name,
                    school=school,
                    ncaa_player_id=ncaa_player_id,
                    ncaa_team_id=ncaa_team_id,
                    position=position,
                    assigned_agent_id=assigned_agent_id,
                    source="ncaa",
                )
                _t = threading.Thread(target=_background_verify_player, args=(player_db_id, name, school), daemon=True)
                _t.start()
                flash(f"Player '{name}' added with NCAA scraper.", "success")
                return redirect(url_for("players"))
            except Exception as exc:
                logger.error("Error adding player: %s", exc)
                flash(f"Could not add player: {exc}", "error")
                return render_template("add_player.html", agents=all_agents, form=request.form)

        # --- Auto-detect platform and find player URL ---
        flash(f"Auto-detecting platform for {name} at {school}…", "success")
        try:
            detection = pd.auto_detect(name, school, sport="baseball")
        except Exception as exc:
            logger.error("Platform detection error: %s", exc)
            flash(f"Auto-detection failed: {exc}. Enter NCAA Stats URL manually.", "error")
            return render_template("add_player.html", agents=all_agents, form=request.form)

        for note in detection.notes:
            logger.info("Detection note: %s", note)

        if detection.source in ("sidearm", "sidearm_legacy") and detection.player_url:
            try:
                player_db_id = db.add_player(
                    name=name,
                    school=school,
                    ncaa_player_id="",
                    ncaa_team_id="",
                    position=position,
                    assigned_agent_id=assigned_agent_id,
                    source=detection.source,
                    sidearm_url=detection.player_url,
                )
                _t = threading.Thread(target=_background_verify_player, args=(player_db_id, name, school), daemon=True)
                _t.start()
                flash(
                    f"Player '{name}' added with Sidearm scraper. "
                    f"Roster URL: {detection.player_url}",
                    "success",
                )
                return redirect(url_for("players"))
            except Exception as exc:
                logger.error("Error adding player: %s", exc)
                flash(f"Could not add player: {exc}", "error")
                return render_template("add_player.html", agents=all_agents, form=request.form)

        # Could not auto-detect fully — show informative message and ask for manual input
        if detection.platform in ("sidearm", "sidearm_legacy"):
            platform_msg = (
                f"{school} uses Sidearm Sports but '{name}' wasn't found on the roster automatically. "
                "Enter the player's Sidearm roster URL below "
                f"(e.g. {detection.athletics_url}/sports/baseball/roster/firstname-lastname/12345)."
            )
        else:
            platform_msg = (
                f"Could not identify the athletics platform for {school}. "
                "Please enter the NCAA Stats player ID below."
            )

        flash(platform_msg, "error")
        return render_template(
            "add_player.html",
            agents=all_agents,
            form=request.form,
            detection=detection,
        )

    return render_template("add_player.html", agents=all_agents, form={}, detection=None)


@app.route("/players/remove/<int:player_id>", methods=["POST"])
def remove_player(player_id: int):
    player = db.get_player(player_id)
    if not player:
        flash("Player not found.", "error")
        return redirect(url_for("players"))

    db.remove_player(player_id)
    flash(f"Player '{player['name']}' removed.", "success")
    return redirect(url_for("players"))


@app.route("/players/assign/<int:player_id>", methods=["POST"])
def assign_player(player_id: int):
    agent_id_raw = request.form.get("assigned_agent_id", "").strip()
    agent_id = int(agent_id_raw) if agent_id_raw.isdigit() else None
    db.update_player_agent(player_id, agent_id)
    flash("Assignment updated.", "success")
    return redirect(url_for("players"))


@app.route("/players/verify/<int:player_id>", methods=["POST"])
def verify_player(player_id: int):
    """Test connectivity for this player and update scrape_status to verified/failed."""
    player = db.get_player(player_id)
    if not player:
        flash("Player not found.", "error")
        return redirect(url_for("players"))
    db.update_player_scrape_status(player_id, "pending", "")
    _t = threading.Thread(
        target=_background_verify_player,
        args=(player_id, player["name"], player["school"]),
        daemon=True,
    )
    _t.start()
    flash(f"Checking {player['name']} — refresh in 30 seconds to see the result.", "success")
    return redirect(url_for("players"))


@app.route("/players/set-ncaa-id/<int:player_id>", methods=["POST"])
def set_ncaa_id(player_id: int):
    """Manually set a player's NCAA stats player ID."""
    player = db.get_player(player_id)
    if not player:
        flash("Player not found.", "error")
        return redirect(url_for("players"))
    ncaa_id = request.form.get("ncaa_player_id", "").strip()
    if not ncaa_id.isdigit():
        flash("Please enter a valid numeric NCAA player ID.", "error")
        return redirect(url_for("players"))
    db.update_player_ncaa_id(player_id, ncaa_id)
    db.update_player_source(player_id, "ncaa")
    db.update_player_scrape_status(player_id, "verified", "")
    flash(f"NCAA ID saved for {player['name']} — ✓ Verified. They'll be scraped tonight.", "success")
    return redirect(url_for("players"))


# ---------------------------------------------------------------------------
# Routes — Logs (debug view) + manual stat entry
# ---------------------------------------------------------------------------


@app.route("/logs")
def logs():
    recent = db.get_recent_logs(limit=200)
    all_players = db.get_all_players()
    all_agents = db.get_all_agents()
    return render_template("logs.html", logs=recent, players=all_players, agents=all_agents)


@app.route("/logs/lookup", methods=["POST"])
def statline_lookup():
    """
    Statline Lookup — scrape a player's stats for a specific date and email
    the result immediately to the chosen agent.
    """
    import scraper as sc
    from emailer import build_email_body, build_html_email_body, _send_email

    player_id_raw = request.form.get("player_id", "").strip()
    agent_id_raw  = request.form.get("agent_id", "").strip()
    game_date     = request.form.get("game_date", "").strip()

    if not player_id_raw or not game_date:
        flash("Player and date are required.", "error")
        return redirect(url_for("logs"))

    player = db.get_player(int(player_id_raw))
    if not player:
        flash("Player not found.", "error")
        return redirect(url_for("logs"))

    agent = db.get_agent(int(agent_id_raw)) if agent_id_raw.isdigit() else None
    if not agent:
        flash("Please select an agent to send the statline to.", "error")
        return redirect(url_for("logs"))

    # Try primary scraper for that date; fall back to NCAA if we have an ID
    source = player.get("source", "ncaa")
    stats = None

    try:
        primary = sc.get_scraper(source)
        if hasattr(primary, "fetch_game_for_date"):
            stats = primary.fetch_game_for_date(player, game_date)
    except Exception as exc:
        logger.warning("Primary scraper lookup failed for %s: %s", player["name"], exc)

    if stats is None and source != "ncaa" and player.get("ncaa_player_id"):
        logger.info("Falling back to NCAA scraper for lookup: %s on %s", player["name"], game_date)
        try:
            stats = sc.NCAAScraper().fetch_game_for_date(player, game_date)
        except Exception as exc:
            logger.warning("NCAA fallback lookup failed for %s: %s", player["name"], exc)

    if stats is None:
        flash(
            f"No stats found for {player['name']} on {game_date}. "
            "The player may not have played that day, or the date is outside "
            "the current season's game log.",
            "error",
        )
        return redirect(url_for("logs"))

    # Persist to logs (unsent=0 so it won't be double-sent by the nightly job)
    log_id = db.upsert_game_log(player["id"], game_date, stats)
    with db.get_conn() as conn:
        conn.execute("UPDATE games_log SET sent = 1 WHERE id = ?", (log_id,))

    # Build and send the email
    player_row = {
        "player_name": player["name"],
        "school":      player["school"],
        "position":    player["position"],
        "stats":       stats,
    }
    subject    = f"Statline Lookup — {player['name']} on {game_date}"
    plain_body = build_email_body(agent["name"], [player_row], game_date)
    html_body  = build_html_email_body(agent["name"], [player_row], game_date)

    try:
        _send_email(agent["email"], subject, plain_body, html_body)
        flash(
            f"Statline for {player['name']} on {game_date} sent to "
            f"{agent['name']} ({agent['email']}).",
            "success",
        )
    except Exception as exc:
        logger.error("Email send failed for statline lookup: %s", exc)
        flash(f"Stats found but email failed: {exc}", "error")

    return redirect(url_for("logs"))


@app.route("/admin/run-now", methods=["POST"])
def admin_run_now():
    """Trigger the nightly scrape+email job immediately."""
    import threading
    from scheduler import run_nightly_job
    t = threading.Thread(target=run_nightly_job, daemon=True)
    t.start()
    flash("Nightly job triggered — scraping and emailing now. Check back in 1–2 minutes.", "success")
    return redirect(url_for("logs"))


@app.route("/logs/delete/<int:log_id>", methods=["POST"])
def delete_log(log_id: int):
    with db.get_conn() as conn:
        conn.execute("DELETE FROM games_log WHERE id = ?", (log_id,))
    flash("Log entry deleted.", "success")
    return redirect(url_for("logs"))


# ---------------------------------------------------------------------------
# Background scheduler thread
# ---------------------------------------------------------------------------
# The nightly job (scrape + email) runs inside the same process as the web UI.
# This avoids the need for a separate worker process on Railway's free tier.
#
# Guard:  Werkzeug's debug reloader spawns a child process with WERKZEUG_RUN_MAIN=true.
#         We only start the thread in that child (or in gunicorn where debug is off),
#         never in the reloader parent, so the job doesn't fire twice during dev.
# ---------------------------------------------------------------------------

def _scheduler_loop() -> None:
    """Daemon thread: schedules and runs the nightly scrape+email job."""
    from scheduler import run_nightly_job
    RUN_AT = os.environ.get("NIGHTLY_RUN_AT", "23:00")
    schedule.every().day.at(RUN_AT).do(run_nightly_job)
    logger.info("Scheduler thread ready — nightly job scheduled at %s UTC.", RUN_AT)
    while True:
        schedule.run_pending()
        time.sleep(30)


_in_reloader_parent = app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true"
if not _in_reloader_parent:
    _sched_thread = threading.Thread(
        target=_scheduler_loop, daemon=True, name="nightly-scheduler"
    )
    _sched_thread.start()


# ---------------------------------------------------------------------------
# Run (local dev only — gunicorn is used in production)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    debug = os.environ.get("FLASK_DEBUG", "1") == "1"
    app.run(debug=debug, port=port, host="0.0.0.0")
