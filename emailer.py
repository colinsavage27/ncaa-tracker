"""
emailer.py — Format and send nightly box score emails to agents.
"""

from __future__ import annotations

import logging
import os
import smtplib
from collections import defaultdict
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

import database as db

load_dotenv()
logger = logging.getLogger(__name__)

GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "NCAA Player Tracker")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _format_hitter_line(stats: dict) -> str:
    ab = stats.get("ab", 0)
    h = stats.get("h", 0)
    hr = stats.get("hr", 0)
    r = stats.get("r", 0)
    rbi = stats.get("rbi", 0)
    bb = stats.get("bb", 0)
    k = stats.get("k", 0)
    return f"{ab}-for-{h}, {hr} HR, {r} R, {rbi} RBI, {bb} BB, {k} K"


def _format_pitcher_line(stats: dict) -> str:
    ip = stats.get("ip", 0.0)
    h = stats.get("h", 0)
    r = stats.get("r", 0)
    er = stats.get("er", 0)
    bb = stats.get("bb", 0)
    hbp = stats.get("hbp", 0)
    k = stats.get("k", 0)
    return f"{ip} IP, {h} H, {r} R, {er} ER, {bb} BB, {hbp} HBP, {k} K"


def _format_score_line(stats: dict, player: dict) -> str:
    """Return 'Team A Score — Team B Score' line."""
    team = stats.get("team_name") or player.get("school", "Unknown")
    opponent = stats.get("opponent", "Unknown")
    team_score = stats.get("team_score", 0)
    opp_score = stats.get("opp_score", 0)
    return f"{team} {team_score} — {opponent} {opp_score}"


def format_player_block(player: dict, stats: dict) -> str:
    """Return a plain-text block for one player."""
    header = f"{player['player_name']} — {player['school']}"
    if player["position"] == "pitcher":
        stat_line = _format_pitcher_line(stats)
    else:
        stat_line = _format_hitter_line(stats)
    score_line = _format_score_line(stats, player)
    return f"{header}\n{stat_line}\n{score_line}"


def build_email_body(agent_name: str, player_rows: list[dict], report_date: str) -> str:
    """Build the full plain-text email body for one agent."""
    lines = [
        f"Good morning {agent_name},",
        "",
        f"Here are your clients' box scores from {report_date}:",
        "",
        "-" * 50,
    ]
    for row in player_rows:
        stats = row["stats"]
        block = format_player_block(row, stats)
        lines.append(block)
        lines.append("-" * 50)

    lines += [
        "",
        "This report was generated automatically by your agency's player tracker.",
        "Reply to this email if you have any questions.",
    ]
    return "\n".join(lines)


def build_html_email_body(agent_name: str, player_rows: list[dict], report_date: str) -> str:
    """Build an HTML version of the email body."""
    blocks = []
    for row in player_rows:
        stats = row["stats"]
        player_name = row["player_name"]
        school = row["school"]
        position = row["position"]

        if position == "pitcher":
            stat_line = _format_pitcher_line(stats)
        else:
            stat_line = _format_hitter_line(stats)

        score_line = _format_score_line(stats, row)

        blocks.append(f"""
        <div style="border-bottom:1px solid #ddd; padding:12px 0;">
          <strong style="font-size:15px;">{player_name}</strong>
          <span style="color:#555;"> — {school}</span><br>
          <span style="font-family:monospace; font-size:13px;">{stat_line}</span><br>
          <span style="color:#666; font-size:12px;">{score_line}</span>
        </div>
        """)

    blocks_html = "\n".join(blocks)

    return f"""
    <html><body style="font-family:Arial,sans-serif; max-width:600px; margin:auto; color:#222;">
      <h2 style="color:#1a3a5c;">NCAA Player Tracker — {report_date}</h2>
      <p>Good morning {agent_name},</p>
      <p>Here are your clients' box scores from <strong>{report_date}</strong>:</p>
      {blocks_html}
      <p style="font-size:11px; color:#999; margin-top:20px;">
        This report was generated automatically by your agency's player tracker.
      </p>
    </body></html>
    """


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------


def _send_email(to_email: str, subject: str, plain_body: str, html_body: str):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        raise RuntimeError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in .env"
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{EMAIL_FROM_NAME} <{GMAIL_USER}>"
    msg["To"] = to_email

    msg.attach(MIMEText(plain_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, to_email, msg.as_string())

    logger.info("Email sent to %s", to_email)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def send_nightly_emails(target_date: str | None = None) -> int:
    """
    Fetch all unsent game logs for target_date (defaults to yesterday),
    group by agent, and send one email per agent.

    Returns the number of emails sent.
    """
    if target_date is None:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    logs = db.get_unsent_logs_for_date(target_date)
    if not logs:
        logger.info("No unsent game logs for %s — no emails to send", target_date)
        return 0

    # Group by agent
    by_agent: dict[int, list[dict]] = defaultdict(list)
    for row in logs:
        agent_id = row.get("assigned_agent_id")
        if agent_id is None:
            logger.warning(
                "Player %s has no assigned agent — skipping email", row["player_name"]
            )
            continue
        by_agent[agent_id].append(row)

    if not by_agent:
        logger.info("No players with assigned agents played on %s", target_date)
        return 0

    emails_sent = 0
    all_sent_log_ids: list[int] = []

    for agent_id, player_rows in by_agent.items():
        agent = db.get_agent(agent_id)
        if agent is None:
            logger.error("Agent id=%d not found in DB", agent_id)
            continue

        agent_name = agent["name"]
        agent_email = agent["email"]

        logger.info(
            "Preparing email for %s (%s) — %d player(s)",
            agent_name,
            agent_email,
            len(player_rows),
        )

        report_date_display = target_date  # YYYY-MM-DD; fine for the email
        subject = f"Player Box Scores — {report_date_display}"

        plain_body = build_email_body(agent_name, player_rows, report_date_display)
        html_body = build_html_email_body(agent_name, player_rows, report_date_display)

        try:
            _send_email(agent_email, subject, plain_body, html_body)
            emails_sent += 1
            all_sent_log_ids.extend(row["log_id"] for row in player_rows)
        except Exception as exc:
            logger.error(
                "Failed to send email to %s (%s): %s",
                agent_name,
                agent_email,
                exc,
            )

    # Mark successfully emailed logs as sent
    if all_sent_log_ids:
        db.mark_logs_sent(all_sent_log_ids)

    logger.info("Nightly email job complete. %d email(s) sent.", emails_sent)
    return emails_sent
