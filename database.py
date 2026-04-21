"""
database.py — SQLite setup and all DB queries for the agency tracker.
"""

from __future__ import annotations

import os
import sqlite3
import json
import logging
from datetime import date
from pathlib import Path

# DATA_DIR lets Railway (or any host) mount a persistent volume and point the
# database at it.  Defaults to the directory that contains this file, which is
# correct for local development.
_data_dir = Path(os.environ.get("DATA_DIR", Path(__file__).parent))
_data_dir.mkdir(parents=True, exist_ok=True)
DB_PATH = _data_dir / "tracker.db"
logger = logging.getLogger(__name__)


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode=WAL")   # allows concurrent reads + one writer
    conn.execute("PRAGMA busy_timeout=10000")  # wait up to 10s if DB is locked
    return conn


def init_db():
    """Create all tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS agents (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                name  TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS players (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                name                 TEXT NOT NULL,
                school               TEXT NOT NULL,
                ncaa_player_id       TEXT,
                ncaa_team_id         TEXT,
                position             TEXT NOT NULL CHECK(position IN ('hitter', 'pitcher')),
                source               TEXT NOT NULL DEFAULT 'ncaa',
                sidearm_schedule_url TEXT,
                assigned_agent_id    INTEGER REFERENCES agents(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS games_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id   INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                game_date   TEXT NOT NULL,
                stats       TEXT NOT NULL,
                sent        INTEGER NOT NULL DEFAULT 0,
                UNIQUE(player_id, game_date)
            );
        """)
        # Migrate existing databases that pre-date the source/sidearm columns
        for col, definition in [
            ("source", "TEXT NOT NULL DEFAULT 'ncaa'"),
            ("sidearm_schedule_url", "TEXT"),
            ("scrape_status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("scrape_error", "TEXT DEFAULT ''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE players ADD COLUMN {col} {definition}")
            except Exception:
                pass  # Column already exists
    logger.info("Database initialized at %s", DB_PATH)


# ---------------------------------------------------------------------------
# Agent queries
# ---------------------------------------------------------------------------

def add_agent(name: str, email: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO agents (name, email) VALUES (?, ?)", (name, email)
        )
        return cur.lastrowid


def remove_agent(agent_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM agents WHERE id = ?", (agent_id,))


def get_all_agents() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM agents ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def get_agent(agent_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM agents WHERE id = ?", (agent_id,)).fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Player queries
# ---------------------------------------------------------------------------

def add_player(
    name: str,
    school: str,
    ncaa_player_id: str,
    ncaa_team_id: str,
    position: str,
    assigned_agent_id: int | None,
    source: str = "ncaa",
    sidearm_url: str | None = None,
) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO players
               (name, school, ncaa_player_id, ncaa_team_id, position,
                assigned_agent_id, source, sidearm_schedule_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, school, ncaa_player_id, ncaa_team_id, position,
             assigned_agent_id, source, sidearm_url),
        )
        return cur.lastrowid


def remove_player(player_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM players WHERE id = ?", (player_id,))


def get_all_players() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT p.*, a.name as agent_name
               FROM players p
               LEFT JOIN agents a ON p.assigned_agent_id = a.id
               ORDER BY a.name, p.name"""
        ).fetchall()
        return [dict(r) for r in rows]


def get_player(player_id: int) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM players WHERE id = ?", (player_id,)
        ).fetchone()
        return dict(row) if row else None


def get_players_by_agent(agent_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM players WHERE assigned_agent_id = ? ORDER BY name",
            (agent_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def update_player_agent(player_id: int, agent_id: int | None):
    with get_conn() as conn:
        conn.execute(
            "UPDATE players SET assigned_agent_id = ? WHERE id = ?",
            (agent_id, player_id),
        )


def update_player_scrape_status(player_id: int, status: str, error: str = ""):
    with get_conn() as conn:
        conn.execute(
            "UPDATE players SET scrape_status = ?, scrape_error = ? WHERE id = ?",
            (status, error or "", player_id),
        )

def update_player_ncaa_id(player_id: int, ncaa_player_id: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE players SET ncaa_player_id = ? WHERE id = ?",
            (ncaa_player_id, player_id),
        )


def update_player_source(player_id: int, source: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE players SET source = ? WHERE id = ?",
            (source, player_id),
        )


# ---------------------------------------------------------------------------
# Games log queries
# ---------------------------------------------------------------------------

def upsert_game_log(player_id: int, game_date: str, stats: dict) -> int:
    """Insert or replace a game log entry. Returns the row id."""
    with get_conn() as conn:
        cur = conn.execute(
            """INSERT INTO games_log (player_id, game_date, stats, sent)
               VALUES (?, ?, ?, 0)
               ON CONFLICT(player_id, game_date)
               DO UPDATE SET stats = excluded.stats""",
            (player_id, game_date, json.dumps(stats)),
        )
        return cur.lastrowid


def get_unsent_logs_for_date(game_date: str) -> list[dict]:
    """Return all unsent game log rows with joined player and agent info."""
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT gl.id as log_id,
                      gl.game_date,
                      gl.stats,
                      p.id as player_id,
                      p.name as player_name,
                      p.school,
                      p.position,
                      p.assigned_agent_id,
                      a.name as agent_name,
                      a.email as agent_email
               FROM games_log gl
               JOIN players p ON gl.player_id = p.id
               LEFT JOIN agents a ON p.assigned_agent_id = a.id
               WHERE gl.game_date = ? AND gl.sent = 0
               ORDER BY a.name, p.name""",
            (game_date,),
        ).fetchall()
        result = []
        for r in rows:
            row = dict(r)
            row["stats"] = json.loads(row["stats"])
            result.append(row)
        return result


def mark_logs_sent(log_ids: list[int]):
    if not log_ids:
        return
    placeholders = ",".join("?" * len(log_ids))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE games_log SET sent = 1 WHERE id IN ({placeholders})", log_ids
        )


def get_recent_logs(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT gl.*, p.name as player_name, p.school
               FROM games_log gl
               JOIN players p ON gl.player_id = p.id
               ORDER BY gl.game_date DESC, p.name
               LIMIT ?""",
            (limit,),
        ).fetchall()
        result = []
        for r in rows:
            row = dict(r)
            row["stats"] = json.loads(row["stats"])
            result.append(row)
        return result
