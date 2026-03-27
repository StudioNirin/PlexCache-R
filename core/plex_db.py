"""
Plex Database direct read for PlexCache.
Fallback for shared users without tokens — queries the Plex Media Server
SQLite database to reconstruct OnDeck items.
"""

import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from core.plex_api import OnDeckItem


# SQLite busy timeout (ms) — how long to wait if Plex has the DB locked
DB_BUSY_TIMEOUT_MS = 5000


def fetch_on_deck_from_db(
    db_path: str,
    usernames: List[str],
    valid_sections: List[int],
    days_to_monitor: int,
    number_episodes: int,
    user_id_map: Dict[str, int]
) -> List[OnDeckItem]:
    """Fetch OnDeck items for shared users by querying the Plex SQLite database.

    This is a fallback for users who have no API token. It queries the Plex
    database directly to find partially watched movies and next episodes for
    recently watched TV shows.

    Args:
        db_path: Path to Plex's com.plexapp.plugins.library.db file.
        usernames: List of usernames to fetch OnDeck for.
        valid_sections: Plex library section IDs to include.
        days_to_monitor: Only include items viewed within this many days.
        number_episodes: Number of next episodes to prefetch per show.
        user_id_map: Pre-mapped {username: plex_account_id} from settings.

    Returns:
        List of OnDeckItem objects, same format as the API-based fetch.
    """
    if not db_path:
        return []

    # If pointed at a directory, look for the Plex database file inside it
    if os.path.isdir(db_path):
        candidate = os.path.join(db_path, "com.plexapp.plugins.library.db")
        if os.path.isfile(candidate):
            logging.debug(f"[DB FALLBACK] Auto-detected database: {candidate}")
            db_path = candidate
        else:
            logging.warning(f"[DB FALLBACK] Directory given but com.plexapp.plugins.library.db not found in: {db_path}")
            return []

    if not os.path.isfile(db_path):
        logging.warning(f"[DB FALLBACK] Plex database not found: {db_path}")
        return []

    results: List[OnDeckItem] = []

    try:
        conn = _connect(db_path)
    except sqlite3.Error as e:
        logging.warning(f"[DB FALLBACK] Failed to open Plex database: {e}")
        return []

    try:
        # Resolve usernames to account IDs
        resolved_ids = _resolve_account_ids(conn, usernames, user_id_map)

        cutoff = datetime.now() - timedelta(days=days_to_monitor)

        for username in usernames:
            account_id = resolved_ids.get(username)
            if account_id is None:
                logging.warning(f"[DB FALLBACK] Could not resolve account ID for {username} — skipping")
                continue

            try:
                tv_items = _fetch_tv_on_deck(conn, account_id, username, valid_sections, cutoff, number_episodes)
                movie_items = _fetch_movie_on_deck(conn, account_id, username, valid_sections, cutoff)
                results.extend(tv_items)
                results.extend(movie_items)
                logging.info(f"[DB FALLBACK] [USER:{username}] Found {len(tv_items)} TV + {len(movie_items)} movie OnDeck items")
            except sqlite3.OperationalError as e:
                logging.warning(f"[DB FALLBACK] Database error for {username}: {e}")
            except Exception as e:
                logging.error(f"[DB FALLBACK] Unexpected error for {username}: {e}")
    finally:
        conn.close()

    return results


def _connect(db_path: str) -> sqlite3.Connection:
    """Open the Plex database read-only with busy timeout."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS}")
    return conn


def _resolve_account_ids(
    conn: sqlite3.Connection,
    usernames: List[str],
    user_id_map: Dict[str, int]
) -> Dict[str, int]:
    """Resolve usernames to Plex account IDs.

    Uses pre-mapped IDs from settings first, falls back to the DB accounts table.
    """
    resolved: Dict[str, int] = {}
    unmapped: List[str] = []

    for username in usernames:
        if username in user_id_map and user_id_map[username]:
            resolved[username] = user_id_map[username]
        else:
            unmapped.append(username)

    if unmapped:
        try:
            cursor = conn.execute("SELECT id, name FROM accounts")
            db_accounts = {row["name"]: row["id"] for row in cursor.fetchall()}
            for username in unmapped:
                # Case-insensitive match
                for db_name, db_id in db_accounts.items():
                    if db_name.lower() == username.lower():
                        resolved[username] = db_id
                        break
        except sqlite3.OperationalError as e:
            logging.warning(f"[DB FALLBACK] Failed to query accounts table: {e}")

    return resolved


def _fetch_tv_on_deck(
    conn: sqlite3.Connection,
    account_id: int,
    username: str,
    valid_sections: List[int],
    cutoff: datetime,
    number_episodes: int
) -> List[OnDeckItem]:
    """Find next unwatched episodes for recently watched shows."""
    items: List[OnDeckItem] = []

    # Step 1: Get most recently watched episode per show
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    recent_shows = _get_recent_watched_shows(conn, account_id, valid_sections, cutoff_str)

    # Step 2: For each show, find the next unwatched episode
    for show_title, last_season, last_episode, library_section_id in recent_shows:
        next_episodes = _find_next_episodes(
            conn, show_title, last_season, last_episode,
            library_section_id, number_episodes
        )

        if not next_episodes:
            logging.debug(f"[DB FALLBACK] [USER:{username}] {show_title} — caught up, no next episode")
            continue

        for i, (metadata_id, ep_title, season_idx, ep_idx, rating_key) in enumerate(next_episodes):
            file_path = _resolve_file_path(conn, metadata_id)
            if not file_path:
                logging.debug(f"[DB FALLBACK] No file path for metadata_id={metadata_id} ({show_title} S{season_idx:02d}E{ep_idx:02d})")
                continue

            items.append(OnDeckItem(
                file_path=file_path,
                username=username,
                episode_info={
                    'show': show_title,
                    'season': season_idx,
                    'episode': ep_idx
                },
                is_current_ondeck=(i == 0),
                rating_key=str(rating_key) if rating_key else None
            ))

    return items


def _get_recent_watched_shows(
    conn: sqlite3.Connection,
    account_id: int,
    valid_sections: List[int],
    cutoff_str: str
) -> List[Tuple[str, int, int, int]]:
    """Get the most recently watched episode per show for a user.

    Returns list of (show_title, last_season_index, last_episode_index, library_section_id).
    """
    if not valid_sections:
        return []

    placeholders = ",".join("?" for _ in valid_sections)
    query = f"""
        SELECT grandparent_title, parent_index, "index", library_section_id,
               MAX(viewed_at) as last_viewed
        FROM metadata_item_views
        WHERE account_id = ?
          AND grandparent_title IS NOT NULL
          AND grandparent_title != ''
          AND parent_index IS NOT NULL
          AND "index" IS NOT NULL
          AND viewed_at >= ?
          AND library_section_id IN ({placeholders})
        GROUP BY grandparent_title
        ORDER BY last_viewed DESC
    """
    params = [account_id, cutoff_str] + list(valid_sections)
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    results = []
    for row in rows:
        results.append((
            row["grandparent_title"],
            int(row["parent_index"]),
            int(row["index"]),
            int(row["library_section_id"])
        ))

    return results


def _find_next_episodes(
    conn: sqlite3.Connection,
    show_title: str,
    last_season: int,
    last_episode: int,
    library_section_id: int,
    number_episodes: int
) -> List[Tuple[int, str, int, int, int]]:
    """Find the next unwatched episodes after a given season/episode position.

    Handles season boundaries naturally (S01 last ep -> S02E01).

    Returns list of (metadata_item_id, episode_title, season_index, episode_index, rating_key).
    """
    # number_episodes is how many to prefetch AFTER the OnDeck episode
    limit = number_episodes + 1

    query = """
        SELECT mi.id, mi.title, season."index" as season_index, mi."index" as episode_index,
               mi.id as rating_key
        FROM metadata_items mi
        JOIN metadata_items season ON mi.parent_id = season.id
        JOIN metadata_items show ON season.parent_id = show.id
        WHERE show.title = ?
          AND show.metadata_type = 2
          AND show.library_section_id = ?
          AND mi.metadata_type = 4
          AND (season."index" > ?
               OR (season."index" = ? AND mi."index" > ?))
        ORDER BY season."index" ASC, mi."index" ASC
        LIMIT ?
    """
    params = [show_title, library_section_id, last_season, last_season, last_episode, limit]
    cursor = conn.execute(query, params)
    rows = cursor.fetchall()

    return [
        (row["id"], row["title"], int(row["season_index"]), int(row["episode_index"]), row["rating_key"])
        for row in rows
    ]


def _fetch_movie_on_deck(
    conn: sqlite3.Connection,
    account_id: int,
    username: str,
    valid_sections: List[int],
    cutoff: datetime
) -> List[OnDeckItem]:
    """Find partially watched movies (started but not finished)."""
    items: List[OnDeckItem] = []

    if not valid_sections:
        return items

    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    placeholders = ",".join("?" for _ in valid_sections)

    query = f"""
        SELECT mi.id, mi.title, mi.id as rating_key
        FROM metadata_item_settings mis
        JOIN metadata_items mi ON mi.guid = mis.guid
        WHERE mis.account_id = ?
          AND mis.view_offset > 0
          AND (mis.view_count IS NULL OR mis.view_count = 0)
          AND mi.metadata_type = 1
          AND mi.library_section_id IN ({placeholders})
          AND mis.last_viewed_at >= ?
    """
    params = [account_id] + list(valid_sections) + [cutoff_str]
    cursor = conn.execute(query, params)

    for row in cursor.fetchall():
        file_path = _resolve_file_path(conn, row["id"])
        if not file_path:
            logging.debug(f"[DB FALLBACK] No file path for movie metadata_id={row['id']} ({row['title']})")
            continue

        items.append(OnDeckItem(
            file_path=file_path,
            username=username,
            episode_info=None,
            is_current_ondeck=True,
            rating_key=str(row["rating_key"]) if row["rating_key"] else None
        ))

    return items


def _resolve_file_path(conn: sqlite3.Connection, metadata_item_id: int) -> Optional[str]:
    """Resolve a metadata item ID to its file path via media_items -> media_parts."""
    query = """
        SELECT mp.file
        FROM media_items mai
        JOIN media_parts mp ON mp.media_item_id = mai.id
        WHERE mai.metadata_item_id = ?
        LIMIT 1
    """
    cursor = conn.execute(query, [metadata_item_id])
    row = cursor.fetchone()
    return row["file"] if row else None
