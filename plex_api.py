"""
Plex API integration for PlexCache.
Handles Plex server connections and media fetching operations.
"""

import json
import logging
import os
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Set, Optional, Generator, Tuple, Dict
from dataclasses import dataclass

from plexapi.server import PlexServer


from plexapi.video import Episode, Movie
from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound, BadRequest
import requests


@dataclass
class OnDeckItem:
    """Represents an OnDeck item with metadata.

    Attributes:
        file_path: Path to the media file.
        username: The user who has this on their OnDeck.
        episode_info: For TV episodes, dict with 'show', 'season', 'episode' keys.
        is_current_ondeck: True if this is the actual OnDeck episode (not prefetched next).
    """
    file_path: str
    username: str
    episode_info: Optional[Dict[str, any]] = None
    is_current_ondeck: bool = False


# API delay between plex.tv calls (seconds)
PLEX_API_DELAY = 1.0


def _log_api_error(context: str, error: Exception) -> None:
    """Log API errors with specific detection for common HTTP status codes."""
    error_str = str(error)

    if "401" in error_str or "Unauthorized" in error_str:
        logging.error(f"[PLEX API] Authentication failed ({context}): {error}")
        logging.error(f"[PLEX API] Your Plex token is invalid or has been revoked.")
        logging.error(f"[PLEX API] To fix: Run 'python3 plexcache_setup.py' and select 'y' to re-authenticate.")
    elif "429" in error_str or "Too Many Requests" in error_str:
        logging.warning(f"[PLEX API] Rate limited by Plex.tv ({context}): {error}")
        logging.warning(f"[PLEX API] Consider increasing delays between API calls")
    elif "403" in error_str or "Forbidden" in error_str:
        logging.error(f"[PLEX API] Access forbidden ({context}): {error}")
        logging.error(f"[PLEX API] User may not have permission for this resource")
    elif "404" in error_str or "Not Found" in error_str:
        logging.warning(f"[PLEX API] Resource not found ({context}): {error}")
    elif "500" in error_str or "502" in error_str or "503" in error_str:
        logging.error(f"[PLEX API] Plex server error ({context}): {error}")
        logging.error(f"[PLEX API] Plex.tv may be experiencing issues")
    else:
        logging.error(f"[PLEX API] Error ({context}): {error}")


class UserTokenCache:
    """Cache for user tokens to reduce API calls to plex.tv.

    Tokens are cached in memory for the duration of the run, and optionally
    persisted to disk for reuse across runs (with configurable expiry).
    """

    def __init__(self, cache_file: Optional[str] = None, cache_expiry_hours: int = 24):
        """Initialize the token cache.

        Args:
            cache_file: Optional path to persist tokens to disk
            cache_expiry_hours: How long cached tokens are valid (default 24 hours)
        """
        self._memory_cache: Dict[str, Dict] = {}  # username -> {token, timestamp, machine_id}
        self._lock = threading.Lock()
        self._cache_file = cache_file
        self._cache_expiry_seconds = cache_expiry_hours * 3600

        # Load from disk if cache file exists
        if cache_file:
            self._load_from_disk()

    def get_token(self, username: str, machine_id: str) -> Optional[str]:
        """Get a cached token for a user, if valid."""
        with self._lock:
            if username in self._memory_cache:
                entry = self._memory_cache[username]
                # Check if token is for the same machine and not expired
                if entry.get('machine_id') == machine_id:
                    age = time.time() - entry.get('timestamp', 0)
                    if age < self._cache_expiry_seconds:
                        logging.debug(f"[TOKEN CACHE] Hit for {username} (age: {age/3600:.1f}h)")
                        return entry.get('token')
                    else:
                        logging.debug(f"[TOKEN CACHE] Expired for {username} (age: {age/3600:.1f}h)")
                else:
                    logging.debug(f"[TOKEN CACHE] Machine ID mismatch for {username}")
            return None

    def set_token(self, username: str, token: str, machine_id: str) -> None:
        """Cache a token for a user."""
        with self._lock:
            self._memory_cache[username] = {
                'token': token,
                'timestamp': time.time(),
                'machine_id': machine_id
            }
            logging.debug(f"[TOKEN CACHE] Stored token for {username}")

            # Persist to disk if configured
            if self._cache_file:
                self._save_to_disk()

    def invalidate(self, username: str) -> None:
        """Invalidate a cached token (e.g., after auth failure)."""
        with self._lock:
            if username in self._memory_cache:
                del self._memory_cache[username]
                logging.info(f"[TOKEN CACHE] Invalidated token for {username}")
                if self._cache_file:
                    self._save_to_disk()

    def _load_from_disk(self) -> None:
        """Load cached tokens from disk."""
        if not self._cache_file or not os.path.exists(self._cache_file):
            return
        try:
            with open(self._cache_file, 'r') as f:
                data = json.load(f)
                self._memory_cache = data.get('tokens', {})
                logging.debug(f"[TOKEN CACHE] Loaded {len(self._memory_cache)} cached tokens from disk")
        except Exception as e:
            logging.warning(f"[TOKEN CACHE] Could not load cache file: {e}")
            self._memory_cache = {}

    def _save_to_disk(self) -> None:
        """Save cached tokens to disk."""
        if not self._cache_file:
            return
        try:
            with open(self._cache_file, 'w') as f:
                json.dump({'tokens': self._memory_cache}, f)
        except Exception as e:
            logging.warning(f"[TOKEN CACHE] Could not save cache file: {e}")


class PlexManager:
    """Manages Plex server connections and operations."""

    def __init__(self, plex_url: str, plex_token: str, retry_limit: int = 3, delay: int = 5,
                 token_cache_file: Optional[str] = None):
        self.plex_url = plex_url
        self.plex_token = plex_token
        self.retry_limit = retry_limit
        self.delay = delay
        self.plex = None
        self._token_cache = UserTokenCache(cache_file=token_cache_file, cache_expiry_hours=24)
        self._user_tokens: Dict[str, str] = {}  # username -> token (populated at startup)
        self._user_id_to_name: Dict[str, str] = {}  # user_id (str) -> username (for RSS author lookup)
        self._users_loaded = False
        self._api_lock = threading.Lock()  # For rate limiting plex.tv calls

    def connect(self) -> None:
        """Connect to the Plex server."""
        logging.debug(f"Connecting to Plex server: {self.plex_url}")

        try:
            self.plex = PlexServer(self.plex_url, self.plex_token)
            logging.debug(f"Plex server version: {self.plex.version}")
        except Exception as e:
            _log_api_error("connect to Plex server", e)
            raise ConnectionError(f"Error connecting to the Plex server: {e}")

    def _rate_limited_api_call(self) -> None:
        """Enforce rate limiting for plex.tv API calls."""
        with self._api_lock:
            time.sleep(PLEX_API_DELAY)

    def load_user_tokens(self, skip_users: Optional[List[str]] = None,
                         settings_users: Optional[List[dict]] = None) -> Dict[str, str]:
        """Load and cache tokens for all users at startup.

        Hybrid approach:
        1. First load tokens from settings file (includes all users: local AND remote)
        2. Then check Plex API for any new users not in settings

        Args:
            skip_users: List of usernames or tokens to skip
            settings_users: List of user dicts from settings file with tokens

        Returns:
            Dict mapping username -> token
        """
        if self._users_loaded:
            logging.debug("[PLEX API] User tokens already loaded, using cached values")
            return self._user_tokens

        skip_users = skip_users or []
        settings_users = settings_users or []
        machine_id = self.plex.machineIdentifier
        logging.debug("[PLEX API] Loading user tokens...")

        try:
            # Add main account token
            self._rate_limited_api_call()
            account = self.plex.myPlexAccount()
            main_username = account.title
            self._user_tokens[main_username] = self.plex_token
            logging.debug(f"[PLEX API] Main account: {main_username}")

            # Step 1: Load tokens from settings file (includes remote users)
            settings_loaded = 0
            settings_usernames = set()
            for user_entry in settings_users:
                username = user_entry.get("title")
                token = user_entry.get("token")
                is_local = user_entry.get("is_local", False)
                user_id = user_entry.get("id")

                if not username or not token:
                    continue

                settings_usernames.add(username)

                # Build user ID -> username map for RSS author lookup
                # RSS feed uses uuid (hex string), so store both id and uuid
                if user_id:
                    self._user_id_to_name[str(user_id)] = username
                    logging.debug(f"[PLEX API] Mapped ID {user_id} -> {username}")
                user_uuid = user_entry.get("uuid")
                if user_uuid:
                    self._user_id_to_name[str(user_uuid)] = username
                    logging.debug(f"[PLEX API] Mapped UUID {user_uuid} -> {username}")

                # Check skip list
                if username in skip_users or token in skip_users:
                    logging.debug(f"[PLEX API] Skipping {username} (in skip list)")
                    continue

                self._user_tokens[username] = token
                self._token_cache.set_token(username, token, machine_id)
                user_type = "home" if is_local else "remote"
                logging.debug(f"[PLEX API] Loaded {user_type} user from settings: {username}")
                settings_loaded += 1

            logging.debug(f"[PLEX API] Loaded {settings_loaded} users from settings file")

            # Step 2: Check Plex API for new users not in settings
            self._rate_limited_api_call()
            users = account.users()
            new_users = 0

            for user in users:
                username = user.title

                # Build user ID -> username map for RSS author lookup (even if skipped)
                # RSS feed uses uuid (hex string from thumb URL), so store both id and uuid
                if hasattr(user, 'id') and user.id:
                    self._user_id_to_name[str(user.id)] = username
                # Extract uuid from thumb URL: https://plex.tv/users/{uuid}/avatar
                thumb = getattr(user, 'thumb', '')
                if thumb and '/users/' in thumb:
                    try:
                        user_uuid = thumb.split('/users/')[1].split('/')[0]
                        self._user_id_to_name[user_uuid] = username
                    except (IndexError, AttributeError):
                        pass

                # Skip if already loaded from settings
                if username in settings_usernames:
                    continue

                # Check skip list
                if username in skip_users:
                    logging.debug(f"[PLEX API] Skipping user (in skip list): {username}")
                    continue

                # Try to get token from disk cache first
                cached_token = self._token_cache.get_token(username, machine_id)
                if cached_token:
                    if cached_token in skip_users:
                        logging.debug(f"[PLEX API] Skipping {username} (token in skip list)")
                        continue
                    self._user_tokens[username] = cached_token
                    logging.debug(f"[PLEX API] Using cached token for new user: {username}")
                    new_users += 1
                    continue

                # Fetch fresh token from plex.tv
                try:
                    self._rate_limited_api_call()
                    token = user.get_token(machine_id)
                    if token:
                        if token in skip_users:
                            logging.debug(f"[PLEX API] Skipping {username} (token in skip list)")
                            continue
                        self._user_tokens[username] = token
                        self._token_cache.set_token(username, token, machine_id)
                        logging.debug(f"[PLEX API] Fetched token for new user: {username}")
                        new_users += 1
                    else:
                        logging.debug(f"[PLEX API] No token available for: {username}")
                except Exception as e:
                    _log_api_error(f"get token for {username}", e)

            if new_users > 0:
                logging.info(f"[PLEX API] Found {new_users} new users not in settings (consider re-running setup)")

            self._users_loaded = True
            total_users = len(self._user_tokens)
            logging.info(f"Connected to Plex ({total_users} users)")
            return self._user_tokens

        except Exception as e:
            _log_api_error("load user tokens", e)
            self._users_loaded = True  # Mark as loaded even on error to prevent retries
            return self._user_tokens

    def get_user_token(self, username: str) -> Optional[str]:
        """Get a cached token for a user (must call load_user_tokens first)."""
        return self._user_tokens.get(username)

    def invalidate_user_token(self, username: str) -> None:
        """Invalidate a user's token (e.g., after auth failure)."""
        if username in self._user_tokens:
            del self._user_tokens[username]
        self._token_cache.invalidate(username)

    def resolve_user_uuid(self, uuid: str) -> Optional[str]:
        """Try to resolve a UUID to a username by querying the Plex API.

        Args:
            uuid: The UUID string to look up (e.g., from RSS feed author).

        Returns:
            The username if found, None otherwise.
        """
        # Check if already in mapping
        if uuid in self._user_id_to_name:
            return self._user_id_to_name[uuid]

        # Track UUIDs we've tried to resolve to avoid repeated API calls
        if not hasattr(self, '_resolved_uuids'):
            self._resolved_uuids = set()

        if uuid in self._resolved_uuids:
            return None  # Already tried, not found

        self._resolved_uuids.add(uuid)

        # Re-query Plex API to find this UUID
        try:
            self._rate_limited_api_call()
            account = self.plex.myPlexAccount()
            users = account.users()

            for user in users:
                username = user.title
                # Extract UUID from thumb URL
                thumb = getattr(user, 'thumb', '')
                if thumb and '/users/' in thumb:
                    try:
                        user_uuid = thumb.split('/users/')[1].split('/')[0]
                        # Add to mapping
                        self._user_id_to_name[user_uuid] = username
                        # Check if this is the one we're looking for
                        if user_uuid == uuid:
                            logging.debug(f"[PLEX API] Resolved UUID {uuid} to username: {username}")
                            return username
                    except (IndexError, AttributeError):
                        pass

            logging.debug(f"[PLEX API] Could not resolve UUID: {uuid}")
            return None

        except Exception as e:
            _log_api_error(f"resolve UUID {uuid}", e)
            return None

    def get_plex_instance(self, user=None) -> Tuple[Optional[str], Optional[PlexServer]]:
        """Get Plex instance for a specific user using cached tokens."""
        if user:
            username = user.title
            # Use cached token if available
            token = self._user_tokens.get(username)
            if not token:
                # Fall back to fetching token (shouldn't happen if load_user_tokens was called)
                logging.warning(f"[PLEX API] No cached token for {username}, fetching fresh...")
                try:
                    self._rate_limited_api_call()
                    token = user.get_token(self.plex.machineIdentifier)
                    if token:
                        self._user_tokens[username] = token
                        self._token_cache.set_token(username, token, self.plex.machineIdentifier)
                except Exception as e:
                    _log_api_error(f"get token for {username}", e)
                    return None, None

            if not token:
                logging.warning(f"[PLEX API] No token available for {username}")
                return None, None

            try:
                return username, PlexServer(self.plex_url, token)
            except Exception as e:
                _log_api_error(f"create PlexServer for {username}", e)
                # Invalidate token on auth failure
                if "401" in str(e) or "Unauthorized" in str(e):
                    self.invalidate_user_token(username)
                return None, None
        else:
            # Main account - use stored token (no API call needed)
            try:
                username = self.plex.myPlexAccount().title
            except Exception:
                username = "main"
            return username, PlexServer(self.plex_url, self.plex_token)
    
    def search_plex(self, title: str):
        """Search for a file in the Plex server."""
        results = self.plex.search(title)
        return results[0] if len(results) > 0 else None
    
    def get_active_sessions(self) -> List:
        """Get active sessions from Plex."""
        return self.plex.sessions()
    
    def get_on_deck_media(self, valid_sections: List[int], days_to_monitor: int,
                        number_episodes: int, users_toggle: bool, skip_ondeck: List[str]) -> List[OnDeckItem]:
        """Get OnDeck media files using cached tokens (no plex.tv API calls).

        Returns:
            List of OnDeckItem objects containing file path, username, and episode metadata.
        """
        on_deck_files: List[OnDeckItem] = []

        # Build list of users to fetch using cached tokens
        users_to_fetch = [None]  # Always include main local account
        if users_toggle:
            # Use cached tokens - no API calls to plex.tv here
            for username, token in self._user_tokens.items():
                # Skip main account (already added as None)
                if token == self.plex_token:
                    continue
                # Check skip list
                if username in skip_ondeck or token in skip_ondeck:
                    logging.info(f"Skipping {username} for OnDeck — in skip list")
                    continue
                # Create a simple object to pass username to get_plex_instance
                class UserProxy:
                    def __init__(self, title):
                        self.title = title
                users_to_fetch.append(UserProxy(username))

        logging.debug(f"Fetching OnDeck media for {len(users_to_fetch)} users (using cached tokens)")

        # Fetch concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    self._fetch_user_on_deck_media,
                    valid_sections, days_to_monitor, number_episodes, user
                )
                for user in users_to_fetch
            }

            for future in as_completed(futures):
                try:
                    on_deck_files.extend(future.result())
                except Exception as e:
                    logging.error(f"An error occurred while fetching OnDeck media for a user: {e}")

        return on_deck_files

    
    def _fetch_user_on_deck_media(self, valid_sections: List[int], days_to_monitor: int,
                                number_episodes: int, user=None) -> List[OnDeckItem]:
        """Fetch onDeck media for a specific user using cached tokens.

        Returns:
            List of OnDeckItem objects containing file path, username, and episode metadata.
        """
        username = user.title if user else "main"
        try:
            username, plex_instance = self.get_plex_instance(user)
            if not plex_instance:
                logging.info(f"Skipping OnDeck fetch for {username} — no Plex instance available")
                return []

            logging.debug(f"Fetching {username}'s onDeck media...")

            on_deck_files: List[OnDeckItem] = []
            # Get all sections available for the user
            available_sections = [section.key for section in plex_instance.library.sections()]
            filtered_sections = list(set(available_sections) & set(valid_sections))

            for video in plex_instance.library.onDeck():
                section_key = video.section().key
                if not filtered_sections or section_key in filtered_sections:
                    delta = datetime.now() - video.lastViewedAt
                    if delta.days <= days_to_monitor:
                        if isinstance(video, Episode):
                            self._process_episode_ondeck(video, number_episodes, on_deck_files, username)
                        elif isinstance(video, Movie):
                            self._process_movie_ondeck(video, on_deck_files, username)
                        else:
                            logging.warning(f"Skipping OnDeck item '{video.title}' — unknown type {type(video)}")
                else:
                    logging.debug(f"Skipping OnDeck item '{video.title}' — section {section_key} not in valid_sections {filtered_sections}")

            logging.info(f"{username}: Found {len(on_deck_files)} OnDeck items")
            return on_deck_files

        except Exception as e:
            _log_api_error(f"fetch OnDeck for {username}", e)
            # Invalidate token on auth failure
            if "401" in str(e) or "Unauthorized" in str(e):
                self.invalidate_user_token(username)
            return []
    
    def _process_episode_ondeck(self, video: Episode, number_episodes: int, on_deck_files: List[OnDeckItem], username: str = "unknown") -> None:
        """Process an episode from onDeck.

        Args:
            video: The episode video object.
            number_episodes: Number of next episodes to fetch.
            on_deck_files: List to append OnDeckItem objects to.
            username: The user who has this OnDeck.
        """
        show = video.grandparentTitle
        current_season = video.parentIndex
        current_episode = video.index

        # Create episode info dict for this episode (the actual OnDeck episode)
        episode_info = None
        if current_season is not None and current_episode is not None:
            episode_info = {
                'show': show,
                'season': current_season,
                'episode': current_episode
            }

        # Add the current OnDeck episode
        for media in video.media:
            for part in media.parts:
                on_deck_files.append(OnDeckItem(
                    file_path=part.file,
                    username=username,
                    episode_info=episode_info,
                    is_current_ondeck=True  # This is the actual OnDeck episode
                ))
                logging.debug(f"OnDeck found ({username}): {part.file}")

        # Skip fetching next episodes if current episode has missing index data
        if current_season is None or current_episode is None:
            logging.warning(f"Skipping next episode fetch for '{show}' - missing index data (parentIndex={current_season}, index={current_episode})")
            return

        library_section = video.section()
        episodes = list(library_section.search(show)[0].episodes())
        next_episodes = self._get_next_episodes(episodes, current_season, current_episode, number_episodes)

        # Add the prefetched next episodes
        for episode in next_episodes:
            next_ep_info = {
                'show': show,
                'season': episode.parentIndex,
                'episode': episode.index
            }
            for media in episode.media:
                for part in media.parts:
                    on_deck_files.append(OnDeckItem(
                        file_path=part.file,
                        username=username,
                        episode_info=next_ep_info,
                        is_current_ondeck=False  # This is a prefetched next episode
                    ))
                    logging.debug(f"OnDeck found ({username}): {part.file}")
    
    def _process_movie_ondeck(self, video: Movie, on_deck_files: List[OnDeckItem], username: str = "unknown") -> None:
        """Process a movie from onDeck.

        Args:
            video: The movie video object.
            on_deck_files: List to append OnDeckItem objects to.
            username: The user who has this OnDeck.
        """
        for media in video.media:
            for part in media.parts:
                on_deck_files.append(OnDeckItem(
                    file_path=part.file,
                    username=username,
                    episode_info=None,  # Movies don't have episode info
                    is_current_ondeck=True
                ))
                logging.debug(f"OnDeck found ({username}): {part.file}")
    
    def _get_next_episodes(self, episodes: List[Episode], current_season: int,
                          current_episode_index: int, number_episodes: int) -> List[Episode]:
        """Get the next episodes after the current one."""
        next_episodes = []
        for episode in episodes:
            # Skip episodes with missing index data
            if episode.parentIndex is None or episode.index is None:
                logging.debug(f"Skipping episode '{episode.title}' from '{episode.grandparentTitle}' - missing index data (parentIndex={episode.parentIndex}, index={episode.index})")
                continue
            if (episode.parentIndex > current_season or
                (episode.parentIndex == current_season and episode.index > current_episode_index)) and len(next_episodes) < number_episodes:
                next_episodes.append(episode)
            if len(next_episodes) == number_episodes:
                break
        return next_episodes

    def clean_rss_title(self, title: str) -> str:
        """Remove trailing year in parentheses from a title, e.g. 'Movie (2023)' -> 'Movie'."""
        import re
        return re.sub(r"\s\(\d{4}\)$", "", title)


    def get_watchlist_media(self, valid_sections: List[int], watchlist_episodes: int,
                            users_toggle: bool, skip_watchlist: List[str], rss_url: Optional[str] = None,
                            home_users: Optional[List[str]] = None) -> Generator[Tuple[str, str, Optional[datetime]], None, None]:
        """Get watchlist media files, optionally via RSS, with proper user filtering.

        Args:
            home_users: List of usernames that are home/managed users (can access watchlist).
                       Remote users (friends) cannot have their watchlists accessed.

        Yields:
            Tuples of (file_path, username, watchlisted_at) where watchlisted_at is the
            datetime when the item was added to the user's watchlist (None for RSS items).
        """
        if home_users is None:
            home_users = []

        def fetch_rss_titles(url: str) -> List[Tuple[str, str, Optional[datetime], str]]:
            """Fetch titles, categories, pubDate, and author ID from a Plex RSS feed."""
            from email.utils import parsedate_to_datetime
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                items = []
                for item in root.findall("channel/item"):
                    title = item.find("title").text
                    category_elem = item.find("category")
                    category = category_elem.text if category_elem is not None else ""
                    # Parse pubDate (RFC 822 format) - this is when item was added to watchlist
                    pub_date = None
                    pub_date_elem = item.find("pubDate")
                    if pub_date_elem is not None and pub_date_elem.text:
                        try:
                            pub_date = parsedate_to_datetime(pub_date_elem.text)
                        except Exception:
                            pass
                    # Get author ID (Plex user ID who added to watchlist)
                    author_id = ""
                    author_elem = item.find("author")
                    if author_elem is not None and author_elem.text:
                        author_id = author_elem.text
                    items.append((title, category, pub_date, author_id))
                return items
            except Exception as e:
                logging.error(f"Failed to fetch or parse RSS feed {url}: {e}")
                return []

        def process_show(file, watchlist_episodes: int, username: str, watchlisted_at: Optional[datetime]) -> Generator[Tuple[str, str, Optional[datetime]], None, None]:
            """Process a show and yield episode file paths with metadata."""
            episodes = file.episodes()
            episodes_to_process = episodes[:watchlist_episodes]
            logging.debug(f"Processing show {file.title} with {len(episodes)} episodes (limit: {watchlist_episodes})")

            yielded_count = 0
            skipped_watched = 0
            skipped_no_media = 0

            for episode in episodes_to_process:
                if len(episode.media) > 0 and len(episode.media[0].parts) > 0:
                    if not episode.isPlayed:
                        yield (episode.media[0].parts[0].file, username, watchlisted_at)
                        yielded_count += 1
                    else:
                        skipped_watched += 1
                else:
                    skipped_no_media += 1

            # Log summary for this show
            if skipped_watched > 0:
                logging.debug(f"  {file.title}: {yielded_count} episodes to cache, {skipped_watched} skipped (already watched)")
            if skipped_no_media > 0:
                logging.warning(f"  {file.title}: {skipped_no_media} episodes skipped (no media files)")

        def process_movie(file, username: str, watchlisted_at: Optional[datetime]) -> Generator[Tuple[str, str, Optional[datetime]], None, None]:
            """Process a movie and yield file path with metadata."""
            if len(file.media) > 0 and len(file.media[0].parts) > 0:
                yield (file.media[0].parts[0].file, username, watchlisted_at)


        def fetch_user_watchlist(user) -> Generator[Tuple[str, str, Optional[datetime]], None, None]:
            """Fetch watchlist media for a user, optionally via RSS, yielding file paths with metadata.

            Uses separate MyPlexAccount instances per user to avoid session state contamination.
            See: https://github.com/StudioNirin/PlexCache-R/issues/20
            """
            current_username = user.title if user else "main"

            # Use rate limiting
            self._rate_limited_api_call()

            # Get username from cached tokens if available
            if user is None:
                try:
                    current_username = self.plex.myPlexAccount().title
                except Exception:
                    current_username = "main"
            else:
                current_username = user.title

            logging.info(f"Fetching watchlist media for {current_username}")

            # Build list of valid sections for filtering
            available_sections = [section.key for section in self.plex.library.sections()]
            filtered_sections = list(set(available_sections) & set(valid_sections))

            # Skip users in the skip list (use cached tokens)
            if user:
                token = self._user_tokens.get(current_username)
                if not token:
                    logging.warning(f"[PLEX API] No cached token for {current_username}; skipping watchlist")
                    return
                if token in skip_watchlist or current_username in skip_watchlist:
                    logging.info(f"Skipping {current_username} due to skip_watchlist")
                    return

            # --- Obtain Plex account instance ---
            # IMPORTANT: We create fresh MyPlexAccount instances with fresh HTTP sessions
            # to avoid session state contamination that can cause cross-user data leakage (Issue #20).
            #
            # For main account: Create fresh MyPlexAccount with admin token + fresh session
            # For home/managed users: Create fresh admin account + fresh session, then switchHomeUser()
            #   (Home users don't have standalone plex.tv accounts, so we can't use
            #    MyPlexAccount(token=their_token) directly - their tokens only work locally)
            try:
                import requests as req
                fresh_session = req.Session()

                if user is None:
                    # Main account - use the main token with a fresh session
                    self._rate_limited_api_call()
                    account = MyPlexAccount(token=self.plex_token, session=fresh_session)
                    logging.debug(f"[PLEX API] Created fresh MyPlexAccount for main user {current_username} (fresh session)")
                else:
                    # Home/managed user - create fresh admin account then switch to home user
                    # This isolates each request while still allowing access to home user watchlists
                    try:
                        self._rate_limited_api_call()
                        fresh_admin_account = MyPlexAccount(token=self.plex_token, session=fresh_session)
                        self._rate_limited_api_call()
                        account = fresh_admin_account.switchHomeUser(current_username)
                        logging.debug(f"[PLEX API] Switched to home user {current_username} via fresh admin account (fresh session)")
                    except Exception as e:
                        _log_api_error(f"switch to home user {current_username}", e)
                        return
            except Exception as e:
                _log_api_error(f"get Plex account for {current_username}", e)
                return

            # --- RSS feed processing (pubDate = when added to watchlist) ---
            if rss_url:
                rss_items = fetch_rss_titles(rss_url)
                logging.info(f"RSS feed contains {len(rss_items)} items")
                unknown_user_ids = set()  # Track unknown IDs to log once
                for title, category, pub_date, author_id in rss_items:
                    # Look up username from author ID, fall back to ID or "Unknown"
                    if author_id and author_id in self._user_id_to_name:
                        rss_username = self._user_id_to_name[author_id]
                    elif author_id:
                        # Try to resolve the unknown UUID via API
                        resolved_name = self.resolve_user_uuid(author_id)
                        if resolved_name:
                            rss_username = resolved_name
                        else:
                            rss_username = f"User#{author_id}"
                            unknown_user_ids.add(author_id)
                    else:
                        rss_username = "Friends (RSS)"
                    cleaned_title = self.clean_rss_title(title)
                    file = self.search_plex(cleaned_title)
                    if file:
                        logging.debug(f"RSS title '{title}' matched Plex item '{file.title}' ({file.TYPE})")
                        if not filtered_sections or file.librarySectionID in filtered_sections:
                            try:
                                if category == 'show' or file.TYPE == 'show':
                                    yield from process_show(file, watchlist_episodes, rss_username, pub_date)
                                elif file.TYPE == 'movie':
                                    yield from process_movie(file, rss_username, pub_date)
                                else:
                                    logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                            except Exception as e:
                                logging.warning(f"Error processing '{file.title}': {e}")
                        else:
                            logging.debug(f"Skipping RSS item '{file.title}' — section {file.librarySectionID} not in valid_sections {filtered_sections}")
                    else:
                        logging.warning(f"RSS title '{title}' (added by {rss_username}) not found in Plex — discarded")
                # Log unknown user IDs once at the end
                if unknown_user_ids:
                    logging.debug(f"[PLEX API] {len(unknown_user_ids)} unknown user ID(s) in RSS feed: {', '.join(sorted(unknown_user_ids))}. Run 'python3 plexcache_setup.py' and refresh users to resolve.")
                return

            # --- Local Plex watchlist processing ---
            try:
                # Rate limit the watchlist API call (hits plex.tv)
                self._rate_limited_api_call()
                # Sort by watchlistedAt descending to get most recent add date first
                watchlist = account.watchlist(filter='released', sort='watchlistedAt:desc')
                logging.info(f"{current_username}: Found {len(watchlist)} watchlist items from Plex")
                for item in watchlist:
                    # Get watchlistedAt timestamp from userState (addedAt is the media release date, not when added to watchlist)
                    watchlisted_at = None
                    try:
                        user_state = account.userState(item)
                        watchlisted_at = getattr(user_state, 'watchlistedAt', None)
                    except Exception as e:
                        logging.debug(f"Could not get userState for {item.title}: {e}")
                    file = self.search_plex(item.title)
                    if file and (not filtered_sections or file.librarySectionID in filtered_sections):
                        try:
                            if file.TYPE == 'show':
                                yield from process_show(file, watchlist_episodes, current_username, watchlisted_at)
                            elif file.TYPE == 'movie':
                                yield from process_movie(file, current_username, watchlisted_at)
                            else:
                                logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                        except Exception as e:
                            logging.warning(f"Error processing '{file.title}': {e}")
                    elif file:
                        logging.debug(f"Skipping watchlist item '{file.title}' — section {file.librarySectionID} not in valid_sections {filtered_sections}")
            except Exception as e:
                logging.error(f"Error fetching watchlist for {current_username}: {e}")


        # --- Prepare users to fetch ---
        # Only the main account and home/managed users have accessible watchlists
        # Remote users (friends) have their own separate Plex accounts we can't access
        users_to_fetch = [None]  # always include the main local account

        if users_toggle:
            for username, token in self._user_tokens.items():
                # Skip main account (already added as None)
                if token == self.plex_token:
                    continue
                # Check skip list
                if username in skip_watchlist or token in skip_watchlist:
                    logging.info(f"Skipping {username} for watchlist — in skip list")
                    continue
                # Only include home/managed users - remote users' watchlists are handled via RSS feed
                if username not in home_users:
                    continue
                # Create a simple object to pass username
                class UserProxy:
                    def __init__(self, title):
                        self.title = title
                users_to_fetch.append(UserProxy(username))

        logging.info(f"Processing {len(users_to_fetch)} users for watchlist (main + {len(users_to_fetch)-1} home users)")

        # --- Fetch concurrently ---
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_user_watchlist, user) for user in users_to_fetch}
            for future in as_completed(futures):
                retries = 0
                while retries < self.retry_limit:
                    try:
                        yield from future.result()
                        break
                    except Exception as e:
                        error_str = str(e)
                        if "429" in error_str or "Too Many Requests" in error_str:
                            logging.warning(f"[PLEX API] Rate limited. Retrying in {self.delay} seconds...")
                            time.sleep(self.delay)
                            retries += 1
                        else:
                            _log_api_error("fetch watchlist media", e)
                            break



