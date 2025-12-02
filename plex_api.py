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

from plexapi.server import PlexServer
from plexapi.video import Episode, Movie
from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound, BadRequest
import requests


# API delay between plex.tv calls (seconds)
PLEX_API_DELAY = 1.0


def _log_api_error(context: str, error: Exception) -> None:
    """Log API errors with specific detection for common HTTP status codes."""
    error_str = str(error)

    if "401" in error_str or "Unauthorized" in error_str:
        logging.error(f"[PLEX API] Authentication failed ({context}): {error}")
        logging.error(f"[PLEX API] This may indicate an invalid or expired token")
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
                logging.info(f"[TOKEN CACHE] Loaded {len(self._memory_cache)} cached tokens from disk")
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
        self._users_loaded = False
        self._api_lock = threading.Lock()  # For rate limiting plex.tv calls

    def connect(self) -> None:
        """Connect to the Plex server."""
        logging.info(f"Connecting to Plex server: {self.plex_url}")

        try:
            self.plex = PlexServer(self.plex_url, self.plex_token)
            logging.info("Successfully connected to Plex server")
            logging.debug(f"Plex server version: {self.plex.version}")
        except Exception as e:
            _log_api_error("connect to Plex server", e)
            raise ConnectionError(f"Error connecting to the Plex server: {e}")

    def _rate_limited_api_call(self) -> None:
        """Enforce rate limiting for plex.tv API calls."""
        with self._api_lock:
            time.sleep(PLEX_API_DELAY)

    def load_user_tokens(self, skip_users: Optional[List[str]] = None) -> Dict[str, str]:
        """Load and cache tokens for all home users at startup.

        This method fetches tokens for all home users once, reducing repeated
        API calls to plex.tv during OnDeck/Watchlist fetching.

        Args:
            skip_users: List of usernames or tokens to skip

        Returns:
            Dict mapping username -> token
        """
        if self._users_loaded:
            logging.debug("[PLEX API] User tokens already loaded, using cached values")
            return self._user_tokens

        skip_users = skip_users or []
        machine_id = self.plex.machineIdentifier
        logging.info("[PLEX API] Loading user tokens (one-time startup operation)...")

        try:
            self._rate_limited_api_call()
            account = self.plex.myPlexAccount()
            main_username = account.title
            self._user_tokens[main_username] = self.plex_token
            logging.info(f"[PLEX API] Main account: {main_username}")

            self._rate_limited_api_call()
            users = account.users()
            logging.info(f"[PLEX API] Found {len(users)} additional users")

            # Count remote vs home users for logging
            remote_users = 0
            for user in users:
                username = user.title

                # Skip remote users (they have a username attribute set)
                # Remote users must use RSS for watchlist - API won't work
                if getattr(user, "username", None) is not None:
                    remote_users += 1
                    logging.debug(f"[PLEX API] Skipping remote user: {username}")
                    continue

                # Check skip list
                if username in skip_users:
                    logging.info(f"[PLEX API] Skipping user (in skip list): {username}")
                    continue

                # Try to get token from cache first
                cached_token = self._token_cache.get_token(username, machine_id)
                if cached_token:
                    # Verify cached token still works
                    if cached_token in skip_users:
                        logging.info(f"[PLEX API] Skipping {username} (token in skip list)")
                        continue
                    self._user_tokens[username] = cached_token
                    logging.info(f"[PLEX API] Using cached token for: {username}")
                    continue

                # Fetch fresh token from plex.tv
                try:
                    self._rate_limited_api_call()
                    token = user.get_token(machine_id)
                    if token:
                        if token in skip_users:
                            logging.info(f"[PLEX API] Skipping {username} (token in skip list)")
                            continue
                        self._user_tokens[username] = token
                        self._token_cache.set_token(username, token, machine_id)
                        logging.info(f"[PLEX API] Fetched and cached token for: {username}")
                    else:
                        logging.warning(f"[PLEX API] No token available for: {username}")
                except Exception as e:
                    _log_api_error(f"get token for {username}", e)

            self._users_loaded = True
            home_users = len(self._user_tokens) - 1  # Subtract main account
            logging.info(f"[PLEX API] Loaded tokens for {len(self._user_tokens)} users ({home_users} home users, {remote_users} remote users skipped)")
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
                        number_episodes: int, users_toggle: bool, skip_ondeck: List[str]) -> List[str]:
        """Get OnDeck media files using cached tokens (no plex.tv API calls)."""
        on_deck_files = []

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

        logging.info(f"Fetching OnDeck media for {len(users_to_fetch)} users (using cached tokens)")

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

        logging.info(f"Found {len(on_deck_files)} OnDeck items")
        return on_deck_files

    
    def _fetch_user_on_deck_media(self, valid_sections: List[int], days_to_monitor: int,
                                number_episodes: int, user=None) -> List[str]:
        """Fetch onDeck media for a specific user using cached tokens."""
        username = user.title if user else "main"
        try:
            username, plex_instance = self.get_plex_instance(user)
            if not plex_instance:
                logging.info(f"Skipping OnDeck fetch for {username} — no Plex instance available")
                return []

            logging.info(f"Fetching {username}'s onDeck media...")

            on_deck_files = []
            # Get all sections available for the user
            available_sections = [section.key for section in plex_instance.library.sections()]
            filtered_sections = list(set(available_sections) & set(valid_sections))

            for video in plex_instance.library.onDeck():
                section_key = video.section().key
                if not filtered_sections or section_key in filtered_sections:
                    delta = datetime.now() - video.lastViewedAt
                    if delta.days <= days_to_monitor:
                        if isinstance(video, Episode):
                            self._process_episode_ondeck(video, number_episodes, on_deck_files)
                        elif isinstance(video, Movie):
                            self._process_movie_ondeck(video, on_deck_files)
                        else:
                            logging.warning(f"Skipping OnDeck item '{video.title}' — unknown type {type(video)}")
                else:
                    logging.debug(f"Skipping OnDeck item '{video.title}' — section {section_key} not in valid_sections {filtered_sections}")

            return on_deck_files

        except Exception as e:
            _log_api_error(f"fetch OnDeck for {username}", e)
            # Invalidate token on auth failure
            if "401" in str(e) or "Unauthorized" in str(e):
                self.invalidate_user_token(username)
            return []
    
    def _process_episode_ondeck(self, video: Episode, number_episodes: int, on_deck_files: List[str]) -> None:
        """Process an episode from onDeck."""
        for media in video.media:
            on_deck_files.extend(part.file for part in media.parts)

        # Skip fetching next episodes if current episode has missing index data
        if video.parentIndex is None or video.index is None:
            logging.warning(f"Skipping next episode fetch for '{video.grandparentTitle}' - missing index data (parentIndex={video.parentIndex}, index={video.index})")
            return

        show = video.grandparentTitle
        library_section = video.section()
        episodes = list(library_section.search(show)[0].episodes())
        current_season = video.parentIndex
        next_episodes = self._get_next_episodes(episodes, current_season, video.index, number_episodes)

        for episode in next_episodes:
            for media in episode.media:
                on_deck_files.extend(part.file for part in media.parts)
                for part in media.parts:
                    logging.info(f"OnDeck found: {part.file}")
    
    def _process_movie_ondeck(self, video: Movie, on_deck_files: List[str]) -> None:
        """Process a movie from onDeck."""
        for media in video.media:
            on_deck_files.extend(part.file for part in media.parts)
            for part in media.parts:
                logging.info(f"OnDeck found: {part.file}")
    
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
                            users_toggle: bool, skip_watchlist: List[str], rss_url: Optional[str] = None) -> Generator[str, None, None]:
        """Get watchlist media files, optionally via RSS, with proper user filtering."""

        def fetch_rss_titles(url: str) -> List[Tuple[str, str]]:
            """Fetch titles and categories from a Plex RSS feed."""
            try:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                items = []
                for item in root.findall("channel/item"):
                    title = item.find("title").text
                    category_elem = item.find("category")
                    category = category_elem.text if category_elem is not None else ""
                    items.append((title, category))
                return items
            except Exception as e:
                logging.error(f"Failed to fetch or parse RSS feed {url}: {e}")
                return []

        def process_show(file, watchlist_episodes: int) -> Generator[str, None, None]:
            episodes = file.episodes()
            logging.debug(f"Processing show {file.title} with {len(episodes)} episodes")
            for episode in episodes[:watchlist_episodes]:
                if len(episode.media) > 0 and len(episode.media[0].parts) > 0:
                    if not episode.isPlayed:
                        yield episode.media[0].parts[0].file

        def process_movie(file) -> Generator[str, None, None]:
            if len(file.media) > 0 and len(file.media[0].parts) > 0:
                yield file.media[0].parts[0].file


        def fetch_user_watchlist(user) -> Generator[str, None, None]:
            """Fetch watchlist media for a user, optionally via RSS, yielding file paths."""
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
            try:
                if user is None:
                    # Use already authenticated main account
                    account = self.plex.myPlexAccount()
                else:
                    # Try to switch to home user
                    try:
                        self._rate_limited_api_call()
                        account = self.plex.myPlexAccount().switchHomeUser(user.title)
                    except Exception as e:
                        _log_api_error(f"switch to user {user.title}", e)
                        return
            except Exception as e:
                _log_api_error(f"get Plex account for {current_username}", e)
                return

            # --- RSS feed processing ---
            if rss_url:
                rss_items = fetch_rss_titles(rss_url)
                logging.info(f"RSS feed contains {len(rss_items)} items")
                for title, category in rss_items:
                    cleaned_title = self.clean_rss_title(title)
                    file = self.search_plex(cleaned_title)
                    if file:
                        logging.info(f"RSS title '{title}' matched Plex item '{file.title}' ({file.TYPE})")
                        if not filtered_sections or file.librarySectionID in filtered_sections:
                            try:
                                if category == 'show' or file.TYPE == 'show':
                                    yield from process_show(file, watchlist_episodes)
                                elif file.TYPE == 'movie':
                                    yield from process_movie(file)
                                else:
                                    logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                            except Exception as e:
                                logging.warning(f"Error processing '{file.title}': {e}")
                        else:
                            logging.debug(f"Skipping RSS item '{file.title}' — section {file.librarySectionID} not in valid_sections {filtered_sections}")
                    else:
                        logging.warning(f"RSS title '{title}' (cleaned: '{cleaned_title}') not found in Plex — discarded")
                return

            # --- Local Plex watchlist processing ---
            try:
                watchlist = account.watchlist(filter='released')
                logging.info(f"{current_username}: Found {len(watchlist)} watchlist items from Plex")
                for item in watchlist:
                    file = self.search_plex(item.title)
                    if file and (not filtered_sections or file.librarySectionID in filtered_sections):
                        try:
                            if file.TYPE == 'show':
                                yield from process_show(file, watchlist_episodes)
                            elif file.TYPE == 'movie':
                                yield from process_movie(file)
                            else:
                                logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                        except Exception as e:
                            logging.warning(f"Error processing '{file.title}': {e}")
                    elif file:
                        logging.debug(f"Skipping watchlist item '{file.title}' — section {file.librarySectionID} not in valid_sections {filtered_sections}")
            except Exception as e:
                logging.error(f"Error fetching watchlist for {current_username}: {e}")


        # --- Prepare users to fetch using cached tokens (no plex.tv API calls) ---
        users_to_fetch = [None]  # always include the main local account

        if users_toggle:
            # Use cached tokens - no API calls to plex.tv here
            for username, token in self._user_tokens.items():
                # Skip main account (already added as None)
                if token == self.plex_token:
                    continue
                # Check skip list
                if username in skip_watchlist or token in skip_watchlist:
                    logging.info(f"Skipping {username} for watchlist — in skip list")
                    continue
                # Create a simple object to pass username
                class UserProxy:
                    def __init__(self, title):
                        self.title = title
                users_to_fetch.append(UserProxy(username))

        logging.info(f"Processing {len(users_to_fetch)} users for local Plex watchlist (using cached tokens)")

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



    def get_watched_media(self, valid_sections: List[int], last_updated: Optional[float], 
                        users_toggle: bool) -> Generator[str, None, None]:
        """Get watched media files (local users only)."""

        def process_video(video) -> Generator[str, None, None]:
            if video.TYPE == 'show':
                for episode in video.episodes():
                    yield from process_episode(episode)
            else:
                if len(video.media) > 0 and len(video.media[0].parts) > 0:
                    yield video.media[0].parts[0].file

        def process_episode(episode) -> Generator[str, None, None]:
            for media in episode.media:
                for part in media.parts:
                    if episode.isPlayed:
                        yield part.file

        def fetch_user_watched_media(plex_instance: PlexServer, username: str) -> Generator[str, None, None]:
            time.sleep(1)
            try:
                logging.info(f"Fetching {username}'s watched media...")
                all_sections = [section.key for section in plex_instance.library.sections()]
                available_sections = list(set(all_sections) & set(valid_sections)) if valid_sections else all_sections

                for section_key in available_sections:
                    section = plex_instance.library.sectionByID(section_key)
                    # Skip non-video sections (music, photos) - they don't support 'unwatched' filter
                    if section.type not in ('movie', 'show'):
                        logging.debug(f"Skipping non-video section '{section.title}' (type: {section.type})")
                        continue
                    for video in section.search(unwatched=False):
                        if last_updated and video.lastViewedAt and video.lastViewedAt < datetime.fromtimestamp(last_updated):
                            continue
                        yield from process_video(video)
            except Exception as e:
                logging.error(f"Error fetching watched media for {username}: {e}")

        # --- Only fetch for main local user ---
        with ThreadPoolExecutor() as executor:
            main_username = self.plex.myPlexAccount().title
            futures = [executor.submit(fetch_user_watched_media, self.plex, main_username)]

            logging.info(f"Processing watched media for local user: {main_username} only")

            for future in as_completed(futures):
                try:
                    yield from future.result()
                except Exception as e:
                    logging.error(f"An error occurred in get_watched_media: {e}")



class CacheManager:
    """Manages cache operations for media files."""
    
    @staticmethod
    def load_media_from_cache(cache_file: Path) -> Tuple[Set[str], Optional[float]]:
        if cache_file.exists():
            with cache_file.open('r') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return set(data.get('media', [])), data.get('timestamp')
                    elif isinstance(data, list):
                        return set(data), None
                except json.JSONDecodeError:
                    with cache_file.open('w') as f:
                        f.write(json.dumps({'media': [], 'timestamp': None}))
                    return set(), None
        return set(), None
    
    @staticmethod
    def save_media_to_cache(cache_file: Path, media_list: List[str], timestamp: Optional[float] = None) -> None:
        if timestamp is None:
            timestamp = datetime.now().timestamp()
        with cache_file.open('w') as f:
            json.dump({'media': media_list, 'timestamp': timestamp}, f)
