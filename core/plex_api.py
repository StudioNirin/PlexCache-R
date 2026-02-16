"""
Plex API integration for PlexCache.
Handles Plex server connections and media fetching operations.
"""

import json
import logging
import os
import re
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Generator, Tuple, Dict, Set
from dataclasses import dataclass

from plexapi.server import PlexServer


from plexapi.video import Episode, Movie
from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound
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

# RSS feed retry and cache settings
RSS_MAX_RETRIES = 3
RSS_TIMEOUT = 15  # seconds


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


class UserProxy:
    """Simple proxy object to pass username to methods expecting a user object."""

    def __init__(self, title: str):
        self.title = title


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
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"[TOKEN CACHE] Could not load cache file: {e}")
            self._memory_cache = {}

    def _save_to_disk(self) -> None:
        """Save cached tokens to disk."""
        if not self._cache_file:
            return
        try:
            with open(self._cache_file, 'w') as f:
                json.dump({'tokens': self._memory_cache}, f, indent=2)
        except IOError as e:
            logging.warning(f"[TOKEN CACHE] Could not save cache file: {e}")


class PlexManager:
    """Manages Plex server connections and operations."""

    def __init__(self, plex_url: str, plex_token: str, retry_limit: int = 3, delay: int = 5,
                 token_cache_file: Optional[str] = None, rss_cache_file: Optional[str] = None):
        self.plex_url = plex_url
        self.plex_token = plex_token
        self.retry_limit = retry_limit
        self.delay = delay
        self.plex = None
        self._token_cache = UserTokenCache(cache_file=token_cache_file, cache_expiry_hours=24)
        self._rss_cache_file = rss_cache_file  # Path to RSS cache file
        self._user_tokens: Dict[str, str] = {}  # username -> token (populated at startup)
        self._user_id_to_name: Dict[str, str] = {}  # user_id (str) -> username (for RSS author lookup)
        self._resolved_uuids: Set[str] = set()  # UUIDs we've tried to resolve (avoid repeated API calls)
        self._newly_discovered_users: List[dict] = []  # Users found on plex.tv but not in settings
        self._users_loaded = False
        self._api_lock = threading.Lock()  # For rate limiting plex.tv calls
        self._plex_tv_reachable = True  # Track if plex.tv is accessible
        self._watchlist_data_complete = True  # Track if we got complete watchlist data

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

    def _load_tokens_from_settings(self, settings_users: List[dict],
                                    skip_users: List[str], machine_id: str) -> Set[str]:
        """Load user tokens from settings file (no plex.tv needed).

        Args:
            settings_users: List of user dicts from settings file with tokens.
            skip_users: List of usernames or tokens to skip.
            machine_id: Plex server machine identifier.

        Returns:
            Set of usernames loaded from settings.
        """
        settings_loaded = 0
        settings_usernames = set()

        for user_entry in settings_users:
            username = user_entry.get("title")
            token = user_entry.get("token")
            is_local = user_entry.get("is_local", False)
            user_id = user_entry.get("id")

            if not username:
                continue

            # Track username even if no token (user may have been auto-added with tracking prefs only)
            settings_usernames.add(username)

            # Skip token loading if no token present
            if not token:
                continue

            # Build user ID -> username map for RSS author lookup
            if user_id:
                self._user_id_to_name[str(user_id)] = username
                logging.debug(f"[PLEX API] Mapped ID {user_id} -> {username}")
            user_uuid = user_entry.get("uuid")
            if user_uuid:
                self._user_id_to_name[str(user_uuid)] = username
                logging.debug(f"[PLEX API] Mapped UUID {user_uuid} -> {username}")

            # Check skip list
            if username in skip_users or token in skip_users:
                logging.debug(f"[USER:{username}] Skipping (in skip list)")
                continue

            self._user_tokens[username] = token
            self._token_cache.set_token(username, token, machine_id)
            user_type = "home" if is_local else "remote"
            logging.debug(f"[USER:{username}] Loaded from settings ({user_type})")
            settings_loaded += 1

        logging.debug(f"[PLEX API] Loaded {settings_loaded} users from settings file")
        return settings_usernames

    def _get_main_account(self, main_username: Optional[str]) -> Optional['MyPlexAccount']:
        """Try to get main account info from plex.tv.

        Args:
            main_username: Main account username from settings (fallback).

        Returns:
            MyPlexAccount object if plex.tv reachable, None otherwise.
        """
        try:
            self._rate_limited_api_call()
            account = self.plex.myPlexAccount()
            actual_main_username = account.title

            # Update main account token with actual username from plex.tv
            if actual_main_username != main_username:
                if main_username and main_username in self._user_tokens:
                    del self._user_tokens[main_username]
                self._user_tokens[actual_main_username] = self.plex_token
                logging.debug(f"[PLEX API] Main account: {actual_main_username}")
            return account
        except Exception as e:
            _log_api_error("load user tokens", e)
            self._plex_tv_reachable = False
            self._watchlist_data_complete = False
            logging.warning("[PLEX API] plex.tv unreachable - using cached user data only")
            logging.warning("[PLEX API] Watchlist data will be incomplete - array restore will be skipped")
            return None

    def _discover_new_users(self, account: 'MyPlexAccount', settings_usernames: Set[str],
                            skip_users: List[str], machine_id: str) -> None:
        """Check plex.tv for new users not in settings.

        Args:
            account: MyPlexAccount object.
            settings_usernames: Set of usernames already loaded from settings.
            skip_users: List of usernames or tokens to skip.
            machine_id: Plex server machine identifier.
        """
        try:
            self._rate_limited_api_call()
            users = account.users()
            self._newly_discovered_users = []  # Reset for this run

            for user in users:
                username = user.title

                # Extract user metadata
                user_id = getattr(user, 'id', None)
                user_uuid = None
                thumb = getattr(user, 'thumb', '')
                if thumb and '/users/' in thumb:
                    try:
                        user_uuid = thumb.split('/users/')[1].split('/')[0]
                    except (IndexError, AttributeError):
                        pass

                # Build user ID -> username map (even if skipped)
                if user_id:
                    self._user_id_to_name[str(user_id)] = username
                if user_uuid:
                    self._user_id_to_name[user_uuid] = username

                # Skip if already loaded from settings
                if username in settings_usernames:
                    continue

                # Check skip list
                if username in skip_users:
                    logging.debug(f"[USER:{username}] Skipping (in skip list)")
                    continue

                # Helper to build user info dict
                def build_user_info(token: str) -> dict:
                    info = {
                        'title': username,
                        'token': token,
                        'is_local': False,  # account.users() returns shared/friend users
                        'skip_ondeck': True,
                        'skip_watchlist': True
                    }
                    if user_id:
                        info['id'] = user_id
                    if user_uuid:
                        info['uuid'] = user_uuid
                    return info

                # Try to get token from disk cache first
                cached_token = self._token_cache.get_token(username, machine_id)
                if cached_token:
                    if cached_token in skip_users:
                        logging.debug(f"[USER:{username}] Skipping (token in skip list)")
                        continue
                    self._user_tokens[username] = cached_token
                    logging.debug(f"[USER:{username}] Using cached token")
                    self._newly_discovered_users.append(build_user_info(cached_token))
                    continue

                # Fetch fresh token from plex.tv
                try:
                    self._rate_limited_api_call()
                    token = user.get_token(machine_id)
                    if token:
                        if token in skip_users:
                            logging.debug(f"[USER:{username}] Skipping (token in skip list)")
                            continue
                        self._user_tokens[username] = token
                        self._token_cache.set_token(username, token, machine_id)
                        logging.debug(f"[USER:{username}] Fetched fresh token")
                        self._newly_discovered_users.append(build_user_info(token))
                    else:
                        logging.debug(f"[USER:{username}] No token available")
                except Exception as e:
                    _log_api_error(f"get token for {username}", e)
        except Exception as e:
            _log_api_error("check for new users", e)

    def get_newly_discovered_users(self) -> List[dict]:
        """Return list of user info dicts for users found on plex.tv but not in settings.

        These users were discovered during load_user_tokens() and their tokens
        were loaded, but they need to be added to settings.

        Returns:
            List of dicts with keys: title, token, is_local, id, uuid, skip_ondeck, skip_watchlist
        """
        return self._newly_discovered_users

    def load_user_tokens(self, skip_users: Optional[List[str]] = None,
                         settings_users: Optional[List[dict]] = None,
                         main_username: Optional[str] = None) -> Dict[str, str]:
        """Load and cache tokens for all users at startup.

        Offline-resilient approach:
        1. First load tokens from settings file (no plex.tv needed)
        2. Then optionally check plex.tv for main account info and new users
        3. If plex.tv is unreachable, proceed with cached data only

        Args:
            skip_users: List of usernames or tokens to skip
            settings_users: List of user dicts from settings file with tokens
            main_username: Main account username from settings (fallback if plex.tv unreachable)

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

        # Step 1: Load tokens from settings file (no plex.tv needed)
        settings_usernames = self._load_tokens_from_settings(settings_users, skip_users, machine_id)

        # Add main account token from settings as fallback
        if main_username and main_username not in skip_users:
            self._user_tokens[main_username] = self.plex_token
            logging.debug(f"[PLEX API] Added main account from settings: {main_username}")

        # Step 2: Try to get main account info from plex.tv
        account = self._get_main_account(main_username)

        # Step 3: Check for new users not in settings
        if account and self._plex_tv_reachable:
            self._discover_new_users(account, settings_usernames, skip_users, machine_id)

        self._users_loaded = True
        total_users = len(self._user_tokens)
        logging.info(f"Connected to Plex ({total_users} users)")
        if self._user_tokens:
            user_names = sorted(self._user_tokens.keys(), key=str.lower)
            logging.info(f"USERS: {', '.join(user_names)}")
        return self._user_tokens

    def get_user_token(self, username: str) -> Optional[str]:
        """Get a cached token for a user (must call load_user_tokens first)."""
        return self._user_tokens.get(username)

    def invalidate_user_token(self, username: str) -> None:
        """Invalidate a user's token (e.g., after auth failure)."""
        if username in self._user_tokens:
            del self._user_tokens[username]
        self._token_cache.invalidate(username)

    def is_plex_tv_reachable(self) -> bool:
        """Check if plex.tv was reachable during token loading."""
        return self._plex_tv_reachable

    def is_watchlist_data_complete(self) -> bool:
        """Check if watchlist data is complete (no fetch failures)."""
        return self._watchlist_data_complete

    def mark_watchlist_incomplete(self) -> None:
        """Mark watchlist data as incomplete (e.g., after fetch failure)."""
        self._watchlist_data_complete = False

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
    
    def search_plex(self, title: str, guid: str = None, expected_type: str = None,
                     valid_sections: List[int] = None):
        """Search for a file in the Plex server.

        Args:
            title: The title to search for (used as fallback)
            guid: IMDB/TVDB GUID like 'imdb://tt0898367' or 'tvdb://267247' (preferred)
            expected_type: Expected type ('movie' or 'show') to filter results
            valid_sections: List of section IDs to search in (None = all sections)

        Returns:
            Matched Plex item or None if not found
        """
        # Try GUID lookup first (most accurate)
        if guid:
            for section in self.plex.library.sections():
                # Skip sections not in valid_sections if specified
                if valid_sections and int(section.key) not in valid_sections:
                    continue
                try:
                    item = section.getGuid(guid)
                    if item:
                        # Verify the item actually has this GUID (defensive check against PlexAPI bugs)
                        item_guids = [g.id for g in getattr(item, 'guids', [])]
                        if guid in item_guids:
                            logging.debug(f"GUID lookup matched '{item.title}' ({item.TYPE}) for {guid}")
                            return item
                        # getGuid returned wrong item (can happen with items that have empty GUIDs)
                        continue
                except NotFound:
                    pass
                except Exception as e:
                    logging.debug(f"GUID lookup error for {guid}: {e}")

        # No GUID match found - item is not in library
        # Note: We intentionally do NOT fall back to title search as it can return
        # incorrect matches (e.g., "Weapons" matching to "Mary Poppins")
        logging.debug(f"No GUID match found for '{title}' (guid={guid}) — item not in library")
        return None
    
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
                    logging.info(f"[USER:{username}] Skipping for OnDeck — in skip list")
                    continue
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

        # Log OnDeck items grouped by user (sequential output after parallel fetch)
        items_by_user: Dict[str, List[OnDeckItem]] = {}
        for item in on_deck_files:
            if item.username not in items_by_user:
                items_by_user[item.username] = []
            items_by_user[item.username].append(item)

        for username in sorted(items_by_user.keys()):
            items = items_by_user[username]
            for item in items:
                logging.debug(f"[USER:{username}] OnDeck found: {item.file_path}")
            logging.debug(f"[USER:{username}] Found {len(items)} OnDeck items")

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
                logging.info(f"[USER:{username}] Skipping OnDeck fetch — no Plex instance available")
                return []

            logging.debug(f"[USER:{username}] Fetching onDeck media...")

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
        return re.sub(r"\s\(\d{4}\)$", "", title)

    # -------------------- Watchlist Helper Methods --------------------

    def _parse_rss_response(self, text: str) -> List[Tuple[str, str, Optional[datetime], str, str]]:
        """Parse RSS XML response into list of items.

        Returns list of tuples: (title, category, pub_date, author_id, guid)
        The guid contains IMDB/TVDB IDs like 'imdb://tt0898367' or 'tvdb://267247'
        """
        root = ET.fromstring(text)
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
                except (ValueError, TypeError):
                    pass  # Invalid date format, use None
            # Get author ID (Plex user ID who added to watchlist)
            author_id = ""
            author_elem = item.find("author")
            if author_elem is not None and author_elem.text:
                author_id = author_elem.text
            # Get GUID (IMDB/TVDB ID) for accurate matching
            guid = ""
            guid_elem = item.find("guid")
            if guid_elem is not None and guid_elem.text:
                guid = guid_elem.text
            items.append((title, category, pub_date, author_id, guid))
        return items

    def _save_rss_cache(self, url: str, items: List[Tuple[str, str, Optional[datetime], str, str]]) -> None:
        """Save RSS items to cache file."""
        if not self._rss_cache_file:
            return
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'url': url,
                'items': [
                    (title, category, pub_date.isoformat() if pub_date else None, author_id, guid)
                    for title, category, pub_date, author_id, guid in items
                ]
            }
            with open(self._rss_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            logging.debug(f"Saved {len(items)} RSS items to cache")
        except IOError as e:
            logging.debug(f"Failed to save RSS cache: {e}")

    def _load_rss_cache(self) -> List[Tuple[str, str, Optional[datetime], str, str]]:
        """Load RSS items from cache file."""
        if not self._rss_cache_file:
            return []
        try:
            if os.path.exists(self._rss_cache_file):
                with open(self._rss_cache_file, 'r') as f:
                    cache_data = json.load(f)
                items = []
                for item_data in cache_data['items']:
                    # Handle both old format (4 fields) and new format (5 fields with guid)
                    if len(item_data) == 4:
                        title, category, pub_date_str, author_id = item_data
                        guid = ""
                    else:
                        title, category, pub_date_str, author_id, guid = item_data
                    pub_date = datetime.fromisoformat(pub_date_str) if pub_date_str else None
                    items.append((title, category, pub_date, author_id, guid))
                cache_time = datetime.fromisoformat(cache_data['timestamp'])
                cache_age = datetime.now() - cache_time
                cache_age_hours = cache_age.total_seconds() / 3600
                logging.warning(f"Using cached RSS data ({len(items)} items, {cache_age_hours:.1f} hours old)")
                return items
        except Exception as e:
            logging.debug(f"Failed to load RSS cache: {e}")
        return []

    def _fetch_rss_titles(self, url: str) -> List[Tuple[str, str, Optional[datetime], str, str]]:
        """Fetch titles, categories, pubDate, author ID, and GUID from a Plex RSS feed.

        Retries up to RSS_MAX_RETRIES times with exponential backoff.
        Falls back to cached data if all retries fail.

        Returns list of tuples: (title, category, pub_date, author_id, guid)
        """
        # Retry loop with exponential backoff
        last_error = None
        for attempt in range(RSS_MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=RSS_TIMEOUT)
                resp.raise_for_status()
                items = self._parse_rss_response(resp.text)
                self._save_rss_cache(url, items)  # Cache successful result
                return items
            except (requests.RequestException, ET.ParseError) as e:
                last_error = e
                if attempt < RSS_MAX_RETRIES - 1:
                    wait_time = 2 ** attempt  # 1s, 2s, 4s
                    logging.warning(f"RSS fetch attempt {attempt + 1}/{RSS_MAX_RETRIES} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)

        # All retries failed - try cache
        logging.error(f"Failed to fetch RSS feed after {RSS_MAX_RETRIES} attempts: {last_error}")
        cached_items = self._load_rss_cache()
        if cached_items:
            return cached_items

        logging.error("No cached RSS data available - remote watchlist items will be missing!")
        return []

    def _process_watchlist_show(self, file, watchlist_episodes: int, username: str,
                                 watchlisted_at: Optional[datetime]) -> Generator[Tuple[str, str, Optional[datetime], Optional[Dict]], None, None]:
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
                    file_path = episode.media[0].parts[0].file
                    logging.debug(f"[USER:{username}] Watchlist found: {file_path}")
                    # Build episode_info from Plex metadata (same format as OnDeck)
                    episode_info = None
                    ep_season = getattr(episode, 'parentIndex', None)
                    ep_index = getattr(episode, 'index', None)
                    if ep_season is not None and ep_index is not None:
                        episode_info = {
                            'show': file.title,
                            'season': ep_season,
                            'episode': ep_index
                        }
                    yield (file_path, username, watchlisted_at, episode_info)
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

    def _process_watchlist_movie(self, file, username: str,
                                  watchlisted_at: Optional[datetime]) -> Generator[Tuple[str, str, Optional[datetime], Optional[Dict]], None, None]:
        """Process a movie and yield file path with metadata."""
        if len(file.media) > 0 and len(file.media[0].parts) > 0:
            file_path = file.media[0].parts[0].file
            logging.debug(f"[USER:{username}] Watchlist found: {file_path}")
            yield (file_path, username, watchlisted_at, None)

    def _fetch_user_watchlist(self, user, valid_sections: List[int], watchlist_episodes: int,
                               skip_watchlist: List[str], rss_url: Optional[str],
                               filtered_sections: List[int]) -> Generator[Tuple[str, str, Optional[datetime], Optional[Dict]], None, None]:
        """Fetch watchlist media for a user, yielding file paths with metadata.

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

        logging.debug(f"[USER:{current_username}] Fetching watchlist media")

        # Skip users in the skip list (use cached tokens)
        if user:
            token = self._user_tokens.get(current_username)
            if not token:
                logging.warning(f"[USER:{current_username}] No cached token; skipping watchlist")
                return
            if token in skip_watchlist or current_username in skip_watchlist:
                logging.info(f"[USER:{current_username}] Skipping — in watchlist skip list")
                return

        # --- Obtain Plex account instance ---
        try:
            fresh_session = requests.Session()

            if user is None:
                # Main account - use the main token with a fresh session
                self._rate_limited_api_call()
                account = MyPlexAccount(token=self.plex_token, session=fresh_session)
                logging.debug(f"[USER:{current_username}] Created fresh MyPlexAccount (main user)")
            else:
                # Home/managed user - create fresh admin account then switch to home user
                try:
                    self._rate_limited_api_call()
                    fresh_admin_account = MyPlexAccount(token=self.plex_token, session=fresh_session)
                    self._rate_limited_api_call()
                    account = fresh_admin_account.switchHomeUser(current_username)
                    logging.debug(f"[USER:{current_username}] Switched to home user via fresh admin account")
                except Exception as e:
                    _log_api_error(f"switch to home user {current_username}", e)
                    self.mark_watchlist_incomplete()
                    return
        except Exception as e:
            _log_api_error(f"get Plex account for {current_username}", e)
            self.mark_watchlist_incomplete()
            return

        # --- RSS feed processing ---
        if rss_url:
            yield from self._process_rss_watchlist(rss_url, current_username, filtered_sections, watchlist_episodes, skip_watchlist)
            return

        # --- Local Plex watchlist processing ---
        try:
            self._rate_limited_api_call()
            watchlist = account.watchlist(filter='released', sort='watchlistedAt:desc')
            logging.debug(f"[USER:{current_username}] Found {len(watchlist)} watchlist items")
            for item in watchlist:
                watchlisted_at = None
                try:
                    user_state = account.userState(item)
                    watchlisted_at = getattr(user_state, 'watchlistedAt', None)
                except Exception as e:
                    logging.debug(f"Could not get userState for {item.title}: {e}")

                # Extract GUID for accurate matching (prefer IMDB, then TVDB)
                guid = None
                item_guids = getattr(item, 'guids', [])
                for g in item_guids:
                    gid = getattr(g, 'id', str(g))
                    if gid.startswith('imdb://') or gid.startswith('tvdb://'):
                        guid = gid
                        break

                # Determine expected type from watchlist item
                expected_type = getattr(item, 'type', None)

                file = self.search_plex(item.title, guid=guid, expected_type=expected_type,
                                       valid_sections=filtered_sections)
                if file and (not filtered_sections or file.librarySectionID in filtered_sections):
                    try:
                        if file.TYPE == 'show':
                            yield from self._process_watchlist_show(file, watchlist_episodes, current_username, watchlisted_at)
                        elif file.TYPE == 'movie':
                            yield from self._process_watchlist_movie(file, current_username, watchlisted_at)
                        else:
                            logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                    except Exception as e:
                        logging.warning(f"Error processing '{file.title}': {e}")
                elif file:
                    logging.debug(f"Skipping watchlist item '{file.title}' — section {file.librarySectionID} not in valid_sections {filtered_sections}")
        except Exception as e:
            logging.error(f"[USER:{current_username}] Error fetching watchlist: {e}")
            self.mark_watchlist_incomplete()

    def _process_rss_watchlist(self, rss_url: str, current_username: str,
                                filtered_sections: List[int], watchlist_episodes: int,
                                skip_watchlist: List[str] = None) -> Generator[Tuple[str, str, Optional[datetime], Optional[Dict]], None, None]:
        """Process RSS feed items and yield matching media files.

        Args:
            skip_watchlist: List of usernames to skip (filters RSS items by who added them)
        """
        if skip_watchlist is None:
            skip_watchlist = []
        rss_items = self._fetch_rss_titles(rss_url)
        logging.debug(f"RSS feed contains {len(rss_items)} items")
        unknown_user_ids = set()
        rss_not_found = []
        rss_skipped_users = {}  # username -> count of skipped items

        for title, category, pub_date, author_id, guid in rss_items:
            # Look up username from author ID
            if author_id and author_id in self._user_id_to_name:
                rss_username = self._user_id_to_name[author_id]
            elif author_id:
                resolved_name = self.resolve_user_uuid(author_id)
                if resolved_name:
                    rss_username = resolved_name
                else:
                    rss_username = f"User#{author_id}"
                    unknown_user_ids.add(author_id)
            else:
                rss_username = "Friends (RSS)"

            # Skip items from users in the skip list (fixes issue #51)
            if skip_watchlist and rss_username in skip_watchlist:
                logging.debug(f"RSS: Skipping '{title}' — added by {rss_username} (in skip list)")
                rss_skipped_users[rss_username] = rss_skipped_users.get(rss_username, 0) + 1
                continue

            cleaned_title = self.clean_rss_title(title)
            file = self.search_plex(cleaned_title, guid=guid, expected_type=category,
                                   valid_sections=filtered_sections)
            if file:
                logging.debug(f"RSS title '{title}' matched Plex item '{file.title}' ({file.TYPE})")
                try:
                    if file.TYPE == 'show':
                        yield from self._process_watchlist_show(file, watchlist_episodes, rss_username, pub_date)
                    elif file.TYPE == 'movie':
                        yield from self._process_watchlist_movie(file, rss_username, pub_date)
                    else:
                        logging.debug(f"Ignoring item '{file.title}' of type '{file.TYPE}'")
                except Exception as e:
                    logging.warning(f"Error processing '{file.title}': {e}")
            else:
                rss_not_found.append((title, rss_username))
                logging.debug(f"RSS title '{title}' (added by {rss_username}) not found in Plex — discarded")

        # Log summary of not-found items
        if rss_not_found:
            if logging.getLogger().getEffectiveLevel() <= logging.DEBUG:
                logging.info(f"RSS: Skipped {len(rss_not_found)} items not in library")
            else:
                logging.info(f"RSS: Skipped {len(rss_not_found)} items not in library (use --verbose for details)")

        # Log summary of skipped users
        if rss_skipped_users:
            total_skipped = sum(rss_skipped_users.values())
            user_summary = ", ".join(f"{user}: {count}" for user, count in sorted(rss_skipped_users.items()))
            logging.info(f"RSS: Skipped {total_skipped} items from disabled users ({user_summary})")

        if unknown_user_ids:
            logging.debug(f"[PLEX API] {len(unknown_user_ids)} unknown user ID(s) in RSS feed: {', '.join(sorted(unknown_user_ids))}. Run 'python3 plexcache_setup.py' and refresh users to resolve.")

    def get_watchlist_media(self, valid_sections: List[int], watchlist_episodes: int,
                            users_toggle: bool, skip_watchlist: List[str], rss_url: Optional[str] = None,
                            home_users: Optional[List[str]] = None) -> Generator[Tuple[str, str, Optional[datetime], Optional[Dict]], None, None]:
        """Get watchlist media files, optionally via RSS, with proper user filtering.

        Args:
            valid_sections: List of library section IDs to include.
            watchlist_episodes: Number of episodes to fetch per show.
            users_toggle: Whether to include other users' media.
            skip_watchlist: List of usernames or tokens to skip.
            rss_url: Optional RSS feed URL for remote user watchlists.
            home_users: List of usernames that are home/managed users (can access watchlist).

        Yields:
            Tuples of (file_path, username, watchlisted_at, episode_info) where watchlisted_at
            is the datetime when the item was added to the user's watchlist (None for RSS items),
            and episode_info is a dict with 'show', 'season', 'episode' keys for TV episodes
            (None for movies).
        """
        if home_users is None:
            home_users = []

        # Build filtered sections list
        available_sections = [section.key for section in self.plex.library.sections()]
        filtered_sections = list(set(available_sections) & set(valid_sections))

        # Prepare users to fetch
        users_to_fetch = [None]  # always include the main local account

        if users_toggle:
            for username, token in self._user_tokens.items():
                if token == self.plex_token:
                    continue
                if username in skip_watchlist or token in skip_watchlist:
                    logging.info(f"[USER:{username}] Skipping for watchlist — in skip list")
                    continue
                if username not in home_users:
                    continue
                users_to_fetch.append(UserProxy(username))

        logging.debug(f"Processing {len(users_to_fetch)} users for watchlist (main + {len(users_to_fetch)-1} home users)")

        # Fetch concurrently using extracted helper methods
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(
                    self._fetch_user_watchlist, user, valid_sections, watchlist_episodes,
                    skip_watchlist, rss_url, filtered_sections
                )
                for user in users_to_fetch
            }
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
