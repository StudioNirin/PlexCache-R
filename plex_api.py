"""
Plex API integration for PlexCache.
Handles Plex server connections and media fetching operations.
"""

import json
import logging
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Set, Optional, Generator, Tuple

from plexapi.server import PlexServer
from plexapi.video import Episode, Movie
from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound, BadRequest
import requests


class PlexManager:
    """Manages Plex server connections and operations."""
    
    def __init__(self, plex_url: str, plex_token: str, retry_limit: int = 3, delay: int = 5):
        self.plex_url = plex_url
        self.plex_token = plex_token
        self.retry_limit = retry_limit
        self.delay = delay
        self.plex = None
        
    def connect(self) -> None:
        """Connect to the Plex server."""
        logging.info(f"Connecting to Plex server: {self.plex_url}")
        
        try:
            self.plex = PlexServer(self.plex_url, self.plex_token)
            logging.info("Successfully connected to Plex server")
            logging.debug(f"Plex server version: {self.plex.version}")
        except Exception as e:
            logging.error(f"Error connecting to the Plex server: {e}")
            raise ConnectionError(f"Error connecting to the Plex server: {e}")
    
    def get_plex_instance(self, user=None) -> Tuple[Optional[str], Optional[PlexServer]]:
        """Get Plex instance for a specific user."""
        if user:
            username = user.title
            try:
                return username, PlexServer(self.plex_url, user.get_token(self.plex.machineIdentifier))
            except Exception as e:
                logging.error(f"Error: Failed to fetch {username} onDeck media. Error: {e}")
                return None, None
        else:
            username = self.plex.myPlexAccount().title
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
        """Get OnDeck media files, skipping users with no token to prevent 401 errors."""
        on_deck_files = []

        # Build list of users to fetch
        users_to_fetch = [None]  # Always include main local account
        if users_toggle:
            for user in self.plex.myPlexAccount().users():
                try:
                    token = user.get_token(self.plex.machineIdentifier)
                    if not token:
                        logging.info(f"Skipping {user.title} for OnDeck — no token available")
                        continue
                    if token in skip_ondeck:
                        logging.info(f"Skipping {user.title} for OnDeck — token in skip list")
                        continue
                    users_to_fetch.append(user)
                except Exception as e:
                    logging.warning(f"Could not get token for {user.title}; skipping. Error: {e}")

        logging.info(f"Fetching OnDeck media for {len(users_to_fetch)} users")

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
        """Fetch onDeck media for a specific user, skipping users with no token."""
        try:
            username, plex_instance = self.get_plex_instance(user)
            if not plex_instance:
                logging.info(f"Skipping OnDeck fetch for {username} — no Plex instance available (likely no token).")
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
            logging.error(f"An error occurred while fetching onDeck media for {username}: {e}")
            return []
    
    def _process_episode_ondeck(self, video: Episode, number_episodes: int, on_deck_files: List[str]) -> None:
        """Process an episode from onDeck."""
        for media in video.media:
            on_deck_files.extend(part.file for part in media.parts)
        
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

            time.sleep(1)  # slight delay for rate-limit protection
            current_username = self.plex.myPlexAccount().title if user is None else user.title
            logging.info(f"Fetching watchlist media for {current_username}")

            # Build list of valid sections for filtering
            available_sections = [section.key for section in self.plex.library.sections()]
            filtered_sections = list(set(available_sections) & set(valid_sections))

            # Skip users in the skip list
            if user:
                try:
                    token = user.get_token(self.plex.machineIdentifier)
                except Exception as e:
                    logging.warning(f"Could not get token for {current_username}; skipping. Error: {e}")
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
                        account = self.plex.myPlexAccount().switchHomeUser(user.title)
                    except Exception as e:
                        logging.warning(f"Could not switch to user {user.title}; skipping. Error: {e}")
                        return
            except Exception as e:
                logging.error(f"Failed to get Plex account for {current_username}: {e}")
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


        # --- Prepare users to fetch ---
        users_to_fetch = [None]  # always include the main local account

        if users_toggle:
            for user in self.plex.myPlexAccount().users():
                title = getattr(user, "title", None)
                username = getattr(user, "username", None)  # None for local/home users

                if username is not None:
                    logging.info(f"Skipping remote user {title} (remote accounts are processed via RSS, not API)")
                    continue

                try:
                    user_token = user.get_token(self.plex.machineIdentifier)
                except Exception as e:
                    logging.warning(f"Could not get token for {title}; skipping. Error: {e}")
                    continue

                if (user_token and user_token in skip_watchlist) or (title and title in skip_watchlist):
                    logging.info(f"Skipping {title} (in skip_watchlist)")
                    continue

                users_to_fetch.append(user)

        logging.info(f"Processing {len(users_to_fetch)} users for local Plex watchlist")

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
                        if "429" in str(e):
                            logging.warning(f"Rate limit exceeded. Retrying in {self.delay} seconds... Error: {e}")
                            time.sleep(self.delay)
                            retries += 1
                        else:
                            logging.error(f"Error fetching watchlist media: {e}")
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
