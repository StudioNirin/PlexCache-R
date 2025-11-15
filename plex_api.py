"""
Plex API integration for PlexCache.
Handles Plex server connections and media fetching operations.
"""

import json
import logging
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Set, Optional, Generator, Tuple

from plexapi.server import PlexServer
from plexapi.video import Episode, Movie
from plexapi.myplex import MyPlexAccount
from plexapi.exceptions import NotFound, BadRequest


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
                logging.error(f"Error: Failed to Fetch {username} onDeck media. Error: {e}")
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
        """Get onDeck media files."""
        on_deck_files = []
        
        users_to_fetch = [None]  # Start with main user (None)
        if users_toggle:
            users_to_fetch += self.plex.myPlexAccount().users()
            # Filter out the users present in skip_ondeck
            users_to_fetch = [user for user in users_to_fetch 
                            if (user is None) or (user.get_token(self.plex.machineIdentifier) not in skip_ondeck)]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._fetch_user_on_deck_media, valid_sections, days_to_monitor, 
                                     number_episodes, user) for user in users_to_fetch}
            for future in as_completed(futures):
                try:
                    on_deck_files.extend(future.result())
                except Exception as e:
                    logging.error(f"An error occurred while fetching onDeck media for a user: {e}")
        
        return on_deck_files
    
    def _fetch_user_on_deck_media(self, valid_sections: List[int], days_to_monitor: int, 
                                 number_episodes: int, user=None) -> List[str]:
        """Fetch onDeck media for a specific user."""
        try:
            username, plex_instance = self.get_plex_instance(user)
            if not plex_instance:
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

            return on_deck_files

        except Exception as e:
            logging.error(f"An error occurred while fetching onDeck media: {e}")
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
    
    def get_watchlist_media(self, valid_sections: List[int], watchlist_episodes: int, 
                           users_toggle: bool, skip_watchlist: List[str]) -> Generator[str, None, None]:
        """Get watchlist media files."""
        def get_watchlist(token: str, user=None, retries: int = 0) -> List:
            """Retrieve the watchlist for the specified user's token."""
            account = MyPlexAccount(token=token)
            try:
                if user:
                    account = account.switchHomeUser(f'{user.title}')
                watchlist = account.watchlist(filter='released')
                logging.debug(f"Found {len(watchlist)} items in watchlist for user {user.title if user else 'main'}")
                return watchlist
            except (BadRequest, NotFound) as e:
                if "429" in str(e) and retries < self.retry_limit:
                    logging.warning(f"Rate limit exceeded. Retrying {retries + 1}/{self.retry_limit}. Sleeping for {self.delay} seconds...")
                    time.sleep(self.delay)
                    return get_watchlist(token, user, retries + 1)
                elif isinstance(e, NotFound):
                    logging.warning(f"Failed to switch to user {user.title if user else 'Unknown'}. Skipping...")
                    return []
                else:
                    raise e

        def process_show(file, watchlist_episodes: int) -> Generator[str, None, None]:
            """Process episodes of a TV show file up to a specified number."""
            episodes = file.episodes()
            logging.debug(f"Processing show {file.title} with {len(episodes)} episodes")
            for episode in episodes[:watchlist_episodes]:
                if len(episode.media) > 0 and len(episode.media[0].parts) > 0:
                    if not episode.isPlayed:
                        yield episode.media[0].parts[0].file

        def process_movie(file) -> Generator[str, None, None]:
            """Process a movie file."""
            # Remove the isPlayed check - move to cache regardless of watch status
            if len(file.media) > 0 and len(file.media[0].parts) > 0:
                file_path = file.media[0].parts[0].file
                yield file_path

        def fetch_user_watchlist(user) -> List[str]:
            # Delay 2 seconds before querying this user to reduce rate limiting
            time.sleep(2)
            
            current_username = self.plex.myPlexAccount().title if user is None else user.title
            available_sections = [section.key for section in self.plex.library.sections()]
            filtered_sections = list(set(available_sections) & set(valid_sections))

            if user and user.get_token(self.plex.machineIdentifier) in skip_watchlist:
                logging.info(f"Skipping {current_username}'s watchlist media...")
                return []

            logging.info(f"Fetching {current_username}'s watchlist media...")
            try:
                watchlist = get_watchlist(self.plex_token, user)
                results = []

                for item in watchlist:
                    file = self.search_plex(item.title)
                    if file:
                        if not filtered_sections or (file.librarySectionID in filtered_sections):
                            if file.TYPE == 'show':
                                results.extend(process_show(file, watchlist_episodes))
                            else:
                                results.extend(process_movie(file))
                        
                return results
            except Exception as e:
                logging.error(f"Error fetching watchlist for {current_username}: {str(e)}")
                return []

        users_to_fetch = [None]  # Start with main user (None)
        if users_toggle:
            users_to_fetch += self.plex.myPlexAccount().users()
            
        logging.debug(f"Processing {len(users_to_fetch)} users for watchlist")

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_user_watchlist, user) for user in users_to_fetch}
            for future in as_completed(futures):
                retries = 0
                while retries < self.retry_limit:
                    try:
                        yield from future.result()
                        break
                    except Exception as e:
                        if "429" in str(e):  # rate limit error
                            logging.warning(f"Rate limit exceeded. Retrying in {self.delay} seconds...")
                            time.sleep(self.delay)
                            retries += 1
                        else:
                            logging.error(f"Error fetching watchlist media: {str(e)}")
                            break
    
    def get_watched_media(self, valid_sections: List[int], last_updated: Optional[float], 
                         users_toggle: bool) -> Generator[str, None, None]:
        """Get watched media files."""
        def fetch_user_watched_media(plex_instance: PlexServer, username: str, retries: int = 0) -> Generator[str, None, None]:
            try:
                logging.info(f"Fetching {username}'s watched media...")
                # Get all sections available for the user
                all_sections = [section.key for section in plex_instance.library.sections()]
                # Check if valid_sections is specified. If not, consider all available sections as valid.
                if valid_sections:
                    available_sections = list(set(all_sections) & set(valid_sections))
                else:
                    available_sections = all_sections
                
                # Filter sections the user has access to
                user_accessible_sections = [section for section in available_sections if section in all_sections]
                
                for section_key in user_accessible_sections:
                    section = plex_instance.library.sectionByID(section_key)
                    # Search for videos in the section
                    for video in section.search(unwatched=False):
                        # Skip if the video was last viewed before the last_updated timestamp
                        if video.lastViewedAt and last_updated and video.lastViewedAt < datetime.fromtimestamp(last_updated):
                            continue
                        # Process the video and yield the file path
                        yield from process_video(video)

            except (BadRequest, NotFound) as e:
                if "429" in str(e) and retries < self.retry_limit:
                    logging.warning(f"Rate limit exceeded. Retrying {retries + 1}/{self.retry_limit}. Sleeping for {self.delay} seconds...")
                    time.sleep(self.delay)
                    return fetch_user_watched_media(plex_instance, username, retries + 1)
                elif isinstance(e, NotFound):
                    logging.warning(f"Failed to switch to user {username}. Skipping...")
                    return
                else:
                    raise e

        def process_video(video) -> Generator[str, None, None]:
            """Process a video and yield file paths."""
            if video.TYPE == 'show':
                # Iterate through each episode of a show video
                for episode in video.episodes():
                    yield from process_episode(episode)
            else:
                # Get the file path of the video
                file_path = video.media[0].parts[0].file
                yield file_path

        def process_episode(episode) -> Generator[str, None, None]:
            """Process an episode and yield file paths."""
            # Iterate through each media and part of an episode
            for media in episode.media:
                for part in media.parts:
                    if episode.isPlayed:
                        # Get the file path of the played episode
                        file_path = part.file
                        yield file_path

        # Create a ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            main_username = self.plex.myPlexAccount().title
            
            # Start a new task for the main user
            futures = [executor.submit(fetch_user_watched_media, self.plex, main_username)]
            
            if users_toggle:
                for user in self.plex.myPlexAccount().users():
                    username = user.title
                    user_token = user.get_token(self.plex.machineIdentifier)
                    user_plex = PlexServer(self.plex_url, user_token)

                    # Start a new task for each other user
                    futures.append(executor.submit(fetch_user_watched_media, user_plex, username))
            
            # As each task completes, yield the results
            for future in as_completed(futures):
                try:
                    yield from future.result()
                except Exception as e:
                    logging.error(f"An error occurred in get_watched_media: {e}")


class CacheManager:
    """Manages cache operations for media files."""
    
    @staticmethod
    def load_media_from_cache(cache_file: Path) -> Tuple[Set[str], Optional[float]]:
        """Load watched media from cache."""
        if cache_file.exists():
            with cache_file.open('r') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return set(data.get('media', [])), data.get('timestamp')
                    elif isinstance(data, list):
                        # cache file contains just a list of media, without timestamp
                        return set(data), None
                except json.JSONDecodeError:
                    # Clear the file and return an empty set
                    with cache_file.open('w') as f:
                        f.write(json.dumps({'media': [], 'timestamp': None}))
                    return set(), None
        return set(), None
    
    @staticmethod
    def save_media_to_cache(cache_file: Path, media_list: List[str], timestamp: Optional[float] = None) -> None:
        """Save media list to cache file."""
        if timestamp is None:
            timestamp = datetime.now().timestamp()
        
        with cache_file.open('w') as f:
            json.dump({'media': media_list, 'timestamp': timestamp}, f) 
