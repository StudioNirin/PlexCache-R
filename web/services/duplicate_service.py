"""Duplicate file scanner service — finds Plex items with multiple media files.

Scans all Plex library sections for items that have more than one media file,
which typically means duplicate downloads or failed Sonarr/Radarr upgrades.

If Sonarr/Radarr credentials are configured, automatically classifies files as
keeper (tracked by arr) or orphan (safe to delete).
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Callable, Dict, List, Optional

from core.file_operations import save_json_atomically
from core.system_utils import format_bytes
from web.services.maintenance_service import ActionResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class DuplicateFileEntry:
    """A single file within a duplicate Plex item."""
    file_path: str          # Plex container path (/data/...)
    fs_path: str            # Filesystem path after translation
    size: int
    size_display: str
    resolution: str
    container: str
    video_codec: str
    bitrate: Optional[int]
    is_keeper: Optional[bool] = None  # None = unresolved


@dataclass
class PlexDuplicateItem:
    """A Plex item (movie/episode) with 2+ media files."""
    rating_key: str
    title: str
    item_type: str          # 'movie' or 'episode'
    library: str
    files: List[DuplicateFileEntry] = field(default_factory=list)
    keeper_file: Optional[str] = None     # fs_path of the keeper
    orphan_files: List[str] = field(default_factory=list)  # fs_paths of orphans
    orphan_bytes: int = 0
    is_resolved: bool = False


@dataclass
class DuplicateScanResults:
    """Full scan output."""
    scanned_at: str         # ISO 8601
    scan_duration_seconds: float
    total_items: int        # Total Plex items scanned
    duplicate_count: int    # Items with 2+ files
    orphan_count: int       # Files classified as orphans
    orphan_bytes: int
    orphan_bytes_display: str  # Human-readable (e.g., "425.30 GB")
    unresolved_count: int   # Items where keeper couldn't be determined
    arr_enabled: bool
    libraries_scanned: List[str]
    items: List[PlexDuplicateItem] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------

def _file_entry_to_dict(entry: DuplicateFileEntry) -> dict:
    return {
        "file_path": entry.file_path,
        "fs_path": entry.fs_path,
        "size": entry.size,
        "size_display": entry.size_display,
        "resolution": entry.resolution,
        "container": entry.container,
        "video_codec": entry.video_codec,
        "bitrate": entry.bitrate,
        "is_keeper": entry.is_keeper,
    }


def _item_to_dict(item: PlexDuplicateItem) -> dict:
    return {
        "rating_key": item.rating_key,
        "title": item.title,
        "item_type": item.item_type,
        "library": item.library,
        "files": [_file_entry_to_dict(f) for f in item.files],
        "keeper_file": item.keeper_file,
        "orphan_files": item.orphan_files,
        "orphan_bytes": item.orphan_bytes,
        "is_resolved": item.is_resolved,
    }


def _results_to_dict(results: DuplicateScanResults) -> dict:
    return {
        "scanned_at": results.scanned_at,
        "scan_duration_seconds": results.scan_duration_seconds,
        "total_items": results.total_items,
        "duplicate_count": results.duplicate_count,
        "orphan_count": results.orphan_count,
        "orphan_bytes": results.orphan_bytes,
        "orphan_bytes_display": results.orphan_bytes_display,
        "unresolved_count": results.unresolved_count,
        "arr_enabled": results.arr_enabled,
        "libraries_scanned": results.libraries_scanned,
        "items": [_item_to_dict(i) for i in results.items],
    }


def _dict_to_file_entry(d: dict) -> DuplicateFileEntry:
    return DuplicateFileEntry(
        file_path=d["file_path"],
        fs_path=d["fs_path"],
        size=d["size"],
        size_display=d["size_display"],
        resolution=d["resolution"],
        container=d["container"],
        video_codec=d["video_codec"],
        bitrate=d.get("bitrate"),
        is_keeper=d.get("is_keeper"),
    )


def _dict_to_item(d: dict) -> PlexDuplicateItem:
    return PlexDuplicateItem(
        rating_key=d["rating_key"],
        title=d["title"],
        item_type=d["item_type"],
        library=d["library"],
        files=[_dict_to_file_entry(f) for f in d.get("files", [])],
        keeper_file=d.get("keeper_file"),
        orphan_files=d.get("orphan_files", []),
        orphan_bytes=d.get("orphan_bytes", 0),
        is_resolved=d.get("is_resolved", False),
    )


def _dict_to_results(d: dict) -> DuplicateScanResults:
    orphan_bytes = d["orphan_bytes"]
    return DuplicateScanResults(
        scanned_at=d["scanned_at"],
        scan_duration_seconds=d["scan_duration_seconds"],
        total_items=d["total_items"],
        duplicate_count=d["duplicate_count"],
        orphan_count=d["orphan_count"],
        orphan_bytes=orphan_bytes,
        orphan_bytes_display=d.get("orphan_bytes_display", format_bytes(orphan_bytes)),
        unresolved_count=d["unresolved_count"],
        arr_enabled=d["arr_enabled"],
        libraries_scanned=d.get("libraries_scanned", []),
        items=[_dict_to_item(i) for i in d.get("items", [])],
    )


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

SCAN_RESULTS_FILE = os.path.join("data", "duplicate_scan.json")


class DuplicateService:
    """Scans Plex libraries for duplicate files and manages cleanup."""

    def __init__(self):
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public: scan
    # ------------------------------------------------------------------

    def scan_plex_libraries(
        self,
        paths: Optional[List[str]] = None,
        dry_run: bool = False,
        stop_check: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable] = None,
        bytes_progress_callback: Optional[Callable] = None,
        max_workers: int = 1,
        active_callback: Optional[Callable] = None,
    ) -> ActionResult:
        """Scan all Plex libraries for items with multiple media files.

        Signature matches MaintenanceRunner's expected service method interface.
        ``paths`` and ``dry_run`` are accepted for signature compatibility but unused.
        """
        start_time = time.time()

        try:
            # Load settings
            from web.config import SETTINGS_FILE
            if not SETTINGS_FILE.exists():
                return ActionResult(success=False, message="Settings file not found")

            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                settings = json.load(f)

            plex_url = settings.get("PLEX_URL", "").strip()
            plex_token = settings.get("PLEX_TOKEN", "").strip()
            if not plex_url or not plex_token:
                return ActionResult(success=False, message="Plex URL or token not configured")

            path_mappings = settings.get("path_mappings", [])

            # Phase 1: Connect to Plex
            if progress_callback:
                progress_callback(0, 0, "Connecting to Plex...")

            from plexapi.server import PlexServer
            plex = PlexServer(plex_url, plex_token, timeout=30)
            logger.info(f"Connected to Plex: {plex.friendlyName}")

            # Get library sections
            sections = [s for s in plex.library.sections() if s.type in ('movie', 'show')]
            if not sections:
                return ActionResult(success=False, message="No movie or TV show libraries found")

            # Phase 2: Scan libraries
            duplicates: List[PlexDuplicateItem] = []
            total_items_scanned = 0
            libraries_scanned = []

            for section in sections:
                if stop_check and stop_check():
                    return ActionResult(success=False, message="Scan cancelled")

                if progress_callback:
                    progress_callback(0, 0, f"Scanning: {section.title}...")

                libraries_scanned.append(section.title)

                if section.type == 'movie':
                    items_scanned = self._scan_movie_section(
                        section, duplicates, path_mappings,
                        stop_check, progress_callback
                    )
                else:
                    items_scanned = self._scan_show_section(
                        section, duplicates, path_mappings,
                        stop_check, progress_callback
                    )
                total_items_scanned += items_scanned

            if stop_check and stop_check():
                return ActionResult(success=False, message="Scan cancelled")

            # Phase 3: Classify orphans if arr configured
            arr_enabled = False
            tracked_files: Dict[str, str] = {}

            arr_instances = settings.get("arr_instances", [])
            for inst in arr_instances:
                if stop_check and stop_check():
                    break
                if not inst.get("enabled"):
                    continue
                inst_url = inst.get("url", "").strip()
                inst_key = inst.get("api_key", "").strip()
                if not inst_url or not inst_key:
                    continue

                inst_name = inst.get("name", inst.get("type", "?"))
                if progress_callback:
                    progress_callback(0, 0, f"Querying {inst_name}...")

                try:
                    if inst["type"] == "sonarr":
                        inst_tracked = self._get_sonarr_tracked(inst_url, inst_key, stop_check)
                    elif inst["type"] == "radarr":
                        inst_tracked = self._get_radarr_tracked(inst_url, inst_key)
                    else:
                        continue
                    logger.info(f"{inst_name}: {len(inst_tracked)} tracked files")
                    tracked_files.update(inst_tracked)
                except Exception as e:
                    logger.error(f"{inst_name} query failed: {e}")

            arr_enabled = bool(tracked_files)

            if tracked_files:
                self._classify_orphans(duplicates, tracked_files)

            # Compute stats
            orphan_count = sum(len(item.orphan_files) for item in duplicates)
            orphan_bytes = sum(item.orphan_bytes for item in duplicates)
            unresolved_count = sum(1 for item in duplicates if not item.is_resolved)

            duration = time.time() - start_time

            results = DuplicateScanResults(
                scanned_at=datetime.now().isoformat(),
                scan_duration_seconds=round(duration, 1),
                total_items=total_items_scanned,
                duplicate_count=len(duplicates),
                orphan_count=orphan_count,
                orphan_bytes=orphan_bytes,
                orphan_bytes_display=format_bytes(orphan_bytes),
                unresolved_count=unresolved_count,
                arr_enabled=arr_enabled,
                libraries_scanned=libraries_scanned,
                items=duplicates,
            )

            # Phase 4: Save results
            self.save_scan_results(results)

            msg = f"Found {len(duplicates)} items with duplicate files"
            if arr_enabled:
                msg += f" ({orphan_count} orphans, {format_bytes(orphan_bytes)})"

            return ActionResult(
                success=True,
                message=msg,
                affected_count=len(duplicates),
            )

        except Exception as e:
            logger.error(f"Duplicate scan failed: {e}", exc_info=True)
            return ActionResult(success=False, message=f"Scan failed: {str(e)[:200]}")

    # ------------------------------------------------------------------
    # Public: delete
    # ------------------------------------------------------------------

    def delete_files(
        self,
        paths: Optional[List[str]] = None,
        dry_run: bool = True,
        stop_check: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable] = None,
        bytes_progress_callback: Optional[Callable] = None,
        max_workers: int = 1,
        active_callback: Optional[Callable] = None,
    ) -> ActionResult:
        """Delete selected duplicate files by filesystem path.

        ``paths`` contains filesystem paths to delete.
        """
        if not paths:
            return ActionResult(success=False, message="No files specified")

        deleted = 0
        freed = 0
        errors = []
        affected_paths = []

        for fs_path in paths:
            if stop_check and stop_check():
                break

            if progress_callback:
                progress_callback(deleted, len(paths), f"Deleting: {os.path.basename(fs_path)}")

            if dry_run:
                if os.path.exists(fs_path):
                    deleted += 1
                    freed += os.path.getsize(fs_path)
                    affected_paths.append(fs_path)
                else:
                    errors.append(f"Not found: {fs_path}")
                continue

            try:
                if os.path.exists(fs_path):
                    file_size = os.path.getsize(fs_path)
                    os.remove(fs_path)
                    deleted += 1
                    freed += file_size
                    affected_paths.append(fs_path)
                    logger.info(f"Deleted duplicate: {fs_path} ({format_bytes(file_size)})")
                else:
                    errors.append(f"Not found: {fs_path}")
            except OSError as e:
                errors.append(f"Error deleting {os.path.basename(fs_path)}: {e}")

        action = "Would delete" if dry_run else "Deleted"
        msg = f"{action} {deleted} file(s), freeing {format_bytes(freed)}"

        if not dry_run and deleted > 0:
            # Update saved scan results to remove deleted files
            self._remove_deleted_from_results(affected_paths)

        return ActionResult(
            success=len(errors) == 0,
            message=msg,
            affected_count=deleted,
            errors=errors,
            affected_paths=affected_paths,
        )

    def delete_all_orphans(
        self,
        paths: Optional[List[str]] = None,
        dry_run: bool = True,
        stop_check: Optional[Callable[[], bool]] = None,
        progress_callback: Optional[Callable] = None,
        bytes_progress_callback: Optional[Callable] = None,
        max_workers: int = 1,
        active_callback: Optional[Callable] = None,
    ) -> ActionResult:
        """Delete all orphan files from the last scan results.

        ``paths`` is accepted for signature compatibility but unused (reads from scan results).
        """
        results = self.load_scan_results()
        if not results:
            return ActionResult(success=False, message="No scan results found. Run a scan first.")

        orphan_paths = []
        for item in results.items:
            orphan_paths.extend(item.orphan_files)

        if not orphan_paths:
            return ActionResult(success=True, message="No orphan files to delete", affected_count=0)

        return self.delete_files(
            paths=orphan_paths,
            dry_run=dry_run,
            stop_check=stop_check,
            progress_callback=progress_callback,
            bytes_progress_callback=bytes_progress_callback,
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load_scan_results(self) -> Optional[DuplicateScanResults]:
        """Load cached scan results from disk."""
        try:
            if not os.path.exists(SCAN_RESULTS_FILE):
                return None
            with open(SCAN_RESULTS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return _dict_to_results(data)
        except (json.JSONDecodeError, IOError, KeyError) as e:
            logger.warning(f"Failed to load duplicate scan results: {e}")
            return None

    def save_scan_results(self, results: DuplicateScanResults) -> None:
        """Save scan results to disk."""
        os.makedirs("data", exist_ok=True)
        save_json_atomically(SCAN_RESULTS_FILE, _results_to_dict(results), label="duplicate_scan")

    # ------------------------------------------------------------------
    # Private: Plex scanning
    # ------------------------------------------------------------------

    def _scan_movie_section(
        self,
        section,
        duplicates: List[PlexDuplicateItem],
        path_mappings: list,
        stop_check: Optional[Callable],
        progress_callback: Optional[Callable],
    ) -> int:
        """Scan a movie library section. Returns item count."""
        items_scanned = 0
        all_movies = section.all()
        total = len(all_movies)

        for i, movie in enumerate(all_movies):
            if stop_check and stop_check():
                break

            if progress_callback and (i % 50 == 0 or i == total - 1):
                progress_callback(i, total, f"Scanning: {section.title} ({i + 1}/{total})")

            files = []
            for media in movie.media:
                for part in media.parts:
                    plex_path = part.file
                    fs_path = self._plex_to_fs(plex_path, path_mappings)
                    files.append(DuplicateFileEntry(
                        file_path=plex_path,
                        fs_path=fs_path,
                        size=part.size or 0,
                        size_display=format_bytes(part.size or 0),
                        resolution=media.videoResolution or '?',
                        container=media.container or '?',
                        video_codec=media.videoCodec or '?',
                        bitrate=media.bitrate,
                    ))

            if len(files) > 1:
                title = f"{movie.title} ({movie.year})" if movie.year else movie.title
                duplicates.append(PlexDuplicateItem(
                    rating_key=str(movie.ratingKey),
                    title=title,
                    item_type='movie',
                    library=section.title,
                    files=files,
                ))

            items_scanned += 1

        return items_scanned

    def _scan_show_section(
        self,
        section,
        duplicates: List[PlexDuplicateItem],
        path_mappings: list,
        stop_check: Optional[Callable],
        progress_callback: Optional[Callable],
    ) -> int:
        """Scan a TV show library section. Returns item count."""
        items_scanned = 0
        all_shows = section.all()
        total_shows = len(all_shows)

        for si, show in enumerate(all_shows):
            if stop_check and stop_check():
                break

            if progress_callback and (si % 10 == 0 or si == total_shows - 1):
                progress_callback(si, total_shows, f"Scanning: {section.title} ({si + 1}/{total_shows} shows)")

            for episode in show.episodes():
                if stop_check and stop_check():
                    break

                files = []
                for media in episode.media:
                    for part in media.parts:
                        plex_path = part.file
                        fs_path = self._plex_to_fs(plex_path, path_mappings)
                        files.append(DuplicateFileEntry(
                            file_path=plex_path,
                            fs_path=fs_path,
                            size=part.size or 0,
                            size_display=format_bytes(part.size or 0),
                            resolution=media.videoResolution or '?',
                            container=media.container or '?',
                            video_codec=media.videoCodec or '?',
                            bitrate=media.bitrate,
                        ))

                if len(files) > 1:
                    show_title = episode.grandparentTitle or show.title
                    season = episode.parentIndex
                    ep_num = episode.index
                    if season is not None and ep_num is not None:
                        ep_title = f"{show_title} - S{season:02d}E{ep_num:02d} - {episode.title}"
                    else:
                        ep_title = f"{show_title} - {episode.title}"

                    duplicates.append(PlexDuplicateItem(
                        rating_key=str(episode.ratingKey),
                        title=ep_title,
                        item_type='episode',
                        library=section.title,
                        files=files,
                    ))

                items_scanned += 1

        return items_scanned

    # ------------------------------------------------------------------
    # Private: Path translation
    # ------------------------------------------------------------------

    def _plex_to_fs(self, plex_path: str, path_mappings: list) -> str:
        """Translate Plex container path to filesystem path using path_mappings.

        Uses the same prefix-match logic as cache_service._plex_to_real().
        """
        for mapping in path_mappings:
            if not mapping.get('enabled', True):
                continue
            plex_prefix = mapping.get('plex_path', '').rstrip('/')
            real_prefix = mapping.get('real_path', '').rstrip('/')
            if plex_prefix and plex_path.startswith(plex_prefix):
                return real_prefix + plex_path[len(plex_prefix):]
        # No mapping matched — return original path
        return plex_path

    # ------------------------------------------------------------------
    # Private: Arr integration
    # ------------------------------------------------------------------

    def _get_sonarr_tracked(
        self,
        sonarr_url: str,
        sonarr_key: str,
        stop_check: Optional[Callable] = None,
    ) -> Dict[str, str]:
        """Get all tracked episode file basenames from Sonarr."""
        import requests

        tracked = {}
        headers = {'X-Api-Key': sonarr_key}
        base_url = sonarr_url.rstrip('/')

        resp = requests.get(f'{base_url}/api/v3/series', headers=headers, timeout=30)
        resp.raise_for_status()
        series_list = resp.json()

        for series in series_list:
            if stop_check and stop_check():
                break

            series_id = series['id']
            try:
                resp = requests.get(
                    f'{base_url}/api/v3/episodefile',
                    params={'seriesId': series_id},
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                for ef in resp.json():
                    path = ef.get('path', '')
                    if path:
                        tracked[os.path.basename(path)] = path
            except Exception as e:
                logger.warning(f"Failed to get Sonarr files for series {series.get('title', '?')}: {e}")

        return tracked

    def _get_radarr_tracked(self, radarr_url: str, radarr_key: str) -> Dict[str, str]:
        """Get all tracked movie file basenames from Radarr."""
        import requests

        tracked = {}
        headers = {'X-Api-Key': radarr_key}
        base_url = radarr_url.rstrip('/')

        resp = requests.get(f'{base_url}/api/v3/movie', headers=headers, timeout=60)
        resp.raise_for_status()
        movies = resp.json()

        for movie in movies:
            movie_file = movie.get('movieFile')
            if movie_file:
                path = movie_file.get('path', '')
                if path:
                    tracked[os.path.basename(path)] = path

        return tracked

    # ------------------------------------------------------------------
    # Private: Orphan classification
    # ------------------------------------------------------------------

    def _classify_orphans(
        self,
        items: List[PlexDuplicateItem],
        tracked_files: Dict[str, str],
    ) -> None:
        """Classify files as keeper or orphan based on arr tracking.

        Mutates items in place. If exactly one file basename matches tracked dict,
        it's the keeper; rest are orphans. If 0 or 2+ match, unresolved.

        Uses case-insensitive basename matching as a fallback when exact match
        fails, since Sonarr/Radarr may use different casing than the filesystem.
        """
        # Build case-insensitive lookup for fallback matching
        tracked_lower = {k.lower(): k for k in tracked_files}

        for item in items:
            tracked_in_set = []
            untracked_in_set = []

            for f in item.files:
                basename = os.path.basename(f.fs_path)
                if basename in tracked_files:
                    tracked_in_set.append(f)
                elif basename.lower() in tracked_lower:
                    # Case-insensitive fallback match
                    tracked_in_set.append(f)
                else:
                    untracked_in_set.append(f)

            if len(tracked_in_set) == 1 and untracked_in_set:
                # Clear case: one tracked, rest are orphans
                keeper = tracked_in_set[0]
                keeper.is_keeper = True
                item.keeper_file = keeper.fs_path
                item.is_resolved = True

                for f in untracked_in_set:
                    f.is_keeper = False
                    item.orphan_files.append(f.fs_path)
                    item.orphan_bytes += f.size
            else:
                # Log unresolved items for debugging
                basenames = [os.path.basename(f.fs_path) for f in item.files]
                if len(tracked_in_set) == 0:
                    logger.debug(
                        f"Duplicate unresolved (no arr match): {item.title} — "
                        f"files: {basenames}"
                    )
                elif len(tracked_in_set) >= 2:
                    logger.debug(
                        f"Duplicate unresolved (multiple arr matches): {item.title} — "
                        f"files: {basenames}"
                    )

    # ------------------------------------------------------------------
    # Private: Post-delete cleanup
    # ------------------------------------------------------------------

    def _remove_deleted_from_results(self, deleted_paths: List[str]) -> None:
        """Remove deleted files from saved scan results."""
        results = self.load_scan_results()
        if not results:
            return

        deleted_set = set(deleted_paths)
        updated_items = []

        for item in results.items:
            # Remove deleted files from the item
            remaining_files = [f for f in item.files if f.fs_path not in deleted_set]

            if len(remaining_files) > 1:
                # Still a duplicate — update the item
                item.files = remaining_files
                item.orphan_files = [p for p in item.orphan_files if p not in deleted_set]
                item.orphan_bytes = sum(
                    f.size for f in item.files if f.is_keeper is False
                )
                updated_items.append(item)
            # else: no longer a duplicate, drop it

        results.items = updated_items
        results.duplicate_count = len(updated_items)
        results.orphan_count = sum(len(i.orphan_files) for i in updated_items)
        results.orphan_bytes = sum(i.orphan_bytes for i in updated_items)
        results.orphan_bytes_display = format_bytes(results.orphan_bytes)
        results.unresolved_count = sum(1 for i in updated_items if not i.is_resolved)

        self.save_scan_results(results)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------

_service: Optional[DuplicateService] = None
_service_lock = threading.Lock()


def get_duplicate_service() -> DuplicateService:
    """Get or create the duplicate service singleton."""
    global _service
    if _service is None:
        with _service_lock:
            if _service is None:
                _service = DuplicateService()
    return _service
