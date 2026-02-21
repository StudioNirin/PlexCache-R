"""Import service for migrating CLI data to Docker version"""

import json
import shutil
import threading
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

from web.config import CONFIG_DIR, DATA_DIR


@dataclass
class ImportSummary:
    """Summary of detected import files"""
    has_settings: bool = False
    has_data: bool = False
    has_exclude_file: bool = False
    timestamps_count: int = 0
    ondeck_count: int = 0
    watchlist_count: int = 0
    user_tokens_count: int = 0
    exclude_entries_count: int = 0
    detected_cache_prefix: Optional[str] = None
    errors: list = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []

    @property
    def has_import_files(self) -> bool:
        return self.has_settings or self.has_data or self.has_exclude_file


class ImportService:
    """Service for importing CLI data into Docker version"""

    def __init__(self):
        self.import_dir = CONFIG_DIR / "import"
        self.import_data_dir = self.import_dir / "data"
        self.completed_dir = self.import_dir / "completed"  # Keep inside import folder

    def detect_import_files(self) -> ImportSummary:
        """Detect what import files are available"""
        summary = ImportSummary()

        if not self.import_dir.exists():
            return summary

        # Check for settings file
        settings_file = self.import_dir / "plexcache_settings.json"
        if settings_file.exists() and settings_file.stat().st_size > 0:
            summary.has_settings = True
            # Try to detect cache prefix from settings
            try:
                with open(settings_file, 'r') as f:
                    settings = json.load(f)
                    cache_dir = settings.get("cache_dir", "")
                    if cache_dir and cache_dir != "/mnt/cache":
                        summary.detected_cache_prefix = cache_dir.rstrip('/') + '/'
            except Exception as e:
                summary.errors.append(f"Error reading settings: {e}")
                summary.has_settings = False  # Invalid JSON doesn't count

        # Check for data files (only if data folder exists AND has actual JSON files)
        if self.import_data_dir.exists():
            # Count timestamps
            timestamps_file = self.import_data_dir / "timestamps.json"
            if timestamps_file.exists() and timestamps_file.stat().st_size > 0:
                try:
                    with open(timestamps_file, 'r') as f:
                        data = json.load(f)
                        summary.timestamps_count = len(data)
                        # Try to detect cache prefix from timestamps
                        if not summary.detected_cache_prefix and data:
                            first_path = next(iter(data.keys()))
                            # Common CLI cache paths
                            for prefix in ['/mnt/cache_downloads/', '/mnt/cache/', '/mnt/user/']:
                                if first_path.startswith(prefix):
                                    # Only set if it's not the Docker default
                                    if prefix != '/mnt/cache/':
                                        summary.detected_cache_prefix = prefix
                                    break
                except Exception as e:
                    summary.errors.append(f"Error reading timestamps: {e}")

            # Count OnDeck items
            ondeck_file = self.import_data_dir / "ondeck_tracker.json"
            if ondeck_file.exists() and ondeck_file.stat().st_size > 0:
                try:
                    with open(ondeck_file, 'r') as f:
                        data = json.load(f)
                        summary.ondeck_count = len(data)
                except Exception as e:
                    summary.errors.append(f"Error reading ondeck: {e}")

            # Count Watchlist items
            watchlist_file = self.import_data_dir / "watchlist_tracker.json"
            if watchlist_file.exists() and watchlist_file.stat().st_size > 0:
                try:
                    with open(watchlist_file, 'r') as f:
                        data = json.load(f)
                        summary.watchlist_count = len(data)
                except Exception as e:
                    summary.errors.append(f"Error reading watchlist: {e}")

            # Count user tokens
            tokens_file = self.import_data_dir / "user_tokens.json"
            if tokens_file.exists() and tokens_file.stat().st_size > 0:
                try:
                    with open(tokens_file, 'r') as f:
                        data = json.load(f)
                        summary.user_tokens_count = len(data)
                except Exception as e:
                    summary.errors.append(f"Error reading user tokens: {e}")

            # Only mark has_data if we found actual data files with content
            summary.has_data = (
                summary.timestamps_count > 0 or
                summary.ondeck_count > 0 or
                summary.watchlist_count > 0 or
                summary.user_tokens_count > 0
            )

        # Check for exclude file (at root level of import folder)
        # Support both old and new filenames (check newest first)
        exclude_file = self.import_dir / "unraid_mover_exclusions.txt"
        if not exclude_file.exists():
            exclude_file = self.import_dir / "plexcache_cached_files.txt"
        if not exclude_file.exists():
            exclude_file = self.import_dir / "plexcache_mover_files_to_exclude.txt"
        if exclude_file.exists() and exclude_file.stat().st_size > 0:
            try:
                with open(exclude_file, 'r') as f:
                    lines = [line.strip() for line in f.readlines() if line.strip()]
                    summary.exclude_entries_count = len(lines)
                    summary.has_exclude_file = summary.exclude_entries_count > 0
                    # Try to detect cache prefix from exclude file paths
                    if not summary.detected_cache_prefix and lines:
                        first_path = lines[0]
                        for prefix in ['/mnt/cache_downloads/', '/mnt/user/']:
                            if first_path.startswith(prefix):
                                summary.detected_cache_prefix = prefix
                                break
            except Exception as e:
                summary.errors.append(f"Error reading exclude file: {e}")

        return summary

    def convert_path(self, path: str, cli_prefix: str, docker_prefix: str = "/mnt/cache/") -> str:
        """Convert a single path from CLI format to Docker format"""
        if path.startswith(cli_prefix):
            return docker_prefix + path[len(cli_prefix):]
        return path

    def convert_timestamps(self, data: Dict, cli_prefix: str, docker_prefix: str = "/mnt/cache/") -> Dict:
        """Convert all paths in timestamps data"""
        converted = {}
        for path, info in data.items():
            new_path = self.convert_path(path, cli_prefix, docker_prefix)
            converted[new_path] = info
        return converted

    def convert_settings(self, settings: Dict, cli_prefix: str, docker_prefix: str = "/mnt/cache/") -> Dict:
        """Convert cache paths in settings"""
        converted = settings.copy()

        # Convert cache_dir
        if "cache_dir" in converted:
            cache_dir = converted["cache_dir"]
            if cache_dir and cache_dir.startswith(cli_prefix.rstrip('/')):
                converted["cache_dir"] = docker_prefix.rstrip('/')

        # Convert path_mappings cache_path entries
        if "path_mappings" in converted:
            new_mappings = []
            for mapping in converted["path_mappings"]:
                new_mapping = mapping.copy()
                if "cache_path" in new_mapping and new_mapping["cache_path"]:
                    new_mapping["cache_path"] = self.convert_path(
                        new_mapping["cache_path"], cli_prefix, docker_prefix
                    )
                new_mappings.append(new_mapping)
            converted["path_mappings"] = new_mappings

        return converted

    def perform_import(
        self,
        cli_cache_prefix: str,
        docker_cache_prefix: str = "/mnt/cache/",
        import_settings: bool = True,
        import_data: bool = True
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Perform the import operation.

        Returns:
            Tuple of (success, message, imported_settings)
        """
        results = {
            "settings_imported": False,
            "timestamps_imported": 0,
            "ondeck_imported": 0,
            "watchlist_imported": 0,
            "tokens_imported": 0,
            "errors": []
        }

        # Ensure cli_cache_prefix has trailing slash
        if cli_cache_prefix and not cli_cache_prefix.endswith('/'):
            cli_cache_prefix += '/'
        if not docker_cache_prefix.endswith('/'):
            docker_cache_prefix += '/'

        imported_settings = {}

        # Import settings
        if import_settings:
            settings_file = self.import_dir / "plexcache_settings.json"
            if settings_file.exists():
                try:
                    with open(settings_file, 'r') as f:
                        cli_settings = json.load(f)

                    # Convert paths in settings
                    imported_settings = self.convert_settings(
                        cli_settings, cli_cache_prefix, docker_cache_prefix
                    )

                    # Write to config location
                    target_settings = CONFIG_DIR / "plexcache_settings.json"
                    with open(target_settings, 'w') as f:
                        json.dump(imported_settings, f, indent=2)

                    results["settings_imported"] = True
                except Exception as e:
                    results["errors"].append(f"Settings import failed: {e}")

        # Import data files
        if import_data and self.import_data_dir.exists():
            # Ensure target data dir exists
            DATA_DIR.mkdir(parents=True, exist_ok=True)

            # Import timestamps (with conversion)
            timestamps_file = self.import_data_dir / "timestamps.json"
            if timestamps_file.exists():
                try:
                    with open(timestamps_file, 'r') as f:
                        cli_timestamps = json.load(f)

                    # Convert paths
                    converted = self.convert_timestamps(
                        cli_timestamps, cli_cache_prefix, docker_cache_prefix
                    )

                    # Write to data dir
                    target_file = DATA_DIR / "timestamps.json"
                    with open(target_file, 'w') as f:
                        json.dump(converted, f, indent=2)

                    results["timestamps_imported"] = len(converted)
                except Exception as e:
                    results["errors"].append(f"Timestamps import failed: {e}")

            # Import ondeck_tracker (no conversion needed)
            ondeck_file = self.import_data_dir / "ondeck_tracker.json"
            if ondeck_file.exists():
                try:
                    target_file = DATA_DIR / "ondeck_tracker.json"
                    shutil.copy2(ondeck_file, target_file)
                    with open(ondeck_file, 'r') as f:
                        results["ondeck_imported"] = len(json.load(f))
                except Exception as e:
                    results["errors"].append(f"OnDeck import failed: {e}")

            # Import watchlist_tracker (no conversion needed)
            watchlist_file = self.import_data_dir / "watchlist_tracker.json"
            if watchlist_file.exists():
                try:
                    target_file = DATA_DIR / "watchlist_tracker.json"
                    shutil.copy2(watchlist_file, target_file)
                    with open(watchlist_file, 'r') as f:
                        results["watchlist_imported"] = len(json.load(f))
                except Exception as e:
                    results["errors"].append(f"Watchlist import failed: {e}")

            # Import user_tokens (no conversion needed)
            tokens_file = self.import_data_dir / "user_tokens.json"
            if tokens_file.exists():
                try:
                    target_file = DATA_DIR / "user_tokens.json"
                    shutil.copy2(tokens_file, target_file)
                    with open(tokens_file, 'r') as f:
                        results["tokens_imported"] = len(json.load(f))
                except Exception as e:
                    results["errors"].append(f"User tokens import failed: {e}")

            # Import rss_cache (no conversion needed)
            rss_file = self.import_data_dir / "rss_cache.json"
            if rss_file.exists():
                try:
                    target_file = DATA_DIR / "rss_cache.json"
                    shutil.copy2(rss_file, target_file)
                except Exception as e:
                    results["errors"].append(f"RSS cache import failed: {e}")

        # Import exclude file (with path conversion) - at root level, not in data folder
        # Support both old and new filenames
        exclude_file = self.import_dir / "unraid_mover_exclusions.txt"
        if not exclude_file.exists():
            exclude_file = self.import_dir / "plexcache_mover_files_to_exclude.txt"
        if exclude_file.exists():
            try:
                with open(exclude_file, 'r') as f:
                    lines = f.readlines()

                # Convert paths in exclude file
                converted_lines = []
                for line in lines:
                    path = line.strip()
                    if path:
                        converted_path = self.convert_path(path, cli_cache_prefix, docker_cache_prefix)
                        converted_lines.append(converted_path + '\n')

                # Write to config location
                target_exclude = CONFIG_DIR / "plexcache_cached_files.txt"
                with open(target_exclude, 'w') as f:
                    f.writelines(converted_lines)

                results["exclude_entries_imported"] = len(converted_lines)
            except Exception as e:
                results["errors"].append(f"Exclude file import failed: {e}")

        # Move import files to completed folder
        if results["settings_imported"] or results["timestamps_imported"] > 0 or results.get("exclude_entries_imported", 0) > 0:
            try:
                self.completed_dir.mkdir(parents=True, exist_ok=True)

                # Move settings file
                settings_file = self.import_dir / "plexcache_settings.json"
                if settings_file.exists():
                    shutil.move(str(settings_file), str(self.completed_dir / "plexcache_settings.json"))

                # Move exclude file (check all filename variants)
                exclude_file = self.import_dir / "unraid_mover_exclusions.txt"
                if not exclude_file.exists():
                    exclude_file = self.import_dir / "plexcache_cached_files.txt"
                if not exclude_file.exists():
                    exclude_file = self.import_dir / "plexcache_mover_files_to_exclude.txt"
                if exclude_file.exists():
                    shutil.move(str(exclude_file), str(self.completed_dir / exclude_file.name))

                # Move data folder
                if self.import_data_dir.exists():
                    target_data = self.completed_dir / "data"
                    if target_data.exists():
                        shutil.rmtree(target_data)
                    shutil.move(str(self.import_data_dir), str(target_data))
            except Exception as e:
                results["errors"].append(f"Failed to move import files to completed: {e}")

        # Determine success
        success = results["settings_imported"] or results["timestamps_imported"] > 0

        if success and not results["errors"]:
            message = "Import completed successfully"
        elif success and results["errors"]:
            message = "Import completed with some errors"
        else:
            message = "Import failed"

        return success, message, imported_settings


# Singleton instance
_import_service = None
_import_service_lock = threading.Lock()


def get_import_service() -> ImportService:
    """Get the singleton import service instance"""
    global _import_service
    if _import_service is None:
        with _import_service_lock:
            if _import_service is None:
                _import_service = ImportService()
    return _import_service
