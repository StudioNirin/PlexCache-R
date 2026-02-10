#!/usr/bin/env python3
"""
Audit script to compare cache files, exclude list, and timestamps.
Run from tools/ directory: python3 tools/audit_cache.py
Or from project root: python3 tools/audit_cache.py
"""

import os
import json
import sys
import shutil
import subprocess

# Get script directory and resolve project root
# If we're in tools/, go up one level to project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(SCRIPT_DIR) == 'tools':
    PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
else:
    PROJECT_ROOT = SCRIPT_DIR

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SETTINGS_FILE = os.path.join(PROJECT_ROOT, "plexcache_settings.json")

# Default paths (will be overwritten if settings file exists)
CACHE_DIRS = []
ARRAY_DIRS = []
EXCLUDE_FILE = os.path.join(PROJECT_ROOT, "plexcache_cached_files.txt")
TIMESTAMPS_FILE = os.path.join(DATA_DIR, "timestamps.json")

# Legacy file locations (for migration)
LEGACY_TIMESTAMPS_FILE = os.path.join(PROJECT_ROOT, "plexcache_timestamps.json")


def load_settings():
    """Load paths from plexcache_settings.json."""
    global CACHE_DIRS, ARRAY_DIRS, EXCLUDE_FILE, TIMESTAMPS_FILE, EXCLUDED_FOLDERS

    if not os.path.exists(SETTINGS_FILE):
        print(f"⚠️  Settings file not found: {SETTINGS_FILE}")
        print("   Run this script from the PlexCache-R project root:")
        print("   python3 tools/audit_cache.py")
        sys.exit(1)

    # Check for legacy timestamps file location
    if not os.path.exists(TIMESTAMPS_FILE) and os.path.exists(LEGACY_TIMESTAMPS_FILE):
        TIMESTAMPS_FILE = LEGACY_TIMESTAMPS_FILE
        print(f"Note: Using legacy timestamps file location: {TIMESTAMPS_FILE}")

    try:
        with open(SETTINGS_FILE, 'r') as f:
            settings = json.load(f)

        # Check for multi-path mode (path_mappings)
        path_mappings = settings.get('path_mappings', [])

        if path_mappings:
            # Multi-path mode: use path_mappings
            for mapping in path_mappings:
                if not mapping.get('enabled', True):
                    continue

                cache_path = mapping.get('cache_path', '').rstrip('/') if mapping.get('cache_path') else ''
                real_path = mapping.get('real_path', '').rstrip('/')

                # Only include cacheable mappings with valid paths
                if mapping.get('cacheable', True) and cache_path and real_path:
                    # Convert real_path (/mnt/user/) to array path (/mnt/user0/)
                    array_path = real_path.replace('/mnt/user/', '/mnt/user0/')
                    CACHE_DIRS.append(cache_path)
                    ARRAY_DIRS.append(array_path)

            if not CACHE_DIRS:
                print("⚠️  No cacheable path mappings found with valid paths")
                sys.exit(1)
        else:
            # Legacy single-path mode
            cache_dir = settings.get('cache_dir', '').rstrip('/')
            real_source = settings.get('real_source', '').rstrip('/')
            nas_library_folders = settings.get('nas_library_folders', [])

            if not cache_dir or not real_source or not nas_library_folders:
                print("⚠️  Missing required settings: cache_dir, real_source, or nas_library_folders")
                print("   (Or use path_mappings for multi-path mode)")
                sys.exit(1)

            # Convert real_source (/mnt/user/) to array path (/mnt/user0/)
            # Unraid: /mnt/user/ is merged view, /mnt/user0/ is array only
            array_source = real_source.replace('/mnt/user/', '/mnt/user0/')

            # Build cache and array directory paths from nas_library_folders
            for folder in nas_library_folders:
                folder = folder.strip('/')
                CACHE_DIRS.append(os.path.join(cache_dir, folder))
                ARRAY_DIRS.append(os.path.join(array_source, folder))

        # Note: EXCLUDE_FILE and TIMESTAMPS_FILE are already set correctly
        # at module level (PROJECT_ROOT and DATA_DIR respectively)

        # Load excluded folders for directory scanning
        EXCLUDED_FOLDERS = settings.get('excluded_folders', [])

        print(f"Loaded settings from: {SETTINGS_FILE}")
        print(f"Cache directories: {CACHE_DIRS}")
        print(f"Array directories: {ARRAY_DIRS}")
        if EXCLUDED_FOLDERS:
            print(f"Excluded folders: {EXCLUDED_FOLDERS}")

    except Exception as e:
        print(f"❌ Error loading settings: {e}")
        sys.exit(1)


# Excluded folders loaded from settings
EXCLUDED_FOLDERS = []


def _should_skip_directory(dirname):
    """Check if directory should be skipped during scanning."""
    if dirname.startswith('.'):
        return True
    return dirname in EXCLUDED_FOLDERS


# Load settings on import
load_settings()

def get_cache_files():
    """Get all media files currently on cache."""
    cache_files = set()
    # Video extensions
    video_ext = ('.mkv', '.mp4', '.avi', '.m4v', '.mov', '.wmv', '.ts')
    # Subtitle extensions
    subtitle_ext = ('.srt', '.sub', '.idx', '.ass', '.ssa', '.vtt', '.smi')
    extensions = video_ext + subtitle_ext

    for cache_dir in CACHE_DIRS:
        if os.path.exists(cache_dir):
            for root, dirs, files in os.walk(cache_dir):
                dirs[:] = [d for d in dirs if not _should_skip_directory(d)]
                for f in files:
                    if f.lower().endswith(extensions):
                        cache_files.add(os.path.join(root, f))

    return cache_files

def get_exclude_files():
    """Get all files in exclude list."""
    exclude_files = set()
    if os.path.exists(EXCLUDE_FILE):
        with open(EXCLUDE_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    exclude_files.add(line)
    return exclude_files

def get_timestamp_files():
    """Get all files in timestamps."""
    timestamp_files = set()
    if os.path.exists(TIMESTAMPS_FILE):
        with open(TIMESTAMPS_FILE, 'r') as f:
            data = json.load(f)
            timestamp_files = set(data.keys())
    return timestamp_files

def get_orphaned_plexcached_files():
    """Find .plexcached files on array that have no corresponding cache file.

    These are backup files where:
    - The .plexcached backup exists on array
    - No corresponding file exists on cache
    - No original (restored) file exists on array

    This can happen if cache was cleared without proper restoration.
    """
    orphaned = []
    cache_files = get_cache_files()

    for array_dir in ARRAY_DIRS:
        if not os.path.exists(array_dir):
            continue
        for root, dirs, files in os.walk(array_dir):
            dirs[:] = [d for d in dirs if not _should_skip_directory(d)]
            for f in files:
                if f.endswith('.plexcached'):
                    plexcached_path = os.path.join(root, f)
                    original_name = f[:-11]  # Remove .plexcached suffix
                    original_array_path = os.path.join(root, original_name)

                    # Find corresponding cache path
                    for i, arr_dir in enumerate(ARRAY_DIRS):
                        if plexcached_path.startswith(arr_dir):
                            cache_path = os.path.join(
                                CACHE_DIRS[i],
                                os.path.relpath(original_array_path, arr_dir)
                            )
                            break
                    else:
                        cache_path = None

                    # Check if orphaned: no cache copy AND no restored original
                    if cache_path and cache_path not in cache_files:
                        if not os.path.exists(original_array_path):
                            orphaned.append((plexcached_path, original_array_path))

    return orphaned


def cache_to_array_path(cache_file):
    """Convert a cache file path to its corresponding array path."""
    for i, cache_dir in enumerate(CACHE_DIRS):
        if cache_file.startswith(cache_dir):
            return cache_file.replace(cache_dir, ARRAY_DIRS[i], 1)
    return None


def check_plexcached_backup(cache_file):
    """Check if a .plexcached backup exists on array for a cache file."""
    array_file = cache_to_array_path(cache_file)
    if not array_file:
        return False, None

    plexcached_file = array_file + ".plexcached"
    return os.path.exists(plexcached_file), plexcached_file


def check_array_duplicate(cache_file):
    """Check if the same file already exists on the array (duplicate)."""
    array_file = cache_to_array_path(cache_file)
    if not array_file:
        return False, None

    return os.path.exists(array_file), array_file

def cleanup_duplicates(dry_run=True):
    """Remove cache files that already exist on array."""
    cache_files = get_cache_files()
    exclude_files = get_exclude_files()

    # Only check files NOT in exclude list (orphaned files)
    orphaned = cache_files - exclude_files

    duplicates = []
    for f in orphaned:
        is_dup, array_path = check_array_duplicate(f)
        if is_dup:
            duplicates.append((f, array_path))

    if not duplicates:
        print("No duplicates found.")
        return

    print(f"\nFound {len(duplicates)} duplicate files on cache:")
    for cache_path, array_path in duplicates:
        print(f"  - {os.path.basename(cache_path)}")

    if dry_run:
        print("\n[DRY RUN] Would delete the above cache files.")
        print("Run with --cleanup to actually delete them.")
    else:
        print("\nDeleting cache duplicates...")
        for cache_path, array_path in duplicates:
            try:
                os.remove(cache_path)
                print(f"  Deleted: {os.path.basename(cache_path)}")
            except Exception as e:
                print(f"  ERROR deleting {cache_path}: {e}")

        # Clean up empty directories
        cleanup_empty_directories()


def cleanup_empty_directories():
    """Remove empty directories from cache paths."""
    print("\nCleaning up empty directories...")
    for cache_dir in CACHE_DIRS:
        if os.path.exists(cache_dir):
            for root, dirs, files in os.walk(cache_dir, topdown=False):
                for d in dirs:
                    if _should_skip_directory(d):
                        continue
                    dir_path = os.path.join(root, d)
                    try:
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                            print(f"  Removed empty dir: {dir_path}")
                    except Exception as e:
                        pass


def get_orphaned_files_by_backup_status():
    """Get orphaned cache files categorized by backup status."""
    cache_files = get_cache_files()
    exclude_files = get_exclude_files()
    orphaned = cache_files - exclude_files

    has_backup = []
    no_backup = []

    for f in orphaned:
        exists, backup_path = check_plexcached_backup(f)
        if exists:
            has_backup.append((f, backup_path))
        else:
            # Check if file already exists on array (duplicate)
            is_dup, array_path = check_array_duplicate(f)
            if is_dup:
                has_backup.append((f, array_path))  # Treat duplicates like backups
            else:
                no_backup.append(f)

    return has_backup, no_backup


def fix_with_backup(dry_run=True):
    """
    Fix files that have .plexcached backup on array.
    - Delete the cache copy
    - Rename .plexcached back to original filename
    """
    has_backup, _ = get_orphaned_files_by_backup_status()

    if not has_backup:
        print("No files found with .plexcached backup to fix.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Fixing {len(has_backup)} files with backup:")

    for cache_path, backup_or_array_path in has_backup:
        filename = os.path.basename(cache_path)

        # Determine if this is a .plexcached backup or a duplicate
        if backup_or_array_path.endswith('.plexcached'):
            # It's a .plexcached backup - need to rename it
            original_array_path = backup_or_array_path[:-11]  # Remove .plexcached suffix
            action = "restore backup"
        else:
            # It's a duplicate - array already has the file
            original_array_path = backup_or_array_path
            action = "remove duplicate"

        if dry_run:
            print(f"  Would {action}: {filename}")
        else:
            try:
                # If it was a .plexcached backup, rename it back FIRST (safer order)
                if backup_or_array_path.endswith('.plexcached'):
                    os.rename(backup_or_array_path, original_array_path)
                    print(f"  Restored backup: {os.path.basename(original_array_path)}")

                # Delete cache copy only after array file is restored
                os.remove(cache_path)
                print(f"  Deleted cache: {filename}")

            except Exception as e:
                print(f"  ERROR fixing {filename}: {e}")

    if not dry_run:
        cleanup_empty_directories()
    else:
        print(f"\n[DRY RUN] Run with --fix-with-backup --execute to apply changes.")


def add_to_exclude(dry_run=True):
    """
    Add orphaned cache files to the exclude list.
    This protects them from being moved by Unraid mover.
    """
    cache_files = get_cache_files()
    exclude_files = get_exclude_files()
    orphaned = cache_files - exclude_files

    if not orphaned:
        print("No orphaned files to add to exclude list.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Adding {len(orphaned)} files to exclude list:")

    for f in sorted(orphaned)[:20]:
        print(f"  + {os.path.basename(f)}")
    if len(orphaned) > 20:
        print(f"  ... and {len(orphaned) - 20} more")

    if dry_run:
        print(f"\n[DRY RUN] Run with --add-to-exclude --execute to apply changes.")
    else:
        try:
            with open(EXCLUDE_FILE, 'a') as f:
                for filepath in sorted(orphaned):
                    f.write(filepath + '\n')
            print(f"\n✅ Added {len(orphaned)} files to exclude list.")
        except Exception as e:
            print(f"\nERROR writing to exclude file: {e}")


def sync_to_array(dry_run=True):
    """
    Sync orphaned cache files (without backup) to array.
    Uses rsync to copy files from cache to array, then removes cache copy.
    """
    _, no_backup = get_orphaned_files_by_backup_status()

    if not no_backup:
        print("No files without backup to sync.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Syncing {len(no_backup)} files to array:")

    for cache_path in sorted(no_backup):
        filename = os.path.basename(cache_path)

        # Determine array destination
        array_path = cache_to_array_path(cache_path)
        if not array_path:
            print(f"  SKIP (unknown path): {filename}")
            continue

        array_dir = os.path.dirname(array_path)

        if dry_run:
            print(f"  Would sync: {filename}")
            print(f"    -> {array_path}")
        else:
            try:
                # Create destination directory if needed
                os.makedirs(array_dir, exist_ok=True)

                # Use rsync to copy file (preserves permissions, shows progress)
                cmd = ['rsync', '-avh', '--progress', cache_path, array_path]
                print(f"\n  Syncing: {filename}")
                result = subprocess.run(cmd, capture_output=True, text=True)

                if result.returncode == 0:
                    # Verify file was copied successfully
                    if os.path.exists(array_path):
                        cache_size = os.path.getsize(cache_path)
                        array_size = os.path.getsize(array_path)

                        if cache_size == array_size:
                            # Remove cache copy
                            os.remove(cache_path)
                            print(f"  ✅ Synced and removed: {filename}")
                        else:
                            print(f"  ⚠️  Size mismatch, keeping cache copy: {filename}")
                    else:
                        print(f"  ❌ Array file not found after sync: {filename}")
                else:
                    print(f"  ❌ rsync failed: {result.stderr}")

            except Exception as e:
                print(f"  ERROR syncing {filename}: {e}")

    if not dry_run:
        cleanup_empty_directories()
    else:
        print(f"\n[DRY RUN] Run with --sync-to-array --execute to apply changes.")


def clean_exclude(dry_run=True):
    """
    Remove stale entries from exclude list.
    These are files listed in exclude but no longer on cache.
    """
    cache_files = get_cache_files()
    exclude_files = get_exclude_files()
    stale_entries = exclude_files - cache_files

    if not stale_entries:
        print("No stale entries in exclude list.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning {len(stale_entries)} stale entries from exclude list:")

    for f in sorted(stale_entries)[:20]:
        print(f"  - {os.path.basename(f)}")
    if len(stale_entries) > 20:
        print(f"  ... and {len(stale_entries) - 20} more")

    if dry_run:
        print(f"\n[DRY RUN] Run with --clean-exclude --execute to apply changes.")
    else:
        try:
            # Keep only entries that still exist on cache
            valid_entries = exclude_files & cache_files
            with open(EXCLUDE_FILE, 'w') as f:
                for filepath in sorted(valid_entries):
                    f.write(filepath + '\n')
            print(f"\n✅ Removed {len(stale_entries)} stale entries from exclude list.")
            print(f"   Exclude list now has {len(valid_entries)} entries.")
        except Exception as e:
            print(f"\nERROR writing to exclude file: {e}")


def clean_timestamps(dry_run=True):
    """
    Remove stale entries from timestamps file.
    These are files listed in timestamps but no longer on cache.
    """
    cache_files = get_cache_files()
    timestamp_files = get_timestamp_files()
    stale_entries = timestamp_files - cache_files

    if not stale_entries:
        print("No stale entries in timestamps file.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Cleaning {len(stale_entries)} stale entries from timestamps file:")

    for f in sorted(stale_entries)[:20]:
        print(f"  - {os.path.basename(f)}")
    if len(stale_entries) > 20:
        print(f"  ... and {len(stale_entries) - 20} more")

    if dry_run:
        print(f"\n[DRY RUN] Run with --clean-timestamps --execute to apply changes.")
    else:
        try:
            # Load the full timestamps data
            with open(TIMESTAMPS_FILE, 'r') as f:
                timestamps_data = json.load(f)

            # Remove stale entries
            for stale_path in stale_entries:
                if stale_path in timestamps_data:
                    del timestamps_data[stale_path]

            # Write back
            with open(TIMESTAMPS_FILE, 'w') as f:
                json.dump(timestamps_data, f, indent=2)

            print(f"\n✅ Removed {len(stale_entries)} stale entries from timestamps file.")
            print(f"   Timestamps file now has {len(timestamps_data)} entries.")
        except Exception as e:
            print(f"\nERROR writing to timestamps file: {e}")


def restore_plexcached(dry_run=True):
    """
    Restore orphaned .plexcached files on array to their original names.
    These are backup files where the cache copy was deleted without restoration.
    """
    orphaned = get_orphaned_plexcached_files()

    if not orphaned:
        print("No orphaned .plexcached files found on array.")
        return

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Restoring {len(orphaned)} orphaned .plexcached files:")

    for plexcached_path, original_path in sorted(orphaned):
        filename = os.path.basename(original_path)

        if dry_run:
            print(f"  Would restore: {os.path.basename(plexcached_path)}")
            print(f"            to: {filename}")
        else:
            try:
                os.rename(plexcached_path, original_path)
                print(f"  Restored: {filename}")
            except Exception as e:
                print(f"  ERROR restoring {filename}: {e}")

    if dry_run:
        print(f"\n[DRY RUN] Run with --restore-plexcached --execute to apply changes.")
    else:
        print(f"\n✅ Restored {len(orphaned)} files to their original names.")


def main():
    print("=" * 80)
    print("PLEXCACHE AUDIT")
    print("=" * 80)

    # Get all sets
    cache_files = get_cache_files()
    exclude_files = get_exclude_files()
    timestamp_files = get_timestamp_files()

    print(f"\nFiles on cache:        {len(cache_files)}")
    print(f"Files in exclude list: {len(exclude_files)}")
    print(f"Files in timestamps:   {len(timestamp_files)}")

    # Find discrepancies
    on_cache_not_in_exclude = cache_files - exclude_files
    in_exclude_not_on_cache = exclude_files - cache_files
    on_cache_not_in_timestamps = cache_files - timestamp_files
    in_timestamps_not_on_cache = timestamp_files - cache_files

    print("\n" + "=" * 80)
    print("DISCREPANCIES")
    print("=" * 80)

    # On cache but not in exclude (PROBLEM - mover will move these!)
    print(f"\n🔴 ON CACHE but NOT in exclude list ({len(on_cache_not_in_exclude)}):")
    print("   (These files will be moved by Unraid mover!)")

    # Check which have .plexcached backups
    has_backup = []
    no_backup = []
    for f in on_cache_not_in_exclude:
        exists, backup_path = check_plexcached_backup(f)
        if exists:
            has_backup.append(f)
        else:
            no_backup.append(f)

    if has_backup:
        print(f"\n   ✅ WITH .plexcached backup ({len(has_backup)}) - safe to delete cache copy:")
        for f in sorted(has_backup)[:10]:
            print(f"      - {os.path.basename(f)}")
        if len(has_backup) > 10:
            print(f"      ... and {len(has_backup) - 10} more")

    if no_backup:
        print(f"\n   ⚠️  NO .plexcached backup ({len(no_backup)}) - need to sync to array:")
        print("      (These are likely new downloads from Radarr/Sonarr that went directly to cache)")
        for f in sorted(no_backup)[:10]:
            print(f"      - {os.path.basename(f)}")
        if len(no_backup) > 10:
            print(f"      ... and {len(no_backup) - 10} more")

    if not on_cache_not_in_exclude:
        print("   None - all good!")

    # In exclude but not on cache (stale entries)
    print(f"\n🟡 In exclude list but NOT on cache ({len(in_exclude_not_on_cache)}):")
    print("   (Stale entries - files were moved/deleted)")
    if in_exclude_not_on_cache:
        for f in sorted(in_exclude_not_on_cache)[:20]:
            print(f"   - {os.path.basename(f)}")
        if len(in_exclude_not_on_cache) > 20:
            print(f"   ... and {len(in_exclude_not_on_cache) - 20} more")
    else:
        print("   None - all good!")

    # On cache but not in timestamps (older files, no retention tracking)
    print(f"\n🟡 On cache but NOT in timestamps ({len(on_cache_not_in_timestamps)}):")
    print("   (Cached before timestamp tracking, or new Radarr/Sonarr downloads)")
    if on_cache_not_in_timestamps:
        for f in sorted(on_cache_not_in_timestamps)[:20]:
            print(f"   - {os.path.basename(f)}")
        if len(on_cache_not_in_timestamps) > 20:
            print(f"   ... and {len(on_cache_not_in_timestamps) - 20} more")
    else:
        print("   None - all good!")

    # In timestamps but not on cache (stale timestamp entries)
    print(f"\n🟡 In timestamps but NOT on cache ({len(in_timestamps_not_on_cache)}):")
    print("   (Stale entries - files were moved/deleted)")
    if in_timestamps_not_on_cache:
        for f in sorted(in_timestamps_not_on_cache)[:20]:
            print(f"   - {os.path.basename(f)}")
        if len(in_timestamps_not_on_cache) > 20:
            print(f"   ... and {len(in_timestamps_not_on_cache) - 20} more")
    else:
        print("   None - all good!")

    # Orphaned .plexcached files on array (no cache copy, not restored)
    orphaned_plexcached = get_orphaned_plexcached_files()
    print(f"\n🔴 Orphaned .plexcached on array ({len(orphaned_plexcached)}):")
    print("   (Backup files with no cache copy - need to restore to original name)")
    if orphaned_plexcached:
        for plexcached_path, original_path in sorted(orphaned_plexcached)[:10]:
            print(f"   - {os.path.basename(plexcached_path)}")
        if len(orphaned_plexcached) > 10:
            print(f"   ... and {len(orphaned_plexcached) - 10} more")
    else:
        print("   None - all good!")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if on_cache_not_in_exclude:
        print(f"\n⚠️  WARNING: {len(on_cache_not_in_exclude)} files on cache are NOT protected!")
        print("   Run the script to add them to exclude list, or sync them back to array.")
    else:
        print("\n✅ All cache files are properly tracked in exclude list.")

    if orphaned_plexcached:
        print(f"\n⚠️  WARNING: {len(orphaned_plexcached)} orphaned .plexcached files on array!")
        print("   Use --restore-plexcached to rename them back to original filenames.")

    # Only show fix options if there are issues to fix
    any_issues = (has_backup or no_backup or in_exclude_not_on_cache or
                  in_timestamps_not_on_cache or orphaned_plexcached)

    if any_issues:
        print("\n" + "=" * 80)
        print("FIX OPTIONS")
        print("=" * 80)

        if has_backup:
            print(f"\nFor {len(has_backup)} files WITH .plexcached backup:")
            print("  --fix-with-backup          Dry run (preview)")
            print("  --fix-with-backup --execute   Apply changes")

        if no_backup:
            print(f"\nFor {len(no_backup)} files WITHOUT backup (need to copy to array):")
            print("  --sync-to-array            Dry run (preview)")
            print("  --sync-to-array --execute     Apply changes")

        if has_backup or no_backup:
            print(f"\nTo protect all {len(on_cache_not_in_exclude)} unprotected files (add to exclude list):")
            print("  --add-to-exclude           Dry run (preview)")
            print("  --add-to-exclude --execute    Apply changes")

        if in_exclude_not_on_cache:
            print(f"\nTo clean {len(in_exclude_not_on_cache)} stale entries from exclude list:")
            print("  --clean-exclude            Dry run (preview)")
            print("  --clean-exclude --execute     Apply changes")

        if in_timestamps_not_on_cache:
            print(f"\nTo clean {len(in_timestamps_not_on_cache)} stale entries from timestamps file:")
            print("  --clean-timestamps         Dry run (preview)")
            print("  --clean-timestamps --execute  Apply changes")

        if orphaned_plexcached:
            print(f"\nTo restore {len(orphaned_plexcached)} orphaned .plexcached files on array:")
            print("  --restore-plexcached       Dry run (preview)")
            print("  --restore-plexcached --execute  Apply changes")

        print("\nRun with --help for full documentation")


def print_help():
    """Print help message with available options."""
    print("""
PlexCache Audit Script - Options:
==================================

Audit (default):
  python3 audit_cache.py              Run audit and show discrepancies

Fix Options (use with --execute to apply):
  --fix-with-backup    For files WITH .plexcached backup or duplicates:
                       Delete cache copy, restore .plexcached to original

  --add-to-exclude     Add all orphaned cache files to exclude list
                       (protects them from Unraid mover)

  --sync-to-array      For files WITHOUT backup:
                       rsync from cache to array, then remove cache copy

  --clean-exclude      Remove stale entries from exclude list
                       (files listed but no longer on cache)

  --clean-timestamps   Remove stale entries from timestamps file
                       (files listed but no longer on cache)

  --restore-plexcached Restore orphaned .plexcached files on array
                       (backups with no cache copy - rename to original)

  --execute            Actually apply changes (without this, shows dry run)

Diagnostic Options:
  --find-malformed     Find .plexcached files missing their media extension
                       (e.g., 'movie.plexcached' instead of 'movie.mkv.plexcached')

  --fix-malformed      Fix malformed .plexcached files by adding correct extension
                       Detects extension from cache file, or use --default-ext
                       Example: --fix-malformed --default-ext .mkv --execute

Legacy Options:
  --dry-run            Show which duplicates would be deleted
  --cleanup            Delete cache files that already exist on array

Examples:
  python3 audit_cache.py                          # Run audit
  python3 audit_cache.py --fix-with-backup        # Dry run - show what would be fixed
  python3 audit_cache.py --fix-with-backup --execute   # Actually fix files
  python3 audit_cache.py --sync-to-array          # Dry run - show what would sync
  python3 audit_cache.py --sync-to-array --execute     # Actually sync files
  python3 audit_cache.py --clean-exclude          # Dry run - show stale exclude entries
  python3 audit_cache.py --clean-exclude --execute     # Remove stale exclude entries
  python3 audit_cache.py --clean-timestamps       # Dry run - show stale timestamp entries
  python3 audit_cache.py --clean-timestamps --execute  # Remove stale timestamp entries
  python3 audit_cache.py --restore-plexcached     # Dry run - show orphaned .plexcached files
  python3 audit_cache.py --restore-plexcached --execute  # Restore orphaned .plexcached files
  python3 audit_cache.py --find-malformed         # Find .plexcached files missing media extension
  python3 audit_cache.py --fix-malformed          # Dry run - show what would be fixed
  python3 audit_cache.py --fix-malformed --execute  # Fix files (auto-detect extension)
  python3 audit_cache.py --fix-malformed --default-ext .mkv --execute  # Fix with default ext
""")


def find_malformed_plexcached():
    """Find .plexcached files that are missing their original media extension.

    Properly named: movie.mkv.plexcached, episode.mp4.plexcached
    Malformed: movie.plexcached (missing .mkv), episode.plexcached (missing .mp4)

    This helps diagnose a bug where .plexcached files were created without
    preserving the original file extension.
    """
    # Settings already loaded on module import

    # Valid media extensions (video + subtitle)
    MEDIA_EXTENSIONS = {
        '.mkv', '.mp4', '.avi', '.m4v', '.mov', '.wmv', '.ts', '.m2ts',
        '.webm', '.flv', '.mpg', '.mpeg', '.divx', '.xvid', '.3gp', '.ogv',
        '.srt', '.sub', '.ass', '.ssa', '.vtt', '.idx'
    }

    print("\n" + "=" * 80)
    print("SCANNING FOR MALFORMED .plexcached FILES")
    print("=" * 80)
    print("\nLooking for .plexcached files missing their original media extension...")
    print("(e.g., 'movie.plexcached' instead of 'movie.mkv.plexcached')\n")

    malformed = []
    total_scanned = 0

    for array_dir in ARRAY_DIRS:
        if not os.path.exists(array_dir):
            print(f"  Skipping (not found): {array_dir}")
            continue

        print(f"  Scanning: {array_dir}")

        for root, dirs, files in os.walk(array_dir):
            dirs[:] = [d for d in dirs if not _should_skip_directory(d)]
            for f in files:
                if f.endswith('.plexcached'):
                    total_scanned += 1
                    # Get the name without .plexcached suffix
                    base_name = f[:-11]  # Remove '.plexcached'

                    # Check if it has a valid media extension
                    has_extension = False
                    for ext in MEDIA_EXTENSIONS:
                        if base_name.lower().endswith(ext):
                            has_extension = True
                            break

                    if not has_extension:
                        full_path = os.path.join(root, f)
                        try:
                            stat = os.stat(full_path)
                            size = stat.st_size
                            size_str = f"{size / (1024**3):.2f} GB" if size >= 1024**3 else f"{size / (1024**2):.2f} MB"
                            from datetime import datetime
                            mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        except OSError:
                            size_str = "Unknown"
                            mtime = "Unknown"

                        # Try to find the correct extension from the cache file
                        detected_ext = None
                        cache_file_path = None

                        # Convert array path to cache path and look for matching file
                        for i, array_dir in enumerate(ARRAY_DIRS):
                            if full_path.startswith(array_dir):
                                relative = os.path.relpath(root, array_dir)
                                cache_dir_to_check = os.path.join(CACHE_DIRS[i], relative)
                                if os.path.exists(cache_dir_to_check):
                                    # Look for files that start with base_name
                                    try:
                                        for cache_file in os.listdir(cache_dir_to_check):
                                            # Check if cache file matches base_name + extension
                                            cache_base, cache_ext = os.path.splitext(cache_file)
                                            if cache_base == base_name and cache_ext.lower() in MEDIA_EXTENSIONS:
                                                detected_ext = cache_ext
                                                cache_file_path = os.path.join(cache_dir_to_check, cache_file)
                                                break
                                    except OSError:
                                        pass
                                break

                        malformed.append({
                            'path': full_path,
                            'filename': f,
                            'base_name': base_name,
                            'size': size_str,
                            'modified': mtime,
                            'detected_ext': detected_ext,
                            'cache_file': cache_file_path
                        })

    print(f"\nScanned {total_scanned} .plexcached files")
    print("=" * 80)

    if malformed:
        # Count how many have detected extensions
        fixable = [m for m in malformed if m['detected_ext']]
        unfixable = [m for m in malformed if not m['detected_ext']]

        print(f"\n⚠️  FOUND {len(malformed)} MALFORMED .plexcached FILES:\n")

        if fixable:
            print(f"  ✅ {len(fixable)} can be auto-fixed (extension detected from cache file)")
        if unfixable:
            print(f"  ❌ {len(unfixable)} need manual fix (no cache file found to detect extension)")

        print()

        for item in malformed:
            print(f"  File: {item['filename']}")
            print(f"  Base: {item['base_name']} (missing extension!)")
            if item['detected_ext']:
                print(f"  Detected: {item['detected_ext']} (from cache file)")
                print(f"  Fix to: {item['base_name']}{item['detected_ext']}.plexcached")
            else:
                print(f"  Detected: UNKNOWN (no cache file found)")
            print(f"  Size: {item['size']}")
            print(f"  Modified: {item['modified']}")
            print(f"  Path: {item['path']}")
            print()

        print("=" * 80)
        print("FIX OPTIONS")
        print("=" * 80)

        if fixable:
            print(f"""
To fix the {len(fixable)} file(s) with detected extensions:
  python3 audit_cache.py --fix-malformed           # Dry run (preview)
  python3 audit_cache.py --fix-malformed --execute # Apply fixes
""")

        if unfixable:
            print(f"""
For the {len(unfixable)} file(s) without detected extensions, manually rename:
  mv 'episode.plexcached' 'episode.mkv.plexcached'

Or specify a default extension:
  python3 audit_cache.py --fix-malformed --default-ext .mkv --execute
""")
    else:
        print("\n✅ No malformed .plexcached files found - all files have proper extensions!")

    return malformed


def fix_malformed_plexcached(dry_run=True, default_ext=None):
    """Fix malformed .plexcached files by renaming them with the correct extension.

    Args:
        dry_run: If True, only show what would be done without making changes.
        default_ext: Default extension to use if cache file not found (e.g., '.mkv')
    """
    malformed = find_malformed_plexcached()

    if not malformed:
        return

    fixable = [m for m in malformed if m['detected_ext'] or default_ext]

    if not fixable:
        print("\n❌ No files can be fixed - no extensions detected and no default provided.")
        print("   Use --default-ext .mkv to specify a default extension.")
        return

    print("\n" + "=" * 80)
    print("FIXING MALFORMED .plexcached FILES" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 80)

    fixed = 0
    errors = []

    for item in malformed:
        ext = item['detected_ext'] or default_ext
        if not ext:
            print(f"\n  SKIP: {item['filename']} (no extension detected, no default)")
            continue

        old_path = item['path']
        # Insert extension before .plexcached
        new_filename = f"{item['base_name']}{ext}.plexcached"
        new_path = os.path.join(os.path.dirname(old_path), new_filename)

        source = "detected" if item['detected_ext'] else "default"
        print(f"\n  {'Would rename' if dry_run else 'Renaming'}: {item['filename']}")
        print(f"         To: {new_filename} ({source}: {ext})")

        if not dry_run:
            try:
                # Check if target already exists
                if os.path.exists(new_path):
                    errors.append(f"{item['filename']}: Target already exists")
                    print(f"    ❌ ERROR: Target file already exists!")
                    continue

                os.rename(old_path, new_path)
                fixed += 1
                print(f"    ✅ Done")
            except OSError as e:
                errors.append(f"{item['filename']}: {e}")
                print(f"    ❌ ERROR: {e}")

    print("\n" + "=" * 80)
    if dry_run:
        print(f"DRY RUN: Would fix {len(fixable)} file(s)")
        print("Run with --execute to apply changes.")
    else:
        print(f"Fixed {fixed} file(s), {len(errors)} error(s)")
        if errors:
            print("\nErrors:")
            for err in errors:
                print(f"  - {err}")


if __name__ == "__main__":
    args = sys.argv[1:]
    execute = "--execute" in args

    if "--help" in args or "-h" in args:
        print_help()
    elif "--fix-with-backup" in args:
        fix_with_backup(dry_run=not execute)
    elif "--add-to-exclude" in args:
        add_to_exclude(dry_run=not execute)
    elif "--sync-to-array" in args:
        sync_to_array(dry_run=not execute)
    elif "--clean-exclude" in args:
        clean_exclude(dry_run=not execute)
    elif "--clean-timestamps" in args:
        clean_timestamps(dry_run=not execute)
    elif "--restore-plexcached" in args:
        restore_plexcached(dry_run=not execute)
    elif "--find-malformed" in args:
        find_malformed_plexcached()
    elif "--fix-malformed" in args:
        # Parse --default-ext argument if provided
        default_ext = None
        for i, arg in enumerate(args):
            if arg == "--default-ext" and i + 1 < len(args):
                default_ext = args[i + 1]
                if not default_ext.startswith('.'):
                    default_ext = '.' + default_ext
                break
        fix_malformed_plexcached(dry_run=not execute, default_ext=default_ext)
    elif "--cleanup" in args:
        cleanup_duplicates(dry_run=False)
    elif "--dry-run" in args:
        cleanup_duplicates(dry_run=True)
    else:
        main()
