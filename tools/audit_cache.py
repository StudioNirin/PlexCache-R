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
EXCLUDE_FILE = os.path.join(PROJECT_ROOT, "plexcache_mover_files_to_exclude.txt")
TIMESTAMPS_FILE = os.path.join(DATA_DIR, "timestamps.json")

# Legacy file locations (for migration)
LEGACY_TIMESTAMPS_FILE = os.path.join(PROJECT_ROOT, "plexcache_timestamps.json")


def load_settings():
    """Load paths from plexcache_settings.json."""
    global CACHE_DIRS, ARRAY_DIRS, EXCLUDE_FILE, TIMESTAMPS_FILE

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

        # Files are in script directory
        EXCLUDE_FILE = os.path.join(SCRIPT_DIR, "plexcache_mover_files_to_exclude.txt")
        TIMESTAMPS_FILE = os.path.join(SCRIPT_DIR, "plexcache_timestamps.json")

        print(f"Loaded settings from: {SETTINGS_FILE}")
        print(f"Cache directories: {CACHE_DIRS}")
        print(f"Array directories: {ARRAY_DIRS}")

    except Exception as e:
        print(f"❌ Error loading settings: {e}")
        sys.exit(1)


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
        print(f"\n   ⚠️  NO .plexcached backup ({len(no_backup)}) - need to rsync back:")
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
    print("   (Cached before timestamp tracking was added)")
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

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    if on_cache_not_in_exclude:
        print(f"\n⚠️  WARNING: {len(on_cache_not_in_exclude)} files on cache are NOT protected!")
        print("   Run the script to add them to exclude list, or sync them back to array.")
    else:
        print("\n✅ All cache files are properly tracked in exclude list.")

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

  --execute            Actually apply changes (without this, shows dry run)

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
""")


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
    elif "--cleanup" in args:
        cleanup_duplicates(dry_run=False)
    elif "--dry-run" in args:
        cleanup_duplicates(dry_run=True)
    else:
        main()
        print("\n" + "=" * 80)
        print("FIX OPTIONS")
        print("=" * 80)
        print("\nFor files WITH .plexcached backup:")
        print("  --fix-with-backup          Dry run (preview)")
        print("  --fix-with-backup --execute   Apply changes")
        print("\nFor files WITHOUT backup (need to copy to array):")
        print("  --sync-to-array            Dry run (preview)")
        print("  --sync-to-array --execute     Apply changes")
        print("\nTo protect files (add to exclude list):")
        print("  --add-to-exclude           Dry run (preview)")
        print("  --add-to-exclude --execute    Apply changes")
        print("\nTo clean stale entries from exclude list:")
        print("  --clean-exclude            Dry run (preview)")
        print("  --clean-exclude --execute     Apply changes")
        print("\nTo clean stale entries from timestamps file:")
        print("  --clean-timestamps         Dry run (preview)")
        print("  --clean-timestamps --execute  Apply changes")
        print("\nRun with --help for full documentation")
