#!/usr/bin/env python3
"""
Find Duplicate Files — Items with multiple files under the same Plex rating_key.

Scans all library sections and reports items that have more than one media file,
which typically means duplicate downloads or failed upgrades.

Cleanup mode uses Sonarr/Radarr APIs to identify which file is actively tracked,
then safely removes the orphaned duplicates.

Usage:
    # Report only
    python tools/find_duplicates.py
    python tools/find_duplicates.py --movies-only
    python tools/find_duplicates.py --tv-only

    # Cleanup (dry-run — shows what would be deleted)
    python tools/find_duplicates.py --cleanup --sonarr-url http://localhost:8989 --sonarr-key YOUR_KEY

    # Cleanup with Radarr for movies too
    python tools/find_duplicates.py --cleanup --sonarr-url http://localhost:8989 --sonarr-key KEY \\
        --radarr-url http://localhost:7878 --radarr-key KEY

    # Actually delete (prompts for confirmation)
    python tools/find_duplicates.py --delete --sonarr-url http://localhost:8989 --sonarr-key KEY

    # Delete without confirmation + path translation (host execution)
    python tools/find_duplicates.py --delete --yes --sonarr-url http://localhost:8989 --sonarr-key KEY \\
        --plex-path /data --fs-path /mnt/user

Environment variables (fallback if CLI args not provided):
    SONARR_URL, SONARR_API_KEY, RADARR_URL, RADARR_API_KEY
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR) if os.path.basename(SCRIPT_DIR) == 'tools' else SCRIPT_DIR
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

SETTINGS_FILE = os.path.join(PROJECT_ROOT, "plexcache_settings.json")


def load_plex_settings():
    """Load PLEX_URL and PLEX_TOKEN from plexcache_settings.json."""
    if not os.path.exists(SETTINGS_FILE):
        print(f"ERROR: Settings file not found: {SETTINGS_FILE}")
        sys.exit(1)

    with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
        settings = json.load(f)

    url = settings.get('PLEX_URL', '').strip()
    token = settings.get('PLEX_TOKEN', '').strip()

    if not url or not token:
        print("ERROR: PLEX_URL or PLEX_TOKEN missing from settings.")
        sys.exit(1)

    return url, token


def connect_to_plex(url, token):
    """Connect to Plex server."""
    try:
        from plexapi.server import PlexServer
    except ImportError:
        print("ERROR: plexapi not installed. Run: pip install plexapi")
        sys.exit(1)

    print(f"Connecting to Plex at {url}...")
    try:
        plex = PlexServer(url, token)
        print(f"Connected: {plex.friendlyName} (version {plex.version})\n")
        return plex
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        sys.exit(1)


def format_size(size_bytes):
    """Format bytes to human-readable size."""
    if not size_bytes:
        return "unknown"
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


# ---------------------------------------------------------------------------
# Plex scanning
# ---------------------------------------------------------------------------

def scan_section(section, duplicates):
    """Scan a library section for items with multiple files."""
    section_type = section.type  # 'movie' or 'show'
    print(f"  Scanning: {section.title} ({section_type})...")

    if section_type == 'movie':
        for movie in section.all():
            files = []
            for media in movie.media:
                for part in media.parts:
                    files.append({
                        'file': part.file,
                        'size': part.size,
                        'resolution': media.videoResolution or '?',
                        'container': media.container or '?',
                        'video_codec': media.videoCodec or '?',
                        'bitrate': media.bitrate,
                    })
            if len(files) > 1:
                duplicates.append({
                    'rating_key': str(movie.ratingKey),
                    'title': f"{movie.title} ({movie.year})" if movie.year else movie.title,
                    'type': 'movie',
                    'library': section.title,
                    'files': files,
                })

    elif section_type == 'show':
        for show in section.all():
            for episode in show.episodes():
                files = []
                for media in episode.media:
                    for part in media.parts:
                        files.append({
                            'file': part.file,
                            'size': part.size,
                            'resolution': media.videoResolution or '?',
                            'container': media.container or '?',
                            'video_codec': media.videoCodec or '?',
                            'bitrate': media.bitrate,
                        })
                if len(files) > 1:
                    show_title = episode.grandparentTitle or show.title
                    season = episode.parentIndex
                    ep_num = episode.index
                    if season is not None and ep_num is not None:
                        ep_title = f"{show_title} - S{season:02d}E{ep_num:02d} - {episode.title}"
                    else:
                        ep_title = f"{show_title} - {episode.title}"

                    duplicates.append({
                        'rating_key': str(episode.ratingKey),
                        'title': ep_title,
                        'type': 'episode',
                        'library': section.title,
                        'files': files,
                    })


# ---------------------------------------------------------------------------
# Arr API integration
# ---------------------------------------------------------------------------

def get_sonarr_tracked_files(sonarr_url, sonarr_key):
    """Get all tracked episode file basenames from Sonarr.

    Returns dict of {basename: full_sonarr_path}.
    """
    import requests

    tracked = {}
    headers = {'X-Api-Key': sonarr_key}
    base_url = sonarr_url.rstrip('/')

    # Get all series
    print("  Sonarr: Fetching series list...")
    resp = requests.get(f'{base_url}/api/v3/series', headers=headers, timeout=30)
    resp.raise_for_status()
    series_list = resp.json()
    total = len(series_list)
    print(f"  Sonarr: {total} series found, scanning episode files...")

    for i, series in enumerate(series_list, 1):
        if i % 25 == 0 or i == total:
            sys.stdout.write(f"\r  Sonarr: Scanning series {i}/{total}...")
            sys.stdout.flush()

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
            print(f"\n  WARNING: Failed to get files for '{series.get('title', '?')}': {e}")

    print(f"\r  Sonarr: {len(tracked)} episode files tracked across {total} series")
    return tracked


def get_radarr_tracked_files(radarr_url, radarr_key):
    """Get all tracked movie file basenames from Radarr.

    Returns dict of {basename: full_radarr_path}.
    """
    import requests

    tracked = {}
    headers = {'X-Api-Key': radarr_key}
    base_url = radarr_url.rstrip('/')

    print("  Radarr: Fetching movie list...")
    resp = requests.get(f'{base_url}/api/v3/movie', headers=headers, timeout=60)
    resp.raise_for_status()
    movies = resp.json()
    print(f"  Radarr: {len(movies)} movies found")

    for movie in movies:
        movie_file = movie.get('movieFile')
        if movie_file:
            path = movie_file.get('path', '')
            if path:
                tracked[os.path.basename(path)] = path

    print(f"  Radarr: {len(tracked)} movie files tracked")
    return tracked


# ---------------------------------------------------------------------------
# Orphan identification
# ---------------------------------------------------------------------------

def identify_orphans(duplicates, tracked_files):
    """Classify files in each duplicate set as keeper or orphan.

    For each duplicate item:
    - If exactly one file is tracked by Sonarr/Radarr, it's the keeper; rest are orphans.
    - If no files are tracked (series not in arr), the item is unresolved and skipped.
    - If multiple files are tracked (shouldn't happen), the item is unresolved and skipped.

    Returns (orphans_list, unresolved_list).
    """
    orphans = []
    unresolved = []

    for item in duplicates:
        files = item['files']

        tracked_in_set = []
        untracked_in_set = []

        for f in files:
            basename = os.path.basename(f['file'])
            if basename in tracked_files:
                tracked_in_set.append(f)
            else:
                untracked_in_set.append(f)

        if len(tracked_in_set) == 1 and untracked_in_set:
            # Clear case: one tracked, rest are orphans
            item['_keeper'] = tracked_in_set[0]
            item['_orphans'] = untracked_in_set
            for f in untracked_in_set:
                orphans.append({
                    'file': f['file'],
                    'size': f.get('size', 0),
                    'item_title': item['title'],
                    'rating_key': item['rating_key'],
                })
        elif len(tracked_in_set) == 0:
            # Neither file tracked — series might not be in Sonarr/Radarr
            item['_keeper'] = None
            item['_orphans'] = []
            unresolved.append(item)
        else:
            # Multiple tracked or other edge case — skip to be safe
            item['_keeper'] = None
            item['_orphans'] = []
            unresolved.append(item)

    return orphans, unresolved


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def print_duplicates(duplicates, cleanup_mode=False):
    """Print duplicates grouped by library.

    In cleanup_mode, shows KEEP/DELETE markers instead of largest markers.
    """
    if not duplicates:
        print("No duplicates found.")
        return

    by_library = {}
    for dup in duplicates:
        lib = dup['library']
        by_library.setdefault(lib, []).append(dup)

    total_wasted = 0
    total_recoverable = 0

    for library, items in sorted(by_library.items()):
        print(f"\n{'='*80}")
        print(f"  {library} -- {len(items)} item(s) with multiple files")
        print(f"{'='*80}")

        for item in items:
            print(f"\n  [{item['rating_key']:>7}] {item['title']}")

            keeper = item.get('_keeper')
            orphan_files = {os.path.basename(o['file']) for o in item.get('_orphans', [])}

            # Sort files by size descending
            sorted_files = sorted(item['files'], key=lambda f: f.get('size') or 0, reverse=True)

            for f in sorted_files:
                size = format_size(f['size'])
                bitrate_str = f"  {f['bitrate']}kbps" if f.get('bitrate') else ""
                basename = os.path.basename(f['file'])

                if cleanup_mode and keeper:
                    if f is keeper:
                        marker = "  <- KEEP (tracked)"
                    elif basename in orphan_files:
                        marker = "  <- DELETE (orphan)"
                    else:
                        marker = ""
                elif cleanup_mode and keeper is None:
                    marker = "  <- SKIP (not in arr)"
                else:
                    # Report mode — show largest
                    marker = "  <- largest" if f is sorted_files[0] else ""

                print(f"    {f['resolution']:>5}p | {f['video_codec']:>5} | {size:>10}{bitrate_str}")
                print(f"           {f['file']}{marker}")

            # Calculate space
            sizes = [f.get('size') or 0 for f in sorted_files]
            if all(sizes):
                wasted = sum(sizes) - max(sizes)
                total_wasted += wasted

                if cleanup_mode and item.get('_orphans'):
                    recoverable = sum(o.get('size', 0) for o in item['_orphans'])
                    total_recoverable += recoverable
                    print(f"    Recoverable: {format_size(recoverable)}")
                else:
                    print(f"    Duplicate space: {format_size(wasted)}")

    print(f"\n{'='*80}")
    print(f"  SUMMARY")
    print(f"{'='*80}")
    print(f"  Total items with duplicates: {len(duplicates)}")
    print(f"  Total duplicate space:       {format_size(total_wasted)}")
    if cleanup_mode:
        orphan_count = sum(len(item.get('_orphans', [])) for item in duplicates)
        unresolved_count = sum(1 for item in duplicates if item.get('_keeper') is None)
        print(f"  Orphans to delete:           {orphan_count} files ({format_size(total_recoverable)})")
        if unresolved_count:
            print(f"  Unresolved (skipped):        {unresolved_count} items (not tracked by arr)")
    print()


def translate_path(plex_path, plex_prefix, fs_prefix):
    """Translate a Plex container path to a filesystem path."""
    if plex_prefix and fs_prefix and plex_path.startswith(plex_prefix):
        return fs_prefix + plex_path[len(plex_prefix):]
    return plex_path


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

def execute_cleanup(orphans, plex_prefix=None, fs_prefix=None):
    """Delete orphan files from the filesystem."""
    deleted = 0
    freed = 0
    errors = []

    for orphan in orphans:
        fs_path = translate_path(orphan['file'], plex_prefix, fs_prefix)
        try:
            if os.path.exists(fs_path):
                file_size = os.path.getsize(fs_path)
                os.remove(fs_path)
                deleted += 1
                freed += file_size
                print(f"  DELETED  {format_size(file_size):>10}  {fs_path}")
            else:
                errors.append(f"  NOT FOUND  {fs_path}")
        except OSError as e:
            errors.append(f"  ERROR    {fs_path}: {e}")

    print(f"\n{'='*80}")
    print(f"  CLEANUP RESULTS")
    print(f"{'='*80}")
    print(f"  Files deleted:  {deleted}")
    print(f"  Space freed:    {format_size(freed)}")
    if errors:
        print(f"  Errors:         {len(errors)}")
        for err in errors:
            print(err)
    print()
    print("  TIP: Run a Plex library scan to clean up removed media entries.")
    print("       (Library > ... > Scan Library Files)")
    print()

    return deleted, freed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find items with multiple files in Plex libraries. "
                    "Use --cleanup to identify orphans via Sonarr/Radarr, "
                    "or --delete to remove them.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  %(prog)s                              Report duplicates only
  %(prog)s --tv-only                    TV shows only
  %(prog)s --library 'TV Shows'         Scan one library by name
  %(prog)s --exclude-library 'TV Shows UHD'
                                        Skip a library
  %(prog)s --cleanup --sonarr-url URL --sonarr-key KEY
                                        Dry-run: show which files would be deleted
  %(prog)s --delete --sonarr-url URL --sonarr-key KEY
                                        Delete orphaned files (with confirmation)
  %(prog)s --delete --yes --plex-path /data --fs-path /mnt/user ...
                                        Delete without prompt, translate paths
""",
    )

    # Scan filters
    scan_group = parser.add_argument_group('scan options')
    scan_group.add_argument('--movies-only', action='store_true',
                            help="Only scan movie libraries")
    scan_group.add_argument('--tv-only', action='store_true',
                            help="Only scan TV show libraries")
    scan_group.add_argument('--library', action='append', metavar='NAME',
                            help="Only scan these Plex libraries (repeatable, "
                                 "e.g., --library 'TV Shows' --library 'Movies')")
    scan_group.add_argument('--exclude-library', action='append', metavar='NAME',
                            help="Skip these Plex libraries (repeatable, "
                                 "e.g., --exclude-library 'TV Shows UHD')")

    # Arr integration
    arr_group = parser.add_argument_group('arr integration (for cleanup/delete)')
    arr_group.add_argument('--sonarr-url',
                           default=os.environ.get('SONARR_URL', ''),
                           help="Sonarr URL (e.g., http://localhost:8989). "
                                "Env: SONARR_URL")
    arr_group.add_argument('--sonarr-key',
                           default=os.environ.get('SONARR_API_KEY', ''),
                           help="Sonarr API key. Env: SONARR_API_KEY")
    arr_group.add_argument('--radarr-url',
                           default=os.environ.get('RADARR_URL', ''),
                           help="Radarr URL (e.g., http://localhost:7878). "
                                "Env: RADARR_URL")
    arr_group.add_argument('--radarr-key',
                           default=os.environ.get('RADARR_API_KEY', ''),
                           help="Radarr API key. Env: RADARR_API_KEY")

    # Cleanup/delete
    action_group = parser.add_argument_group('cleanup actions')
    action_group.add_argument('--cleanup', action='store_true',
                              help="Dry-run: show cleanup plan (KEEP/DELETE markers)")
    action_group.add_argument('--delete', action='store_true',
                              help="Delete orphaned files from disk")
    action_group.add_argument('--yes', '-y', action='store_true',
                              help="Skip confirmation prompt (use with --delete)")

    # Path translation (when running on host vs inside Docker)
    path_group = parser.add_argument_group('path translation (optional)')
    path_group.add_argument('--plex-path',
                            help="Plex container path prefix (e.g., /data)")
    path_group.add_argument('--fs-path',
                            help="Filesystem path prefix (e.g., /mnt/user)")

    args = parser.parse_args()

    # Validation
    cleanup_mode = args.cleanup or args.delete

    if cleanup_mode:
        has_sonarr = args.sonarr_url and args.sonarr_key
        has_radarr = args.radarr_url and args.radarr_key
        if not has_sonarr and not has_radarr:
            parser.error("--cleanup/--delete requires at least one of:\n"
                         "  --sonarr-url + --sonarr-key (for TV shows)\n"
                         "  --radarr-url + --radarr-key (for movies)\n"
                         "Or set SONARR_URL/SONARR_API_KEY environment variables.")

    if args.plex_path and not args.fs_path:
        parser.error("--plex-path requires --fs-path")
    if args.fs_path and not args.plex_path:
        parser.error("--fs-path requires --plex-path")

    # Connect to Plex and scan
    url, token = load_plex_settings()
    plex = connect_to_plex(url, token)

    duplicates = []
    print("Scanning libraries for duplicate files...")

    include_libs = {name.lower() for name in args.library} if args.library else None
    exclude_libs = {name.lower() for name in args.exclude_library} if args.exclude_library else set()

    for section in plex.library.sections():
        if args.movies_only and section.type != 'movie':
            continue
        if args.tv_only and section.type != 'show':
            continue
        if include_libs and section.title.lower() not in include_libs:
            continue
        if section.title.lower() in exclude_libs:
            print(f"  Skipping: {section.title} (excluded)")
            continue
        if section.type in ('movie', 'show'):
            scan_section(section, duplicates)

    if not duplicates:
        print("\nNo duplicates found.")
        return

    # In cleanup mode, query arr APIs and identify orphans
    if cleanup_mode:
        tracked = {}
        print("\nQuerying arr APIs for tracked files...")

        if args.sonarr_url and args.sonarr_key:
            try:
                sonarr_tracked = get_sonarr_tracked_files(args.sonarr_url, args.sonarr_key)
                tracked.update(sonarr_tracked)
            except Exception as e:
                print(f"  ERROR: Sonarr query failed: {e}")
                if not (args.radarr_url and args.radarr_key):
                    sys.exit(1)

        if args.radarr_url and args.radarr_key:
            try:
                radarr_tracked = get_radarr_tracked_files(args.radarr_url, args.radarr_key)
                tracked.update(radarr_tracked)
            except Exception as e:
                print(f"  ERROR: Radarr query failed: {e}")
                if not tracked:
                    sys.exit(1)

        if not tracked:
            print("ERROR: No tracked files found from any arr API.")
            sys.exit(1)

        # Identify orphans
        orphans, unresolved = identify_orphans(duplicates, tracked)
        print_duplicates(duplicates, cleanup_mode=True)

        # Delete if requested
        if args.delete and orphans:
            orphan_count = len(orphans)
            orphan_size = sum(o['size'] for o in orphans)

            if not args.yes:
                print(f"  About to delete {orphan_count} orphaned files "
                      f"({format_size(orphan_size)}).")
                try:
                    resp = input("  Proceed? (yes/no): ").strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print("\n  Aborted.")
                    return
                if resp not in ('yes', 'y'):
                    print("  Aborted.")
                    return

            print()
            execute_cleanup(orphans, args.plex_path, args.fs_path)
        elif args.delete and not orphans:
            print("  No orphans identified — nothing to delete.")
    else:
        # Report mode (no arr integration)
        print_duplicates(duplicates, cleanup_mode=False)


if __name__ == '__main__':
    main()
