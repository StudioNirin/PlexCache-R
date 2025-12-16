import json, os, requests, ntpath, posixpath, re, uuid, time, webbrowser
from urllib.parse import urlparse
from plexapi.server import PlexServer
from plexapi.exceptions import BadRequest

# Script folder and settings file
script_folder = os.path.dirname(os.path.abspath(__file__))
settings_filename = os.path.join(script_folder, "plexcache_settings.json")

# ensure a settings container exists early so helper functions can reference it
settings_data = {}

# ---------------- Helper Functions ----------------

def check_directory_exists(folder):
    if not os.path.exists(folder):
        raise FileNotFoundError(f'Wrong path given, please edit the "{folder}" variable accordingly.')

def read_existing_settings(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (IOError, OSError) as e:
        print(f"Error reading settings file: {e}")
        raise

def write_settings(filename, data):
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except (IOError, OSError) as e:
        print(f"Error writing settings file: {e}")
        raise

def convert_path_to_posix(path):
    path = path.replace(ntpath.sep, posixpath.sep)
    return posixpath.normpath(path)

def convert_path_to_nt(path):
    path = path.replace(posixpath.sep, ntpath.sep)
    return ntpath.normpath(path)

def prompt_user_for_number(prompt_message, default_value, data_key, data_type=int):
    while True:
        user_input = input(prompt_message) or default_value
        try:
            value = data_type(user_input)
            if value < 0:
                print("Please enter a non-negative number")
                continue
            settings_data[data_key] = value
            break
        except ValueError:
            print("User input is not a valid number")

def prompt_user_for_duration(prompt_message, default_value, data_key):
    """Prompt for a duration value that accepts hours (default) or days.

    Accepts formats: 12, 12h, 12d (defaults to hours if no suffix)
    Stores the value in hours.
    """
    while True:
        user_input = (input(prompt_message) or default_value).strip().lower()
        try:
            # Check for day suffix
            if user_input.endswith('d'):
                days = float(user_input[:-1])
                if days < 0:
                    print("Please enter a non-negative number")
                    continue
                hours = int(days * 24)
                settings_data[data_key] = hours
                print(f"  Set to {hours} hours ({days} days)")
                break
            # Check for hour suffix (or no suffix - default to hours)
            elif user_input.endswith('h'):
                hours = int(user_input[:-1])
            else:
                hours = int(user_input)

            if hours < 0:
                print("Please enter a non-negative number")
                continue
            settings_data[data_key] = hours
            break
        except ValueError:
            print("Invalid input. Enter a number, optionally with 'h' for hours or 'd' for days (e.g., 12, 12h, 2d)")

def prompt_user_for_duration_days(prompt_message, default_value, data_key):
    """Prompt for a duration value that accepts days (default) or hours.

    Accepts formats: 30, 30d, 12h (defaults to days if no suffix)
    Stores the value in days (as float to support fractional days from hours).
    """
    while True:
        user_input = (input(prompt_message) or default_value).strip().lower()
        try:
            # Check for hour suffix
            if user_input.endswith('h'):
                hours = float(user_input[:-1])
                if hours < 0:
                    print("Please enter a non-negative number")
                    continue
                days = hours / 24
                settings_data[data_key] = days
                print(f"  Set to {days:.2f} days ({hours} hours)")
                break
            # Check for day suffix (or no suffix - default to days)
            elif user_input.endswith('d'):
                days = float(user_input[:-1])
            else:
                days = float(user_input)

            if days < 0:
                print("Please enter a non-negative number")
                continue
            settings_data[data_key] = days
            break
        except ValueError:
            print("Invalid input. Enter a number, optionally with 'd' for days or 'h' for hours (e.g., 30, 30d, 12h)")

def is_valid_plex_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

# Helper to compute a common root for a list of paths
def find_common_root(paths):
    """Return the deepest common directory for all given paths."""
    if not paths:
        return "/"

    # Normalize trailing slashes and split
    normed = [p.rstrip('/') for p in paths]
    split_paths = [p.split('/') for p in normed]

    common_parts = []
    for parts in zip(*split_paths):
        if all(part == parts[0] for part in parts):
            common_parts.append(parts[0])
        else:
            break

    # Handle leading empty string (absolute paths)
    if common_parts and common_parts[0] == '':
        if len(common_parts) == 1:
            return '/'
        return "/" + "/".join(common_parts[1:])
    return "/" + "/".join(common_parts) if common_parts else "/"


def is_unraid():
    """Check if running on Unraid."""
    return os.path.exists('/etc/unraid-version')


# ---------------- Multi-Path Mapping Functions ----------------

def prompt_library_path_mapping(library_name: str, plex_locations: list, cache_root: str = None) -> list:
    """Prompt user to configure path mappings for a library's locations.

    Args:
        library_name: Display name of the library (e.g., "Movies")
        plex_locations: List of Plex paths for this library
        cache_root: Cache root directory (e.g., /mnt/cache_downloads/)

    Returns:
        List of path mapping dicts for this library's locations
    """
    mappings = []

    if len(plex_locations) > 1:
        print(f"\n  Plex locations for this library:")
        for loc in plex_locations:
            print(f"    - {loc}")

    for i, plex_path in enumerate(plex_locations):
        # For libraries with multiple locations, number them
        if len(plex_locations) > 1:
            mapping_name = f"{library_name} ({i+1})"
            print(f"\n  Configuring location {i+1}: {plex_path}")
        else:
            mapping_name = library_name

        # Suggest a real path based on common patterns
        suggested_real = plex_path.replace('/data/', '/mnt/user/').replace('/media/', '/mnt/user/')

        print(f"  Where is this located on your filesystem?")
        real_path = input(f"  Real path [{suggested_real}]: ").strip() or suggested_real

        # Ensure trailing slash
        if real_path and not real_path.endswith('/'):
            real_path = real_path + '/'
        # Ensure plex_path has trailing slash
        plex_path_normalized = plex_path if plex_path.endswith('/') else plex_path + '/'

        # Ask if cacheable
        print(f"\n  Can this be cached locally? (No for remote/network storage)")
        cacheable_input = input(f"  Cacheable? [Y/n]: ").strip().lower()
        cacheable = cacheable_input not in ['n', 'no']

        cache_path = None
        if cacheable and cache_root:
            # Use cache_root + library name (sanitized)
            # e.g., /mnt/cache_downloads/ + "Movies" = /mnt/cache_downloads/Movies/
            lib_folder = library_name.replace('/', '_').replace('\\', '_')
            if len(plex_locations) > 1:
                # For multi-location libraries, use folder name from plex path
                lib_folder = plex_path.rstrip('/').split('/')[-1]
            suggested_cache = cache_root.rstrip('/') + '/' + lib_folder + '/'

            print(f"\n  Where should cached files be stored?")
            cache_path = input(f"  Cache path [{suggested_cache}]: ").strip() or suggested_cache

            # Ensure trailing slash
            if cache_path and not cache_path.endswith('/'):
                cache_path = cache_path + '/'
        elif cacheable and not cache_root:
            # No cache root set - shouldn't happen in new flow but handle gracefully
            suggested_cache = real_path.replace('/mnt/user/', '/mnt/cache/')
            print(f"\n  Where should cached files be stored?")
            cache_path = input(f"  Cache path [{suggested_cache}]: ").strip() or suggested_cache
            if cache_path and not cache_path.endswith('/'):
                cache_path = cache_path + '/'

        mapping = {
            'name': mapping_name,
            'plex_path': plex_path_normalized,
            'real_path': real_path,
            'cache_path': cache_path,
            'cacheable': cacheable,
            'enabled': True
        }
        mappings.append(mapping)

        cache_display = f" → {cache_path}" if cache_path else " (non-cacheable)"
        print(f"\n  ✓ {mapping_name}: {plex_path_normalized} → {real_path}{cache_display}")

    return mappings


def display_path_mappings(mappings):
    """Display current path mappings in a formatted table."""
    if not mappings:
        print("\n  No path mappings configured.")
        return

    print("\n  Current Path Mappings:")
    print("  " + "-" * 70)
    for i, m in enumerate(mappings, 1):
        status = "enabled" if m.get('enabled', True) else "DISABLED"
        cacheable = "cacheable" if m.get('cacheable', True) else "non-cacheable"
        print(f"  {i}. {m.get('name', 'Unnamed')}")
        print(f"     Plex path:  {m.get('plex_path', '')}")
        print(f"     Real path:  {m.get('real_path', '')}")
        if m.get('cacheable', True):
            cache_path = m.get('cache_path') or 'Not set'
            print(f"     Cache path: {cache_path}")
        print(f"     Status: {status}, {cacheable}")
        print()


def prompt_for_path_mapping(existing=None):
    """Prompt user to create or edit a path mapping."""
    print("\n" + "-" * 60)
    if existing:
        print("EDIT PATH MAPPING")
        print(f"Current name: {existing.get('name', '')}")
    else:
        print("ADD NEW PATH MAPPING")
    print("-" * 60)

    # Name
    default_name = existing.get('name', '') if existing else ''
    name = input(f"\nMapping name (e.g., 'Local Array', 'Remote NAS') [{default_name}]: ").strip()
    if not name and default_name:
        name = default_name
    elif not name:
        name = f"Mapping {1}"

    # Plex path
    default_plex = existing.get('plex_path', '') if existing else ''
    print(f"\nPlex path: The path as seen by Plex (inside Docker container)")
    print(f"  Example: /data or /media")
    plex_path = input(f"Plex path [{default_plex}]: ").strip()
    if not plex_path and default_plex:
        plex_path = default_plex
    # Ensure trailing slash
    if plex_path and not plex_path.endswith('/'):
        plex_path = plex_path + '/'

    # Real path
    default_real = existing.get('real_path', '') if existing else ''
    print(f"\nReal path: The actual filesystem path (on host/Unraid)")
    print(f"  Example: /mnt/user or /mnt/remotes/NAS")
    real_path = input(f"Real path [{default_real}]: ").strip()
    if not real_path and default_real:
        real_path = default_real
    # Ensure trailing slash
    if real_path and not real_path.endswith('/'):
        real_path = real_path + '/'

    # Cacheable?
    default_cacheable = existing.get('cacheable', True) if existing else True
    default_cacheable_str = 'Y' if default_cacheable else 'N'
    print(f"\nIs this path cacheable? (Set to No for remote/network storage)")
    cacheable_input = input(f"Cacheable? [{'Y/n' if default_cacheable else 'y/N'}]: ").strip().lower()
    if not cacheable_input:
        cacheable = default_cacheable
    else:
        cacheable = cacheable_input in ['y', 'yes']

    # Cache path (only if cacheable)
    cache_path = None
    if cacheable:
        default_cache = existing.get('cache_path', '') if existing else ''
        print(f"\nCache path: Where cached files are stored")
        print(f"  Example: /mnt/cache")
        cache_path = input(f"Cache path [{default_cache}]: ").strip()
        if not cache_path and default_cache:
            cache_path = default_cache
        # Ensure trailing slash
        if cache_path and not cache_path.endswith('/'):
            cache_path = cache_path + '/'

    # Enabled?
    default_enabled = existing.get('enabled', True) if existing else True

    return {
        'name': name,
        'plex_path': plex_path,
        'real_path': real_path,
        'cache_path': cache_path,
        'cacheable': cacheable,
        'enabled': default_enabled
    }


def configure_path_mappings(settings):
    """Interactive menu to configure multiple path mappings."""
    mappings = settings.get('path_mappings', [])

    # If no mappings but legacy settings exist, offer to convert
    if not mappings and settings.get('plex_source') and settings.get('real_source'):
        print("\n" + "=" * 60)
        print("MULTI-PATH MAPPING CONFIGURATION")
        print("=" * 60)
        print("\nYou have legacy single-path settings configured:")
        print(f"  Plex source: {settings.get('plex_source')}")
        print(f"  Real source: {settings.get('real_source')}")
        print(f"  Cache dir:   {settings.get('cache_dir')}")

        convert = input("\nConvert to multi-path format? [Y/n]: ").strip().lower()
        if convert in ['', 'y', 'yes']:
            mappings = [{
                'name': 'Primary',
                'plex_path': settings.get('plex_source', ''),
                'real_path': settings.get('real_source', ''),
                'cache_path': settings.get('cache_dir', ''),
                'cacheable': True,
                'enabled': True
            }]
            print("Converted legacy settings to path mapping.")

    while True:
        print("\n" + "=" * 60)
        print("PATH MAPPINGS MENU")
        print("=" * 60)
        display_path_mappings(mappings)

        print("  Options:")
        print("    [A] Add new path mapping")
        if mappings:
            print("    [E] Edit existing mapping")
            print("    [D] Delete mapping")
            print("    [T] Toggle enabled/disabled")
        print("    [S] Save and return")
        print()

        choice = input("Select option: ").strip().lower()

        if choice == 'a':
            new_mapping = prompt_for_path_mapping()
            if new_mapping.get('plex_path') and new_mapping.get('real_path'):
                mappings.append(new_mapping)
                print(f"\nAdded mapping: {new_mapping['name']}")
            else:
                print("\nMapping not added - plex_path and real_path are required.")

        elif choice == 'e' and mappings:
            try:
                idx = int(input(f"Enter mapping number to edit (1-{len(mappings)}): ")) - 1
                if 0 <= idx < len(mappings):
                    mappings[idx] = prompt_for_path_mapping(mappings[idx])
                    print(f"\nUpdated mapping: {mappings[idx]['name']}")
                else:
                    print("Invalid selection.")
            except ValueError:
                print("Invalid input.")

        elif choice == 'd' and mappings:
            try:
                idx = int(input(f"Enter mapping number to delete (1-{len(mappings)}): ")) - 1
                if 0 <= idx < len(mappings):
                    removed = mappings.pop(idx)
                    print(f"\nDeleted mapping: {removed['name']}")
                else:
                    print("Invalid selection.")
            except ValueError:
                print("Invalid input.")

        elif choice == 't' and mappings:
            try:
                idx = int(input(f"Enter mapping number to toggle (1-{len(mappings)}): ")) - 1
                if 0 <= idx < len(mappings):
                    mappings[idx]['enabled'] = not mappings[idx].get('enabled', True)
                    status = "enabled" if mappings[idx]['enabled'] else "disabled"
                    print(f"\nMapping '{mappings[idx]['name']}' is now {status}")
                else:
                    print("Invalid selection.")
            except ValueError:
                print("Invalid input.")

        elif choice == 's':
            settings['path_mappings'] = mappings
            print(f"\nSaved {len(mappings)} path mapping(s).")
            return settings

        else:
            print("Invalid option. Please try again.")


# ----------------  Plex OAuth PIN Authentication ----------------

# PlexCache-R client identifier - stored in settings for consistency
PLEXCACHE_CLIENT_ID_KEY = 'plexcache_client_id'
PLEXCACHE_PRODUCT_NAME = 'PlexCache-R'
PLEXCACHE_PRODUCT_VERSION = '1.0'


def get_or_create_client_id(settings: dict) -> str:
    """Get existing client ID from settings or create a new one."""
    if PLEXCACHE_CLIENT_ID_KEY in settings:
        return settings[PLEXCACHE_CLIENT_ID_KEY]
    # Generate new UUID for this installation
    client_id = str(uuid.uuid4())
    settings[PLEXCACHE_CLIENT_ID_KEY] = client_id
    return client_id


def plex_oauth_authenticate(settings: dict, timeout_seconds: int = 300):
    """
    Authenticate with Plex using the PIN-based OAuth flow.

    This is the official Plex authentication method that provides a user-scoped token.

    Workflow:
    1. Generate a PIN via POST to plex.tv/api/v2/pins
    2. User opens URL in browser and logs in
    3. Script polls until token is returned or timeout
    4. Returns the authentication token

    Args:
        settings: The settings dict (used to get/store client ID)
        timeout_seconds: How long to wait for user to authenticate (default 5 min)

    Returns:
        Authentication token string, or None if failed/cancelled
    """
    client_id = get_or_create_client_id(settings)

    headers = {
        'Accept': 'application/json',
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

    # Step 1: Request a PIN
    print("\nRequesting authentication PIN from Plex...")
    try:
        response = requests.post(
            'https://plex.tv/api/v2/pins',
            headers=headers,
            data={'strong': 'true'},  # Request a strong (long-lived) token
            timeout=30
        )
        response.raise_for_status()
        pin_data = response.json()
    except requests.RequestException as e:
        print(f"Error requesting PIN from Plex: {e}")
        return None

    pin_id = pin_data.get('id')
    pin_code = pin_data.get('code')

    if not pin_id or not pin_code:
        print("Error: Invalid response from Plex PIN endpoint")
        return None

    # Step 2: Build the auth URL and prompt user
    auth_url = f"https://app.plex.tv/auth#?clientID={client_id}&code={pin_code}&context%5Bdevice%5D%5Bproduct%5D={PLEXCACHE_PRODUCT_NAME}"

    print("\n" + "=" * 70)
    print("PLEX AUTHENTICATION")
    print("=" * 70)
    print("\nPlease open the following URL in your browser to authenticate:")
    print(f"\n  {auth_url}\n")

    # Try to open browser automatically
    try:
        webbrowser.open(auth_url)
        print("(A browser window should have opened automatically)")
    except Exception:
        print("(Could not open browser automatically - please copy the URL above)")

    print("\nAfter logging in and clicking 'Allow', return here.")
    print(f"Waiting for authentication (timeout: {timeout_seconds // 60} minutes)...")
    print("=" * 70)

    # Step 3: Poll for the token
    poll_interval = 2  # seconds between polls
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        try:
            response = requests.get(
                f'https://plex.tv/api/v2/pins/{pin_id}',
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            pin_status = response.json()

            auth_token = pin_status.get('authToken')
            if auth_token:
                print("\nAuthentication successful!")
                return auth_token

            # Check if PIN expired
            if pin_status.get('expiresAt'):
                # PIN is still valid, keep polling
                pass

        except requests.RequestException as e:
            print(f"\nWarning: Error checking PIN status: {e}")
            # Continue polling despite transient errors

        # Show progress indicator
        elapsed = int(time.time() - start_time)
        remaining = timeout_seconds - elapsed
        print(f"\r  Waiting... ({remaining}s remaining)    ", end='', flush=True)

        time.sleep(poll_interval)

    print("\n\nAuthentication timed out. Please try again.")
    return None


# ---------------- Setup Function ----------------

def print_step_header(step: int, total: int, title: str):
    """Print a formatted step header with progress indicator."""
    print("\n" + "=" * 60)
    print(f"STEP {step}/{total}: {title}")
    print("=" * 60)


def setup(advanced_mode: bool = False):
    """Run the PlexCache-R setup wizard.

    Args:
        advanced_mode: If True, show all configuration options.
                      If False, use sensible defaults for most settings.
    """
    global settings_data
    settings_data['firststart'] = False

    total_steps = 5  # Connection, Libraries, Behavior, Users, Final

    # ============================================================
    # STEP 1: PLEX CONNECTION
    # ============================================================
    print_step_header(1, total_steps, "PLEX CONNECTION")

    # Plex URL
    while 'PLEX_URL' not in settings_data:
        url = input('\nPlex server address [http://localhost:32400]: ').strip() or 'http://localhost:32400'
        if is_valid_plex_url(url):
            settings_data['PLEX_URL'] = url
            print(f"✓ Plex URL: {url}")
        else:
            print("✗ Invalid URL format")

    # Plex Token
    while 'PLEX_TOKEN' not in settings_data:
        token = None

        print("\nHow would you like to authenticate?")
        print("  [1] Authenticate via Plex.tv (recommended)")
        print("  [2] Enter token manually")

        while token is None:
            auth_choice = input("\nSelect option [1/2]: ").strip()

            if auth_choice == '1':
                token = plex_oauth_authenticate(settings_data)
                if token is None:
                    print("\nAuthentication failed or was cancelled.")
                    retry = input("Try again or enter manually? [retry/manual]: ").strip().lower()
                    if retry == 'manual':
                        token = input('\nEnter your Plex token: ')
                break

            elif auth_choice == '2':
                print("\nTo get your token:")
                print("  1. Open Plex Web App → Developer Tools (F12)")
                print("  2. Network tab → Find request to plex.tv")
                print("  3. Copy 'X-Plex-Token' from headers")
                token = input('\nEnter your Plex token: ')
                break
            else:
                print("Please enter 1 or 2")

        if not token or not token.strip():
            print("Token cannot be empty.")
            continue

        try:
            plex = PlexServer(settings_data['PLEX_URL'], token)
            user = plex.myPlexAccount().username
            print(f"\n✓ Connected as: {user}")
            libraries = plex.library.sections()
            settings_data['PLEX_TOKEN'] = token
            print(f"✓ Plex platform: {plex.platform}")

            valid_sections = []
            selected_libraries = []
            path_mappings = []

            # ============================================================
            # STEP 2: LIBRARY SELECTION & PATH CONFIGURATION
            # ============================================================
            print_step_header(2, total_steps, "LIBRARY SELECTION")

            # Ask for cache root ONCE at the beginning
            print("\nWhere is your cache drive located?")
            print("(All cacheable libraries will use subdirectories here)")
            default_cache = '/mnt/cache' if is_unraid() else '/mnt/cache'
            cache_root = input(f"Cache drive path [{default_cache}]: ").strip() or default_cache
            if not cache_root.endswith('/'):
                cache_root = cache_root + '/'

            print(f"\n✓ Cache root: {cache_root}")
            print("\nNow select which libraries to include and configure their paths.")
            print("For each library, you'll specify where files are actually stored.\n")

            while not valid_sections:
                for library in libraries:
                    # Get library locations
                    try:
                        locs = library.locations
                        if isinstance(locs, str):
                            locs = [locs]
                    except Exception as e:
                        print(f"\nWarning: Could not get locations for '{library.title}': {e}")
                        locs = []

                    print("-" * 60)
                    print(f"Library: {library.title}")
                    if locs:
                        print(f"  Plex path: {locs[0]}" + (f" (+{len(locs)-1} more)" if len(locs) > 1 else ""))

                    include = input("Include? [Y/n] ") or 'yes'
                    if include.lower() in ['n', 'no']:
                        print(f"  → Skipped\n")
                        continue
                    elif include.lower() in ['y', 'yes']:
                        if library.key not in valid_sections:
                            valid_sections.append(library.key)
                            selected_libraries.append(library)

                            # Collect path mappings for this library
                            if locs:
                                lib_mappings = prompt_library_path_mapping(
                                    library.title,
                                    locs,
                                    cache_root
                                )
                                path_mappings.extend(lib_mappings)
                            print()
                    else:
                        print("Please enter yes or no")

                if not valid_sections:
                    print("\n⚠ You must select at least one library. Please try again.\n")

            settings_data['valid_sections'] = valid_sections
            settings_data['path_mappings'] = path_mappings
            settings_data['cache_dir'] = cache_root  # Store cache root

            # Note: Legacy fields (plex_source, real_source, plex_library_folders,
            # nas_library_folders) are deprecated and no longer generated.
            # Path conversion now uses path_mappings exclusively.

            # Show library summary
            print("-" * 60)
            print(f"✓ Configured {len(path_mappings)} library path(s)")
            for m in path_mappings:
                status = "cacheable" if m.get('cacheable', True) else "non-cacheable"
                print(f"  • {m['name']}: {status}")


        except (BadRequest, requests.exceptions.RequestException) as e:
            print(f'Unable to connect to Plex server. Error: {e}')
        except ValueError as e:
            print(f'Token is not valid. Error: {e}')
        except TypeError as e:
            print(f'An unexpected error occurred: {e}')

    # ============================================================
    # STEP 3: CACHING BEHAVIOR
    # ============================================================
    print_step_header(3, total_steps, "CACHING BEHAVIOR")

    # OnDeck Settings
    if 'number_episodes' not in settings_data:
        print("\n--- OnDeck Settings ---")
        prompt_user_for_number('Episodes to fetch from OnDeck per show [6]: ', '6', 'number_episodes')

    if 'days_to_monitor' not in settings_data:
        prompt_user_for_number('Max age of OnDeck items in days [99]: ', '99', 'days_to_monitor')

    # Watchlist Settings
    if 'watchlist_toggle' not in settings_data:
        print("\n--- Watchlist Settings ---")
        watchlist = input('Fetch your own watchlist media? [Y/n] ') or 'yes'
        if watchlist.lower() in ['n', 'no']:
            settings_data['watchlist_toggle'] = False
            settings_data['watchlist_episodes'] = 0
        elif watchlist.lower() in ['y', 'yes']:
            settings_data['watchlist_toggle'] = True
            prompt_user_for_number('Episodes to fetch per watchlist show [3]: ', '3', 'watchlist_episodes')
        else:
            print("Please enter yes or no")

    # ============================================================
    # STEP 4: USER CONFIGURATION
    # ============================================================
    print_step_header(4, total_steps, "USER CONFIGURATION")

    while 'users_toggle' not in settings_data:
        skip_ondeck = []
        skip_watchlist = []

        fetch_all_users = input('\nFetch OnDeck media from other Plex users? [Y/n] ') or 'yes'
        if fetch_all_users.lower() not in ['y', 'yes', 'n', 'no']:
            print("Please enter yes or no")
            continue

        if fetch_all_users.lower() in ['y', 'yes']:
            settings_data['users_toggle'] = True

            # Build the full user list (local + remote)
            user_entries = []
            local_users = []
            remote_users = []
            skipped_users = []

            for user in plex.myPlexAccount().users():
                name = user.title
                user_id = getattr(user, "id", None)
                user_uuid = None
                thumb = getattr(user, "thumb", "")
                if thumb and "/users/" in thumb:
                    try:
                        user_uuid = thumb.split("/users/")[1].split("/")[0]
                    except (IndexError, AttributeError):
                        pass

                is_home = getattr(user, "home", False)
                is_restricted = getattr(user, "restricted", False)
                is_local = bool(is_home) or (is_restricted == "1" or is_restricted == 1 or is_restricted is True)

                try:
                    token = user.get_token(plex.machineIdentifier)
                except Exception:
                    skipped_users.append((name, "no server access"))
                    continue

                if token is None:
                    skipped_users.append((name, "no token"))
                    continue

                user_entry = {
                    "title": name,
                    "id": user_id,
                    "uuid": user_uuid,
                    "token": token,
                    "is_local": is_local,
                    "skip_ondeck": False,
                    "skip_watchlist": False
                }
                user_entries.append(user_entry)

                if is_local:
                    local_users.append(name)
                else:
                    remote_users.append(name)

            # Display user summary
            print(f"\nFound {len(user_entries)} accessible user(s):")
            if local_users:
                print(f"  Local/Home ({len(local_users)}): {', '.join(local_users)}")
                print("    → Can fetch OnDeck + Watchlist")
            if remote_users:
                print(f"  Remote/Friends ({len(remote_users)}): {', '.join(remote_users)}")
                print("    → OnDeck only (Watchlist via RSS)")
            if skipped_users:
                print(f"  Skipped ({len(skipped_users)}):")
                for name, reason in skipped_users:
                    print(f"    • {name} ({reason})")

            settings_data["users"] = user_entries

            # --- Skip OnDeck ---
            if user_entries:
                skip_users_choice = input('\nSkip OnDeck for specific users? [y/N] ') or 'no'
                if skip_users_choice.lower() in ['y', 'yes']:
                    for u in settings_data["users"]:
                        answer = input(f'  Skip OnDeck for {u["title"]}? [y/N] ') or 'no'
                        if answer.lower() in ['y', 'yes']:
                            u["skip_ondeck"] = True

            # --- Skip Watchlist (local users only) ---
            local_user_entries = [u for u in settings_data["users"] if u["is_local"]]
            if local_user_entries:
                print("\nLocal users can have their watchlists fetched individually.")
                for u in local_user_entries:
                    answer = input(f'  Skip watchlist for {u["title"]}? [y/N] ') or 'no'
                    if answer.lower() in ['y', 'yes']:
                        u["skip_watchlist"] = True

            # Build final skip lists
            skip_ondeck = [u["token"] for u in settings_data["users"] if u["skip_ondeck"]]
            skip_watchlist = [u["token"] for u in settings_data["users"] if u["is_local"] and u["skip_watchlist"]]

            settings_data["skip_ondeck"] = skip_ondeck
            settings_data["skip_watchlist"] = skip_watchlist

        else:
            settings_data['users_toggle'] = False
            settings_data["skip_ondeck"] = []
            settings_data["skip_watchlist"] = []

    # Remote Watchlist RSS (still part of user config)
    if 'remote_watchlist_toggle' not in settings_data:
        remote_watchlist = input('\nFetch watchlists from remote/friend users via RSS? [y/N] ') or 'no'
        if remote_watchlist.lower() in ['n', 'no']:
            settings_data['remote_watchlist_toggle'] = False
        elif remote_watchlist.lower() in ['y', 'yes']:
            settings_data['remote_watchlist_toggle'] = True
            print("\nTo get the RSS feed URL:")
            print("  1. Go to https://app.plex.tv/desktop/#!/settings/watchlist")
            print("  2. Enable 'Friends Watchlist'")
            print("  3. Copy the generated RSS URL")
            while True:
                rss_url = input('\nEnter RSS URL: ').strip()
                if not rss_url:
                    print("URL cannot be empty.")
                    continue
                try:
                    response = requests.get(rss_url, timeout=10)
                    if response.status_code == 200 and b'<Error' not in response.content:
                        print("✓ RSS feed validated")
                        settings_data['remote_watchlist_rss_url'] = rss_url
                        break
                    else:
                        print("✗ Invalid RSS feed. Please check and try again.")
                except requests.RequestException as e:
                    print(f"✗ Error accessing URL: {e}")

    # ============================================================
    # STEP 5: ADVANCED SETTINGS
    # ============================================================
    print_step_header(5, total_steps, "ADVANCED SETTINGS")

    # Watched Move
    if 'watched_move' not in settings_data:
        watched_move = input('Move watched media from cache back to array? [Y/n] ') or 'yes'
        settings_data['watched_move'] = watched_move.lower() in ['y', 'yes']

    # Cache Retention
    if 'cache_retention_hours' not in settings_data:
        print('\n--- Retention Settings ---')
        print('Cache retention: How long to keep files on cache before moving back.')
        print('(Protects against accidental unwatching or Plex glitches)')
        prompt_user_for_duration('Cache retention in hours [12]: ', '12', 'cache_retention_hours')

    # Watchlist Retention
    if 'watchlist_retention_days' not in settings_data:
        print('\nWatchlist retention: Auto-expire watchlist items after X days.')
        print('(0 = keep forever while on watchlist)')
        prompt_user_for_duration_days('Watchlist retention in days [0]: ', '0', 'watchlist_retention_days')

    # Cache Size Limit
    if 'cache_limit' not in settings_data:
        print('\n--- Cache Limits ---')
        print('Limit cache usage (e.g., 250GB, 50%, or empty for no limit)')
        cache_limit = input('Cache size limit [no limit]: ').strip()
        settings_data['cache_limit'] = cache_limit

    # Smart Cache Eviction
    if 'cache_eviction_mode' not in settings_data:
        print('\nEviction mode when cache is full:')
        print('  none  - Skip new files (default)')
        print('  smart - Evict lowest priority items')
        print('  fifo  - Evict oldest items first')
        eviction_mode = input('Eviction mode [none]: ').strip().lower() or 'none'
        if eviction_mode not in ['none', 'smart', 'fifo']:
            eviction_mode = 'none'
        settings_data['cache_eviction_mode'] = eviction_mode

        if eviction_mode in ['smart', 'fifo']:
            threshold = input('Eviction threshold % [90]: ').strip() or '90'
            # Strip % sign if user included it
            threshold = threshold.rstrip('%').strip()
            try:
                settings_data['cache_eviction_threshold_percent'] = int(threshold)
            except ValueError:
                print(f"Invalid number '{threshold}', using default 90")
                settings_data['cache_eviction_threshold_percent'] = 90

            if eviction_mode == 'smart':
                min_pri = input('Min priority to evict (0-100) [60]: ').strip() or '60'
                try:
                    settings_data['eviction_min_priority'] = int(min_pri)
                except ValueError:
                    print(f"Invalid number '{min_pri}', using default 60")
                    settings_data['eviction_min_priority'] = 60

    # Notification Level
    if 'unraid_level' not in settings_data:
        print('\n--- Unraid Notifications ---')
        print('  summary - Notify after every run with files moved (default)')
        print('  warning - Only notify on warnings and errors')
        print('  error   - Only notify on errors')
        print('  blank   - Disable notifications')
        unraid_level = input('Notification level [summary]: ').strip().lower() or 'summary'
        if unraid_level in ['disable', 'disabled', 'none', 'blank', '']:
            unraid_level = ''
        settings_data['unraid_level'] = unraid_level

    # ---------------- Cache / Array Paths (Legacy - skip if path_mappings configured) ----------------
    # If path_mappings were configured during library selection, skip this section
    if settings_data.get('path_mappings'):
        # Path mappings already configured during library selection
        pass
    elif 'cache_dir' not in settings_data:
        # Legacy path configuration for users who somehow skip library-centric setup
        cache_dir = input('\nInsert the path of your cache drive: (default: "/mnt/cache") ').replace('"', '').replace("'", '') or '/mnt/cache'
        while True:
            test_path = input('\nDo you want to test the given path? [y/N]  ') or 'no'
            if test_path.lower() in ['y', 'yes']:
                if os.path.exists(cache_dir):
                    print('The path appears to be valid. Settings saved.')
                    break
                else:
                    print('The path appears to be invalid.')
                    edit_path = input('\nDo you want to edit the path? [y/N]  ') or 'no'
                    if edit_path.lower() in ['y', 'yes']:
                        cache_dir = input('\nInsert the path of your cache drive: (default: "/mnt/cache") ').replace('"', '').replace("'", '') or '/mnt/cache'
                    elif edit_path.lower() in ['n', 'no']:
                        break
                    else:
                        print("Invalid choice. Please enter either yes or no")
            elif test_path.lower() in ['n', 'no']:
                break
            else:
                print("Invalid choice. Please enter either yes or no")
        # Ensure trailing slash for consistency
        if not cache_dir.endswith('/'):
            cache_dir = cache_dir + '/'
        settings_data['cache_dir'] = cache_dir

    # Skip real_source/nas_library_folders if path_mappings configured
    if settings_data.get('path_mappings'):
        pass
    elif 'real_source' not in settings_data:
        real_source = input('\nInsert the path where your media folders are located?: (default: "/mnt/user") ').replace('"', '').replace("'", '') or '/mnt/user'
        while True:
            test_path = input('\nDo you want to test the given path? [y/N]  ') or 'no'
            if test_path.lower() in ['y', 'yes']:
                if os.path.exists(real_source):
                    print('The path appears to be valid. Settings saved.')
                    break
                else:
                    print('The path appears to be invalid.')
                    edit_path = input('\nDo you want to edit the path? [y/N]  ') or 'no'
                    if edit_path.lower() in ['y', 'yes']:
                        real_source = input('\nInsert the path where your media folders are located?: (default: "/mnt/user") ').replace('"', '').replace("'", '') or '/mnt/user'
                    elif edit_path.lower() in ['n', 'no']:
                        break
                    else:
                        print("Invalid choice. Please enter either yes or no")
            elif test_path.lower() in ['n', 'no']:
                break
            else:
                print("Invalid choice. Please enter either yes or no")
        # Ensure trailing slash for consistency
        if not real_source.endswith('/'):
            real_source = real_source + '/'
        settings_data['real_source'] = real_source

        num_folders = len(settings_data['plex_library_folders'])
        nas_library_folder = []
        for i in range(num_folders):
            folder_name = input(f"\nEnter the corresponding NAS/Unraid library folder for the Plex mapped folder: (Default is the same as plex) '{settings_data['plex_library_folders'][i]}' ") or settings_data['plex_library_folders'][i]
            folder_name = folder_name.replace(real_source, '').strip('/')
            nas_library_folder.append(folder_name)
        settings_data['nas_library_folders'] = nas_library_folder

    # ---------------- Multi-Path Mappings (skip if already configured during library selection) ----------------
    if settings_data.get('path_mappings'):
        # Already configured during library selection - skip this section
        pass
    elif 'path_mappings' not in settings_data:
        print('\n' + '-' * 60)
        print('ADVANCED: MULTI-PATH MAPPING')
        print('-' * 60)
        print('\nMulti-path mapping allows you to configure multiple source paths')
        print('with different caching behavior. This is useful if you have:')
        print('  - Multiple Docker path mappings (e.g., /data and /nas)')
        print('  - Remote/network storage that should not be cached')
        print('  - Different cache destinations for different libraries')
        print('\nMost users can skip this - your paths above will work as-is.')

        configure_multi = input('\nWould you like to configure multiple path mappings? [y/N] ') or 'no'
        if configure_multi.lower() in ['y', 'yes']:
            configure_path_mappings(settings_data)
        else:
            # Auto-create single mapping from legacy settings for consistency
            if settings_data.get('plex_source') and settings_data.get('real_source'):
                settings_data['path_mappings'] = [{
                    'name': 'Primary',
                    'plex_path': settings_data.get('plex_source', ''),
                    'real_path': settings_data.get('real_source', ''),
                    'cache_path': settings_data.get('cache_dir', ''),
                    'cacheable': True,
                    'enabled': True
                }]
                print('Created default path mapping from your settings.')

    # Active Session Handling
    if 'exit_if_active_session' not in settings_data:
        print('\n--- Playback Handling ---')
        print('When someone is actively watching media:')
        print('  No  - Skip that file but continue processing others (default)')
        print('  Yes - Exit completely and try again next run')
        session = input('Exit if media is actively playing? [y/N] ') or 'no'
        settings_data['exit_if_active_session'] = session.lower() in ['y', 'yes']

    # Concurrent Moves
    if 'max_concurrent_moves_cache' not in settings_data:
        print('\n--- Performance ---')
        prompt_user_for_number('Concurrent file moves (array→cache) [5]: ', '5', 'max_concurrent_moves_cache')

    if 'max_concurrent_moves_array' not in settings_data:
        prompt_user_for_number('Concurrent file moves (cache→array) [2]: ', '2', 'max_concurrent_moves_array')

    # Debug/dry-run mode - default to off, users can use --dry-run flag
    if 'debug' not in settings_data:
        settings_data['debug'] = False

    # Save settings
    write_settings(settings_filename, settings_data)

    # ============================================================
    # SETUP COMPLETE - SUMMARY
    # ============================================================
    print("\n" + "=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)

    # Configuration summary
    lib_count = len(settings_data.get('path_mappings', []))
    cacheable_count = sum(1 for m in settings_data.get('path_mappings', []) if m.get('cacheable', True))
    user_count = len(settings_data.get('users', []))
    cache_limit = settings_data.get('cache_limit', '')

    print(f"\n  Libraries: {lib_count} configured ({cacheable_count} cacheable)")
    print(f"  Users: {user_count + 1} (you + {user_count} others)")
    print(f"  Cache limit: {cache_limit if cache_limit else 'No limit'}")
    print(f"  Eviction: {settings_data.get('cache_eviction_mode', 'none')}")

    print(f"\n  Config saved to: {settings_filename}")

    # Offer to run a test
    print("\n" + "-" * 60)
    run_test = input("Run a test (dry-run) now to verify configuration? [Y/n] ") or 'yes'
    if run_test.lower() in ['y', 'yes']:
        print("\nRunning: python3 plexcache_app.py --dry-run --verbose\n")
        import subprocess
        try:
            subprocess.run(['python3', 'plexcache_app.py', '--dry-run', '--verbose'], cwd=script_folder)
        except Exception as e:
            print(f"Could not run test: {e}")
            print("You can run manually: python3 plexcache_app.py --dry-run --verbose")
    else:
        print("\nYou can run PlexCache with: python3 plexcache_app.py")
        print("Or test first with: python3 plexcache_app.py --dry-run --verbose")

    print()

# ---------------- Main ----------------
check_directory_exists(script_folder)

def check_for_missing_settings(settings: dict) -> list:
    """Check for new settings that aren't in the existing config."""
    # List of settings that setup() can configure
    optional_new_settings = [
        'cache_retention_hours',
        'cache_limit',
        'unraid_level',
        'watchlist_retention_days',
        'cache_eviction_mode',
        'cache_eviction_threshold_percent',
        'eviction_min_priority',
        'path_mappings',
    ]
    missing = [s for s in optional_new_settings if s not in settings]
    return missing


def refresh_users(settings: dict) -> dict:
    """Refresh user list from Plex API, preserving skip settings.

    Re-fetches all users and updates is_local detection while keeping
    existing skip_ondeck and skip_watchlist preferences.
    """
    url = settings.get('PLEX_URL')
    token = settings.get('PLEX_TOKEN')

    if not url or not token:
        print("Error: PLEX_URL or PLEX_TOKEN not found in settings.")
        return settings

    try:
        plex = PlexServer(url, token)
    except Exception as e:
        print(f"Error connecting to Plex: {e}")
        return settings

    # Build lookup of existing skip preferences by username
    existing_users = {u.get("title"): u for u in settings.get("users", [])}

    print("\nRefreshing user list from Plex API...")
    print("-" * 60)

    new_user_entries = []
    for user in plex.myPlexAccount().users():
        name = user.title
        user_id = getattr(user, "id", None)
        # Extract uuid from thumb URL: https://plex.tv/users/{uuid}/avatar
        user_uuid = None
        thumb = getattr(user, "thumb", "")
        if thumb and "/users/" in thumb:
            try:
                user_uuid = thumb.split("/users/")[1].split("/")[0]
            except (IndexError, AttributeError):
                pass

        # Detect if home/local user
        is_home = getattr(user, "home", False)
        is_restricted = getattr(user, "restricted", False)
        # Convert to proper boolean (restricted comes as string "0" or "1")
        is_local = bool(is_home) or (is_restricted == "1" or is_restricted == 1 or is_restricted is True)

        try:
            user_token = user.get_token(plex.machineIdentifier)
        except Exception as e:
            print(f"  {name}: SKIPPED (error getting token: {e})")
            continue

        if user_token is None:
            print(f"  {name}: SKIPPED (no token available)")
            continue

        # Preserve existing skip preferences if user existed before
        existing = existing_users.get(name, {})
        skip_ondeck = existing.get("skip_ondeck", False)
        skip_watchlist = existing.get("skip_watchlist", False)
        old_is_local = existing.get("is_local", None)

        new_user_entries.append({
            "title": name,
            "id": user_id,
            "uuid": user_uuid,
            "token": user_token,
            "is_local": is_local,
            "skip_ondeck": skip_ondeck,
            "skip_watchlist": skip_watchlist
        })

        # Show what changed
        status = "home/local" if is_local else "remote/friend"
        if old_is_local is not None and old_is_local != is_local:
            print(f"  {name}: {status} (CHANGED from {'local' if old_is_local else 'remote'})")
        else:
            print(f"  {name}: {status}")

    settings["users"] = new_user_entries

    # Update skip lists
    settings["skip_ondeck"] = [u["token"] for u in new_user_entries if u["skip_ondeck"]]
    settings["skip_watchlist"] = [u["token"] for u in new_user_entries if u["is_local"] and u["skip_watchlist"]]

    print("-" * 60)
    home_count = sum(1 for u in new_user_entries if u["is_local"])
    remote_count = len(new_user_entries) - home_count
    print(f"Total: {len(new_user_entries)} users ({home_count} home/local, {remote_count} remote/friends)")

    return settings

if os.path.exists(settings_filename):
    try:
        settings_data = read_existing_settings(settings_filename)
        print("Settings file exists, loading...!\n")

        if settings_data.get('firststart'):
            print("First start unset or set to yes:\nPlease answer the following questions: \n")
            settings_data = {}
            setup()
        else:
            # Check for missing new settings
            missing_settings = check_for_missing_settings(settings_data)
            if missing_settings:
                print(f"Found {len(missing_settings)} new setting(s) available: {', '.join(missing_settings)}")
                update = input("Would you like to configure these now? [Y/n] ") or 'yes'
                if update.lower() in ['y', 'yes']:
                    print("Updating configuration with new settings...\n")
                    setup()
                else:
                    print("Skipping new settings. You can configure them later or edit the settings file directly.\n")
            else:
                print("Configuration exists and appears to be valid.")

            # Offer to re-authenticate (useful for switching from auto-detected to OAuth token)
            reauth = input("\nWould you like to re-authenticate with Plex? [y/N] ") or 'no'
            if reauth.lower() in ['y', 'yes']:
                print("\nRe-authenticating will replace your current Plex token.")
                new_token = None

                # Run OAuth flow directly (not full setup)
                print("\n" + "-" * 60)
                print("PLEX AUTHENTICATION")
                print("-" * 60)
                print("\nHow would you like to authenticate with Plex?")
                print("  1. Authenticate via Plex.tv (recommended - opens browser)")
                print("  2. Enter token manually (from browser inspection)")
                print("")

                while new_token is None:
                    auth_choice = input("Select option [1/2]: ").strip()

                    if auth_choice == '1':
                        new_token = plex_oauth_authenticate(settings_data)
                        if new_token is None:
                            print("\nOAuth authentication failed or was cancelled.")
                            retry = input("Would you like to try again or enter token manually? [retry/manual] ").strip().lower()
                            if retry == 'manual':
                                new_token = input('\nEnter your plex token: ')
                        break

                    elif auth_choice == '2':
                        print("\nTo get your token manually:")
                        print("  1. Open Plex Web App in your browser")
                        print("  2. Open Developer Tools (F12) -> Network tab")
                        print("  3. Refresh the page and look for any request to plex.tv")
                        print("  4. Find 'X-Plex-Token' in the request headers")
                        print("")
                        new_token = input('Enter your plex token: ')
                        break

                    else:
                        print("Invalid choice. Please enter 1 or 2")

                if new_token and new_token.strip():
                    # Validate the new token
                    try:
                        plex = PlexServer(settings_data['PLEX_URL'], new_token)
                        user = plex.myPlexAccount().username
                        print(f"Connection successful! Currently connected as {user}")
                        settings_data['PLEX_TOKEN'] = new_token
                        write_settings(settings_filename, settings_data)
                        print("New token saved!")
                    except Exception as e:
                        print(f"Error: Could not connect with new token: {e}")
                        print("Keeping existing token.")
                else:
                    print("No valid token provided. Keeping existing token.")

            # Always offer to refresh users (fixes is_local detection for existing configs)
            if settings_data.get('users_toggle') and settings_data.get('users'):
                user_count = len(settings_data.get('users', []))
                home_count = sum(1 for u in settings_data.get('users', []) if u.get('is_local'))
                print(f"\nCurrent user list: {user_count} users ({home_count} marked as home/local)")
                refresh = input("Would you like to refresh the user list from Plex? [y/N] ") or 'no'
                if refresh.lower() in ['y', 'yes']:
                    settings_data = refresh_users(settings_data)
                    write_settings(settings_filename, settings_data)
                    print("\nUser list refreshed and saved!")
                else:
                    print("Keeping existing user list.")

            # Offer to manage path mappings
            mapping_count = len(settings_data.get('path_mappings', []))
            if mapping_count > 0:
                print(f"\nCurrent path mappings: {mapping_count} configured")
                for m in settings_data.get('path_mappings', []):
                    status = "enabled" if m.get('enabled', True) else "disabled"
                    cacheable = "cacheable" if m.get('cacheable', True) else "non-cacheable"
                    print(f"  - {m.get('name', 'Unnamed')}: {m.get('plex_path', '')} -> {m.get('real_path', '')} ({status}, {cacheable})")
            else:
                print("\nNo multi-path mappings configured (using legacy single-path mode).")
            manage_paths = input("Would you like to manage path mappings? [y/N] ") or 'no'
            if manage_paths.lower() in ['y', 'yes']:
                settings_data = configure_path_mappings(settings_data)
                write_settings(settings_filename, settings_data)
                print("Path mappings saved!")

            print("\nYou can now run the plexcache.py script.\n")
    except json.decoder.JSONDecodeError as e:
        print(f"Settings file appears to be corrupted (JSON error: {e}). Re-initializing...\n")
        settings_data = {}
        setup()
else:
    # New setup - just start it directly without asking
    print("Welcome to PlexCache-R Setup!")
    print(f"Creating new configuration at: {settings_filename}\n")
    settings_data = {}
    setup()
