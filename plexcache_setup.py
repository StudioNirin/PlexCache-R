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

def setup():
    settings_data['firststart'] = False

    # ---------------- Plex URL ----------------
    while 'PLEX_URL' not in settings_data:
        url = input('\nEnter your plex server address (Example: http://localhost:32400 or https://plex.mydomain.ext): ')
        if not url.strip():
            print("URL is not valid. It cannot be empty.")
            continue
        if is_valid_plex_url(url):
            settings_data['PLEX_URL'] = url
            print("Valid Plex URL")
        else:
            print("Invalid Plex URL")

    # ---------------- Plex Token ----------------
    while 'PLEX_TOKEN' not in settings_data:
        token = None

        # Offer authentication options
        print("\n" + "-" * 60)
        print("PLEX AUTHENTICATION")
        print("-" * 60)
        print("\nHow would you like to authenticate with Plex?")
        print("  1. Authenticate via Plex.tv (recommended - opens browser)")
        print("  2. Enter token manually (from browser inspection)")
        print("")

        while token is None:
            auth_choice = input("Select option [1/2]: ").strip()

            if auth_choice == '1':
                # OAuth PIN-based authentication
                token = plex_oauth_authenticate(settings_data)
                if token is None:
                    print("\nOAuth authentication failed or was cancelled.")
                    retry = input("Would you like to try again or enter token manually? [retry/manual] ").strip().lower()
                    if retry == 'manual':
                        token = input('\nEnter your plex token: ')
                    # else loop continues for retry
                break

            elif auth_choice == '2':
                # Manual token entry
                print("\nTo get your token manually:")
                print("  1. Open Plex Web App in your browser")
                print("  2. Open Developer Tools (F12) -> Network tab")
                print("  3. Refresh the page and look for any request to plex.tv")
                print("  4. Find 'X-Plex-Token' in the request headers")
                print("")
                token = input('Enter your plex token: ')
                break

            else:
                print("Invalid choice. Please enter 1 or 2")

        if not token.strip():
            print("Token is not valid. It cannot be empty.")
            continue
        try:
            plex = PlexServer(settings_data['PLEX_URL'], token)
            user = plex.myPlexAccount().username
            print(f"Connection successful! Currently connected as {user}")
            libraries = plex.library.sections()
            settings_data['PLEX_TOKEN'] = token

            operating_system = plex.platform
            print(f"Plex is running on {operating_system}")

            valid_sections = []
            selected_libraries = []
            plex_library_folders = []

            # Step 1: Collect library selections from user
            while not valid_sections:
                for library in libraries:
                    print(f"\nYour plex library name: {library.title}")
                    include = input("Do you want to include this library? [Y/n]  ") or 'yes'
                    if include.lower() in ['n', 'no']:
                        continue
                    elif include.lower() in ['y', 'yes']:
                        if library.key not in valid_sections:
                            valid_sections.append(library.key)
                            selected_libraries.append(library)
                    else:
                        print("Invalid choice. Please enter either yes or no")

                if not valid_sections:
                    print("You must select at least one library to include. Please try again.")

            settings_data['valid_sections'] = valid_sections

            # Step 2: Compute plex_source from ONLY selected libraries (fixes Issue #12)
            if 'plex_source' not in settings_data:
                selected_locations = []
                for lib in selected_libraries:
                    try:
                        locs = lib.locations
                        if isinstance(locs, list):
                            selected_locations.extend(locs)
                        elif isinstance(locs, str):
                            selected_locations.append(locs)
                    except Exception as e:
                        print(f"Warning: Could not get locations for library '{lib.title}': {e}")
                        continue

                plex_source = find_common_root(selected_locations)

                # Warn user if plex_source is just "/" and allow manual override
                if plex_source == "/":
                    print(f"\nWarning: The computed plex_source is '/' (root).")
                    print("This usually happens when your selected libraries have different base paths.")
                    print(f"Selected library paths: {selected_locations}")
                    print("\nUsing '/' as plex_source will likely cause path issues.")

                    while True:
                        manual_source = input("\nEnter the correct plex_source path (e.g., '/data') or press Enter to keep '/': ").strip()
                        if manual_source == "":
                            print("Keeping plex_source as '/' - please verify your settings work correctly.")
                            break
                        elif manual_source.startswith("/"):
                            plex_source = manual_source.rstrip("/")
                            print(f"plex_source set to: {plex_source}")
                            break
                        else:
                            print("Path must start with '/'")

                # Ensure trailing slash for consistency
                if not plex_source.endswith('/'):
                    plex_source = plex_source + '/'
                print(f"\nPlex source path set to: {plex_source}")
                settings_data['plex_source'] = plex_source

            # Step 3: Compute relative library folders from selected libraries
            for lib in selected_libraries:
                for location in lib.locations:
                    rel = os.path.relpath(location, settings_data['plex_source']).strip('/')
                    rel = rel.replace('\\', '/')
                    if rel not in plex_library_folders:
                        plex_library_folders.append(rel)

            settings_data['plex_library_folders'] = plex_library_folders


        except (BadRequest, requests.exceptions.RequestException) as e:
            print(f'Unable to connect to Plex server. Please check your token. Error: {e}')
        except ValueError as e:
            print(f'Token is not valid. Error: {e}')
        except TypeError as e:
            print(f'An unexpected error occurred: {e}')

    # ---------------- OnDeck Settings ----------------
    while 'number_episodes' not in settings_data:
        prompt_user_for_number('\nHow many episodes (digit) do you want fetch from your OnDeck? (default: 6) ', '6', 'number_episodes')

    while 'days_to_monitor' not in settings_data:
        prompt_user_for_number('\nMaximum age of the media onDeck to be fetched? (default: 99) ', '99', 'days_to_monitor')

    # ----------------Primary User Watchlist Settings ----------------
    while 'watchlist_toggle' not in settings_data:
        watchlist = input('\nDo you want to fetch your own watchlist media? [y/N] ') or 'no'
        if watchlist.lower() in ['n', 'no']:
            settings_data['watchlist_toggle'] = False
            settings_data['watchlist_episodes'] = 0
        elif watchlist.lower() in ['y', 'yes']:
            settings_data['watchlist_toggle'] = True
            prompt_user_for_number('\nHow many episodes do you want fetch from your Watchlist? (default: 3) ', '3', 'watchlist_episodes')
        else:
            print("Invalid choice. Please enter either yes or no")

    # ---------------- Users / Skip Lists ----------------
    while 'users_toggle' not in settings_data:
        skip_ondeck = []
        skip_watchlist = []

        fetch_all_users = input('\nDo you want to fetch onDeck media from other users?  [Y/n] ') or 'yes'
        if fetch_all_users.lower() not in ['y', 'yes', 'n', 'no']:
            print("Invalid choice. Please enter either yes or no")
            continue

        if fetch_all_users.lower() in ['y', 'yes']:
            settings_data['users_toggle'] = True

            # Build the full user list (local + remote)
            user_entries = []
            for user in plex.myPlexAccount().users():
                name = user.title
                user_id = getattr(user, "id", None)
                # Check if user is a home/managed user (not a remote friend)
                # home=True means they're part of Plex Home
                # restricted="1" means they're a managed user (no separate plex.tv account)
                # Note: restricted comes as string "0" or "1" from API, not boolean
                is_home = getattr(user, "home", False)
                is_restricted = getattr(user, "restricted", False)
                # Convert to proper boolean (restricted is a string from the API)
                is_local = bool(is_home) or (is_restricted == "1" or is_restricted == 1 or is_restricted is True)
                try:
                    token = user.get_token(plex.machineIdentifier)
                except Exception as e:
                    print(f"\nSkipping user '{name}' (error getting token: {e})")
                    continue

                if token is None:
                    print(f"\nSkipping user '{name}' (no token available).")
                    continue

                user_entries.append({
                    "title": name,
                    "id": user_id,
                    "token": token,
                    "is_local": is_local,
                    "skip_ondeck": False,
                    "skip_watchlist": False
                })

            settings_data["users"] = user_entries

            # --- Skip OnDeck ---
            skip_users_choice = input('\nWould you like to skip onDeck for some of the users? [y/N] ') or 'no'
            if skip_users_choice.lower() in ['y', 'yes']:
                for u in settings_data["users"]:
                    while True:
                        answer_ondeck = input(f'\nDo you want to skip onDeck for this user? {u["title"]} [y/N] ') or 'no'
                        if answer_ondeck.lower() not in ['y', 'yes', 'n', 'no']:
                            print("Invalid choice. Please enter either yes or no")
                            continue
                        if answer_ondeck.lower() in ['y', 'yes']:
                            u["skip_ondeck"] = True
                        break

            # --- Skip Watchlist (local users only) ---
            for u in settings_data["users"]:
                if u["is_local"]:
                    while True:
                        answer_watchlist = input(f'\nDo you want to skip watchlist for this local user? {u["title"]} [y/N] ') or 'no'
                        if answer_watchlist.lower() not in ['y', 'yes', 'n', 'no']:
                            print("Invalid choice. Please enter either yes or no")
                            continue
                        if answer_watchlist.lower() in ['y', 'yes']:
                            u["skip_watchlist"] = True
                        break

            # Build final skip lists
            skip_ondeck = [u["token"] for u in settings_data["users"] if u["skip_ondeck"]]
            skip_watchlist = [u["token"] for u in settings_data["users"] if u["is_local"] and u["skip_watchlist"]]

            settings_data["skip_ondeck"] = skip_ondeck
            settings_data["skip_watchlist"] = skip_watchlist

        else:
            settings_data['users_toggle'] = False
            settings_data["skip_ondeck"] = []
            settings_data["skip_watchlist"] = []

    # ---------------- Remote Watchlist RSS ----------------
    while 'remote_watchlist_toggle' not in settings_data:
        remote_watchlist = input('\nWould you like to fetch Watchlist media from ALL remote Plex users? [y/N] ') or 'no'
        if remote_watchlist.lower() in ['n', 'no']:
            settings_data['remote_watchlist_toggle'] = False
        elif remote_watchlist.lower() in ['y', 'yes']:
            settings_data['remote_watchlist_toggle'] = True
            while True:
                rss_url = input('\nGo to https://app.plex.tv/desktop/#!/settings/watchlist and activate the Friends\' Watchlist.\nEnter the generated URL here: ').strip()
                if not rss_url:
                    print("URL is not valid. It cannot be empty.")
                    continue
                try:
                    response = requests.get(rss_url, timeout=10)
                    if response.status_code == 200 and b'<Error' not in response.content:
                        print("RSS feed URL validated successfully.")
                        settings_data['remote_watchlist_rss_url'] = rss_url
                        break
                    else:
                        print("Invalid RSS feed URL or feed not accessible. Please check and try again.")
                except requests.RequestException as e:
                    print(f"Error accessing the URL: {e}")
        else:
            print("Invalid choice. Please enter either yes or no")

    # ---------------- Watched Move ----------------
    while 'watched_move' not in settings_data:
        watched_move = input('\nDo you want to move watched media from the cache back to the array? [y/N] ') or 'no'
        if watched_move.lower() in ['n', 'no']:
            settings_data['watched_move'] = False
        elif watched_move.lower() in ['y', 'yes']:
            settings_data['watched_move'] = True
        else:
            print("Invalid choice. Please enter either yes or no")

    # ---------------- Cache Retention Period ----------------
    if 'cache_retention_hours' not in settings_data:
        print('\nCache retention prevents files from being moved back to array immediately after caching.')
        print('This protects against accidental unwatching, watchlist removal, or Plex glitches.')
        print('Applies to all cached files (OnDeck, Watchlist, etc.).')
        print('Enter a number in hours (default) or use "d" suffix for days (e.g., 12, 12h, 2d)')
        prompt_user_for_duration('Cache retention period (default: 12h): ', '12', 'cache_retention_hours')

    # ---------------- Watchlist Retention Period ----------------
    if 'watchlist_retention_days' not in settings_data:
        print('\nWatchlist retention automatically expires cached files after a set number of days.')
        print('Files are removed from cache X days after being added to watchlist, even if still on watchlist.')
        print('This prevents watchlist items from sitting on cache indefinitely.')
        print('Multi-user: If another user adds the same item, the retention timer resets.')
        print('Enter 0 to disable (files stay cached as long as they are on any watchlist).')
        print('Enter a number in days (default) or use "h" suffix for hours (e.g., 30, 30d, 12h)')
        prompt_user_for_duration_days('Watchlist retention (0 to disable, default: 0): ', '0', 'watchlist_retention_days')

    # ---------------- Cache Size Limit ----------------
    if 'cache_limit' not in settings_data:
        print('\nSet a maximum amount of space PlexCache can use on your cache drive.')
        print('This prevents your cache from being overwhelmed by large watchlists.')
        print('Supported formats:')
        print('  - "250GB" or "250" (defaults to GB)')
        print('  - "500MB"')
        print('  - "1TB"')
        print('  - "50%" (percentage of total cache drive size)')
        print('  - Leave empty for no limit')
        cache_limit = input('\nEnter cache size limit (e.g., 250GB, 50%, or leave empty for no limit): ').strip()
        settings_data['cache_limit'] = cache_limit
        if cache_limit:
            print(f'Cache limit set to: {cache_limit}')
        else:
            print('No cache limit set.')

    # ---------------- Notification Level ----------------
    if 'unraid_level' not in settings_data:
        print('\nNotification level controls when you receive Unraid notifications from PlexCache.')
        print('Options:')
        print('  - "summary" : Notify on every run with a summary (default)')
        print('  - "error"   : Only notify when errors occur')
        print('  - "warning" : Notify on warnings and errors')
        print('  - ""        : Disable notifications entirely')
        while True:
            unraid_level = input('\nEnter notification level (summary/error/warning/blank to disable) [default: summary]: ').strip().lower()
            if unraid_level == '':
                # User pressed enter - use default
                unraid_level = 'summary'
                break
            elif unraid_level in ['summary', 'error', 'warning', 'disable', 'disabled', 'none']:
                if unraid_level in ['disable', 'disabled', 'none']:
                    unraid_level = ''
                break
            else:
                print('Invalid option. Please enter: summary, error, warning, or leave blank.')
        settings_data['unraid_level'] = unraid_level
        if unraid_level:
            print(f'Notification level set to: {unraid_level}')
        else:
            print('Notifications disabled.')

    # ---------------- Cache / Array Paths ----------------
    if 'cache_dir' not in settings_data:
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

    if 'real_source' not in settings_data:
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

    # ---------------- Active Session ----------------
    while 'exit_if_active_session' not in settings_data:
        session = input('\nIf there is an active session in plex, do you want to exit the script (Yes) or just skip the playing media (No)? [y/N] ') or 'no'
        if session.lower() in ['n', 'no']:
            settings_data['exit_if_active_session'] = False
        elif session.lower() in ['y', 'yes']:
            settings_data['exit_if_active_session'] = True
        else:
            print("Invalid choice. Please enter either yes or no")

    # ---------------- Concurrent Moves ----------------
    if 'max_concurrent_moves_cache' not in settings_data:
        prompt_user_for_number('\nHow many files do you want to move from the array to the cache at the same time? (default: 5) ', '5', 'max_concurrent_moves_cache')

    if 'max_concurrent_moves_array' not in settings_data:
        prompt_user_for_number('\nHow many files do you want to move from the cache to the array at the same time? (default: 2) ', '2', 'max_concurrent_moves_array')

    # ---------------- Debug ----------------
    while 'debug' not in settings_data:
        debug = input('\nDo you want to debug the script? No data will actually be moved. [y/N] ') or 'no'
        if debug.lower() in ['n', 'no']:
            settings_data['debug'] = False
        elif debug.lower() in ['y', 'yes']:
            settings_data['debug'] = True
        else:
            print("Invalid choice. Please enter either yes or no")

    write_settings(settings_filename, settings_data)
    print("Setup complete! You can now run the plexcache.py script.\n")

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

            print("\nYou can now run the plexcache.py script.\n")
    except json.decoder.JSONDecodeError as e:
        print(f"Settings file appears to be corrupted (JSON error: {e}). Re-initializing...\n")
        settings_data = {}
        setup()
else:
    print(f"Settings file {settings_filename} doesn't exist, please check the path:\n")
    while True:
        creation = input("\nIf the path is correct, do you want to create the file? [Y/n] ") or 'yes'
        if creation.lower() in ['y', 'yes']:
            print("Starting setup...\n")
            settings_data = {}
            setup()
            break
        elif creation.lower() in ['n', 'no']:
            exit("Exiting as requested, setting file not created.")
        else:
            print("Invalid choice. Please enter either 'yes' or 'no'")
