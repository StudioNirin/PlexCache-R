import json, os, requests, ntpath, posixpath, subprocess, re
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


def auto_detect_plex_token():
    """
    Auto-detect Plex token from Preferences.xml on Unraid.
    Uses optimized search: finds appdata/apps folders first, then searches within.
    Returns tuple of (token, preferences_path) or (None, None) if not found.
    """
    if not is_unraid():
        return None, None

    print("Searching for Plex installation...")

    # Step 1: Find appdata/apps folders first (fast, limited scope)
    try:
        result = subprocess.run(
            ['find', '/mnt', '-maxdepth', '4', '-type', 'd', '(', '-name', 'appdata', '-o', '-name', 'apps', ')'],
            capture_output=True, text=True, timeout=30
        )
        app_folders = [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
        print(f"Error searching for app folders: {e}")
        return None, None

    # Step 2: Search for Plex Preferences.xml within those folders
    preferences_path = None
    for folder in app_folders:
        try:
            result = subprocess.run(
                ['find', folder, '-maxdepth', '5', '-path', '*/Library/Application Support/Plex Media Server/Preferences.xml'],
                capture_output=True, text=True, timeout=30
            )
            if result.stdout.strip():
                preferences_path = result.stdout.strip().split('\n')[0]
                break
        except (subprocess.TimeoutExpired, subprocess.SubprocessError):
            continue

    # Step 3: Fallback to broader search if not found
    if not preferences_path:
        print("Not found in common locations, searching /mnt (this may take a moment)...")
        try:
            result = subprocess.run(
                ['find', '/mnt', '-maxdepth', '8', '-path', '*/Library/Application Support/Plex Media Server/Preferences.xml'],
                capture_output=True, text=True, timeout=60
            )
            if result.stdout.strip():
                preferences_path = result.stdout.strip().split('\n')[0]
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            print(f"Error during fallback search: {e}")
            return None, None

    if not preferences_path:
        print("Could not find Plex Preferences.xml")
        return None, None

    print(f"Found: {preferences_path}")

    # Step 4: Extract token from Preferences.xml
    try:
        with open(preferences_path, 'r', encoding='utf-8') as f:
            content = f.read()
        match = re.search(r'PlexOnlineToken="([^"]+)"', content)
        if match:
            return match.group(1), preferences_path
        else:
            print("Could not find PlexOnlineToken in Preferences.xml")
            return None, None
    except (IOError, OSError) as e:
        print(f"Error reading Preferences.xml: {e}")
        return None, None


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

        # Offer auto-detection on Unraid
        if is_unraid():
            while True:
                auto_detect = input('\nWould you like to auto-detect your Plex token? [Y/n] ') or 'yes'
                if auto_detect.lower() in ['y', 'yes']:
                    detected_token, plex_path = auto_detect_plex_token()
                    if detected_token:
                        while True:
                            use_token = input(f'Token detected! Use this token? [Y/n] ') or 'yes'
                            if use_token.lower() in ['y', 'yes']:
                                token = detected_token
                                break
                            elif use_token.lower() in ['n', 'no']:
                                print("Token not used. Please enter manually.")
                                break
                            else:
                                print("Invalid choice. Please enter either yes or no")
                    break
                elif auto_detect.lower() in ['n', 'no']:
                    break
                else:
                    print("Invalid choice. Please enter either yes or no")

        # Manual entry if no token yet
        if not token:
            token = input('\nEnter your plex token: ')

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
            settings_data['watchlist_cache_expiry'] = 1
        elif watchlist.lower() in ['y', 'yes']:
            settings_data['watchlist_toggle'] = True
            prompt_user_for_number('\nHow many episodes do you want fetch from your Watchlist? (default: 3) ', '3', 'watchlist_episodes')
            prompt_user_for_number('\nDefine the watchlist cache expiry duration in hours (default: 6) ', '6', 'watchlist_cache_expiry')
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
                username = getattr(user, "username", None)
                is_local = username is None
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
            settings_data['watched_cache_expiry'] = 48
        elif watched_move.lower() in ['y', 'yes']:
            settings_data['watched_move'] = True
            prompt_user_for_number('\nDefine the watched cache expiry duration in hours (default: 48) ', '48', 'watched_cache_expiry')
        else:
            print("Invalid choice. Please enter either yes or no")

    # ---------------- Cache / Array Paths ----------------
    if 'cache_dir' not in settings_data:
        cache_dir = input('\nInsert the path of your cache drive: (default: "/mnt/cache/media") ').replace('"', '').replace("'", '') or '/mnt/cache/media'
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
                        cache_dir = input('\nInsert the path of your cache drive: (default: "/mnt/cache/media") ').replace('"', '').replace("'", '') or '/mnt/cache/media'
                    elif edit_path.lower() in ['n', 'no']:
                        break
                    else:
                        print("Invalid choice. Please enter either yes or no")
            elif test_path.lower() in ['n', 'no']:
                break
            else:
                print("Invalid choice. Please enter either yes or no")
        settings_data['cache_dir'] = cache_dir

    if 'real_source' not in settings_data:
        real_source = input('\nInsert the path where your media folders are located?: (default: "/mnt/user/media") ').replace('"', '').replace("'", '') or '/mnt/user/media'
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
                        real_source = input('\nInsert the path where your media folders are located?: (default: "/mnt/user/media") ').replace('"', '').replace("'", '') or '/mnt/user/media'
                    elif edit_path.lower() in ['n', 'no']:
                        break
                    else:
                        print("Invalid choice. Please enter either yes or no")
            elif test_path.lower() in ['n', 'no']:
                break
            else:
                print("Invalid choice. Please enter either yes or no")
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

if os.path.exists(settings_filename):
    try:
        settings_data = read_existing_settings(settings_filename)
        print("Settings file exists, loading...!\n")

        if settings_data.get('firststart'):
            print("First start unset or set to yes:\nPlease answer the following questions: \n")
            settings_data = {}
            setup()
        else:
            print("Configuration exists and appears to be valid, you can now run the plexcache.py script.\n")
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
