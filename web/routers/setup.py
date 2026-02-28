"""Setup wizard routes for first-run configuration"""

import json
import uuid
import time
import requests
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from web.config import templates, PROJECT_ROOT, PLEXCACHE_PRODUCT_VERSION
from web.services import get_settings_service

router = APIRouter()

# PlexCache-D OAuth identifiers
PLEXCACHE_PRODUCT_NAME = 'PlexCache-D'

# Store OAuth state in memory (cleared on restart, which is fine for setup)
_oauth_state: Dict[str, Any] = {}

# Store setup wizard state in memory until completion
# This prevents writing partial/broken config if setup is abandoned
_setup_state: Dict[str, Any] = {}


def get_setup_state() -> Dict[str, Any]:
    """Get the current in-memory setup state"""
    return _setup_state


def update_setup_state(updates: Dict[str, Any]) -> None:
    """Update the in-memory setup state"""
    _setup_state.update(updates)


def clear_setup_state() -> None:
    """Clear the in-memory setup state"""
    _setup_state.clear()


def is_setup_complete() -> bool:
    """Check if initial setup has been completed"""
    settings_service = get_settings_service()
    settings = settings_service.get_all()

    # Setup is complete if we have Plex URL and token configured
    plex_url = settings.get("PLEX_URL", "")
    plex_token = settings.get("PLEX_TOKEN", "")

    return bool(plex_url and plex_token)


def get_or_create_client_id(settings: Dict) -> str:
    """Get existing client ID or create a new one"""
    # Check setup state first, then saved settings
    if "plexcache_client_id" in _setup_state:
        return _setup_state["plexcache_client_id"]
    if "plexcache_client_id" in settings:
        return settings["plexcache_client_id"]
    return str(uuid.uuid4())


@router.get("/setup", response_class=HTMLResponse)
def setup_wizard(request: Request, step: int = 1):
    """Main setup wizard page"""
    settings_service = get_settings_service()
    saved_settings = settings_service.get_all()

    # Merge saved settings with in-memory setup state (setup state takes priority)
    settings = {**saved_settings, **_setup_state}

    # Get any existing values for pre-population
    context = {
        "request": request,
        "page_title": "Setup",
        "step": step,
        "total_steps": 6,
        "settings": settings,
    }

    # Add step-specific data
    if step == 2:
        # Plex connection - check for existing values
        context["plex_url"] = settings.get("PLEX_URL", "")
        context["plex_token"] = settings.get("PLEX_TOKEN", "")

    elif step == 3:
        # Libraries - use cached data if available, otherwise fetch from Plex
        plex_url = settings.get("PLEX_URL", "")
        plex_token = settings.get("PLEX_TOKEN", "")
        if "_cached_libraries" in _setup_state:
            # Use cached libraries for faster back-navigation
            context["libraries"] = _setup_state["_cached_libraries"]
        elif plex_url and plex_token:
            # Fetch and cache libraries
            libraries = settings_service.get_plex_libraries(
                plex_url=plex_url, plex_token=plex_token
            )
            _setup_state["_cached_libraries"] = libraries
            context["libraries"] = libraries
        context["valid_sections"] = settings.get("valid_sections", [])
        context["library_cacheable"] = settings.get("library_cacheable", {})
        context["cache_dir"] = settings.get("cache_dir", "/mnt/cache")
        context["path_mappings"] = settings.get("path_mappings", [])

    elif step == 4:
        # Users - use cached data if available, otherwise fetch from Plex
        plex_url = settings.get("PLEX_URL", "")
        plex_token = settings.get("PLEX_TOKEN", "")
        if "_cached_users" in _setup_state:
            # Use cached users for faster back-navigation
            context["users"] = _setup_state["_cached_users"]
        elif plex_url and plex_token:
            # Fetch and cache users
            users = settings_service.get_plex_users(
                plex_url=plex_url, plex_token=plex_token
            )
            _setup_state["_cached_users"] = users
            context["users"] = users
        context["users_toggle"] = settings.get("users_toggle", True)
        context["existing_users"] = settings.get("users", [])
        context["remote_watchlist_rss_url"] = settings.get("remote_watchlist_rss_url", "")

    elif step == 5:
        # Behavior & Schedule
        context["number_episodes"] = settings.get("number_episodes", 6)
        context["days_to_monitor"] = settings.get("days_to_monitor", 183)
        context["watchlist_toggle"] = settings.get("watchlist_toggle", True)
        context["watchlist_episodes"] = settings.get("watchlist_episodes", 3)
        context["watchlist_retention_days"] = settings.get("watchlist_retention_days", 14)
        context["cache_retention_hours"] = settings.get("cache_retention_hours", 12)
        context["watched_move"] = settings.get("watched_move", True)
        context["cache_limit"] = settings.get("cache_limit", "")

    elif step == 6:
        # Summary - gather all configured settings from setup state
        context["plex_url"] = settings.get("PLEX_URL", "")
        context["libraries_count"] = len(settings.get("valid_sections", []))
        context["path_mappings_count"] = len(settings.get("path_mappings", []))
        context["users_count"] = len(settings.get("users", [])) + 1  # +1 for main account

    return templates.TemplateResponse(f"setup/step{step}.html", context)


@router.post("/setup/step1", response_class=HTMLResponse)
def setup_step1_post(request: Request):
    """Handle step 1 (Welcome) - just move to next step"""
    return RedirectResponse(url="/setup?step=2", status_code=303)


@router.post("/setup/step2", response_class=HTMLResponse)
def setup_step2_post(
    request: Request,
    plex_url: str = Form(...),
    plex_token: str = Form(...)
):
    """Handle step 2 (Plex Connection) form submission"""
    # Validate connection before storing in memory
    try:
        from plexapi.server import PlexServer
        plex = PlexServer(plex_url, plex_token, timeout=10)
        server_name = plex.friendlyName
    except Exception as e:
        # Return to step 2 with error
        return templates.TemplateResponse(
            "setup/step2.html",
            {
                "request": request,
                "page_title": "Setup",
                "step": 2,
                "total_steps": 6,
                "plex_url": plex_url,
                "plex_token": plex_token,
                "error": f"Could not connect to Plex: {str(e)}"
            }
        )

    # Store in memory (not to disk yet)
    client_id = get_or_create_client_id({})
    update_setup_state({
        "PLEX_URL": plex_url,
        "PLEX_TOKEN": plex_token,
        "plexcache_client_id": client_id
    })

    # Invalidate caches to force refresh with new credentials
    settings_service = get_settings_service()
    settings_service.invalidate_plex_cache()

    # Clear cached libraries/users since credentials changed
    if "_cached_libraries" in _setup_state:
        del _setup_state["_cached_libraries"]
    if "_cached_users" in _setup_state:
        del _setup_state["_cached_users"]
    if "_prefetched_user_tokens" in _setup_state:
        del _setup_state["_prefetched_user_tokens"]

    return RedirectResponse(url="/setup?step=3", status_code=303)


@router.post("/setup/step3", response_class=HTMLResponse)
async def setup_step3_post(request: Request):
    """Handle step 3 (Libraries & Paths) form submission"""
    form_data = await request.form()

    # Get cache_dir from form, default to standard Docker mount point
    cache_dir = form_data.get("cache_dir", "").strip() or "/mnt/cache"

    # Get selected libraries and their cacheable status
    selected_libraries = form_data.getlist("libraries")
    valid_sections = [int(lib_id) for lib_id in selected_libraries]

    # Get cacheable status for each library
    library_cacheable = {}
    for lib_id in selected_libraries:
        # Checkbox is "on" if checked, absent if unchecked
        is_cacheable = form_data.get(f"library_cacheable_{lib_id}") == "on"
        library_cacheable[lib_id] = is_cacheable

    # Get path mappings from form (manual entries)
    path_mappings = []
    mapping_index = 0
    while f"mapping_name_{mapping_index}" in form_data:
        cache_path = form_data.get(f"mapping_cache_path_{mapping_index}", "")
        host_cache_path = form_data.get(f"mapping_host_cache_path_{mapping_index}", "")
        mapping = {
            "name": form_data.get(f"mapping_name_{mapping_index}", ""),
            "plex_path": form_data.get(f"mapping_plex_path_{mapping_index}", ""),
            "real_path": form_data.get(f"mapping_real_path_{mapping_index}", ""),
            "cache_path": cache_path,
            "host_cache_path": host_cache_path if host_cache_path else cache_path,  # Default to cache_path if not set
            "cacheable": form_data.get(f"mapping_cacheable_{mapping_index}") == "on",
            "enabled": True
        }
        if mapping["name"] and mapping["plex_path"] and mapping["real_path"]:
            path_mappings.append(mapping)
        mapping_index += 1

    # If no manual path mappings, auto-generate from library locations
    if not path_mappings:
        cached_libraries = _setup_state.get("_cached_libraries", [])
        cache_dir_normalized = cache_dir.rstrip('/') if cache_dir else "/mnt/cache"

        for lib in cached_libraries:
            lib_id = lib.get("id")
            if lib_id not in valid_sections:
                continue

            lib_title = lib.get("title", "Unknown")
            locations = lib.get("locations", [])
            is_cacheable = library_cacheable.get(str(lib_id), True)

            for i, plex_path in enumerate(locations):
                # Normalize plex_path with trailing slash
                plex_path_normalized = plex_path if plex_path.endswith('/') else plex_path + '/'

                # Generate mapping name (add index if multiple locations)
                if len(locations) > 1:
                    mapping_name = f"{lib_title} ({i + 1})"
                else:
                    mapping_name = lib_title

                # Suggest real_path based on common Docker path patterns
                # Common patterns: /data/ -> /mnt/user/, /media/ -> /mnt/user/
                real_path = plex_path_normalized
                for docker_prefix, host_prefix in [('/data/', '/mnt/user/'), ('/media/', '/mnt/user/')]:
                    if plex_path_normalized.startswith(docker_prefix):
                        real_path = plex_path_normalized.replace(docker_prefix, host_prefix, 1)
                        break

                # Generate cache_path from library folder name
                lib_folder = plex_path_normalized.rstrip('/').split('/')[-1]
                cache_path = f"{cache_dir_normalized}/{lib_folder}/" if is_cacheable else None
                # host_cache_path defaults to same as cache_path (user can override in settings)
                host_cache_path = cache_path

                path_mappings.append({
                    "name": mapping_name,
                    "plex_path": plex_path_normalized,
                    "real_path": real_path,
                    "cache_path": cache_path,
                    "host_cache_path": host_cache_path,
                    "cacheable": is_cacheable,
                    "enabled": True
                })

    # Store in memory (not to disk yet)
    update_setup_state({
        "valid_sections": valid_sections,
        "library_cacheable": library_cacheable,
        "cache_dir": cache_dir,
        "path_mappings": path_mappings
    })

    return RedirectResponse(url="/setup?step=4", status_code=303)


@router.post("/setup/step4", response_class=HTMLResponse)
async def setup_step4_post(request: Request):
    """Handle step 4 (Users) form submission"""
    form_data = await request.form()

    users_toggle = form_data.get("users_toggle") == "on"

    # Build user list - prefer prefetched tokens for speed
    users = []
    if users_toggle:
        prefetched_tokens = _setup_state.get("_prefetched_user_tokens", {})

        if prefetched_tokens:
            # Use prefetched tokens (fast path)
            for username, user_data in prefetched_tokens.items():
                # Check if user is selected
                if form_data.get(f"user_{username}") != "on":
                    continue

                skip_ondeck = form_data.get(f"skip_ondeck_{username}") == "on"
                skip_watchlist = form_data.get(f"skip_watchlist_{username}") == "on"

                users.append({
                    "title": user_data["title"],
                    "id": user_data["id"],
                    "uuid": user_data["uuid"],
                    "token": user_data["token"],
                    "is_local": user_data["is_local"],
                    "skip_ondeck": skip_ondeck,
                    "skip_watchlist": skip_watchlist
                })
        else:
            # Fallback: fetch tokens now (slow path - prefetch didn't complete)
            plex_url = _setup_state.get("PLEX_URL", "")
            plex_token = _setup_state.get("PLEX_TOKEN", "")

            if plex_url and plex_token:
                try:
                    from plexapi.server import PlexServer
                    plex = PlexServer(plex_url, plex_token, timeout=10)

                    for plex_user in plex.myPlexAccount().users():
                        username = plex_user.title

                        # Check if user is selected
                        if form_data.get(f"user_{username}") != "on":
                            continue

                        # Get user token
                        try:
                            token = plex_user.get_token(plex.machineIdentifier)
                            if token is None:
                                continue
                        except Exception:
                            continue

                        # Get user ID and UUID
                        user_id = getattr(plex_user, "id", None)
                        user_uuid = None
                        thumb = getattr(plex_user, "thumb", "")
                        if thumb and "/users/" in thumb:
                            try:
                                user_uuid = thumb.split("/users/")[1].split("/")[0]
                            except (IndexError, AttributeError):
                                pass

                        is_home = getattr(plex_user, "home", False)
                        skip_ondeck = form_data.get(f"skip_ondeck_{username}") == "on"
                        skip_watchlist = form_data.get(f"skip_watchlist_{username}") == "on"

                        users.append({
                            "title": username,
                            "id": user_id,
                            "uuid": user_uuid,
                            "token": token,
                            "is_local": bool(is_home),
                            "skip_ondeck": skip_ondeck,
                            "skip_watchlist": skip_watchlist
                        })
                except Exception:
                    pass

    # Build skip lists
    skip_ondeck = [u["token"] for u in users if u.get("skip_ondeck")]
    skip_watchlist = [u["token"] for u in users if u.get("is_local") and u.get("skip_watchlist")]

    # Get RSS URL for remote watchlists (optional)
    remote_watchlist_rss_url = form_data.get("remote_watchlist_rss_url", "").strip()
    remote_watchlist_toggle = bool(remote_watchlist_rss_url)

    # Store in memory (not to disk yet)
    update_setup_state({
        "users_toggle": users_toggle,
        "users": users,
        "skip_ondeck": skip_ondeck,
        "skip_watchlist": skip_watchlist,
        "remote_watchlist_toggle": remote_watchlist_toggle,
        "remote_watchlist_rss_url": remote_watchlist_rss_url
    })

    # Clean up prefetched tokens - no longer needed
    if "_prefetched_user_tokens" in _setup_state:
        del _setup_state["_prefetched_user_tokens"]

    return RedirectResponse(url="/setup?step=5", status_code=303)


@router.post("/setup/step5", response_class=HTMLResponse)
async def setup_step5_post(request: Request):
    """Handle step 5 (Behavior & Schedule) form submission"""
    form_data = await request.form()

    # Parse form values (checkboxes come as "on" or are absent)
    watchlist_toggle = form_data.get("watchlist_toggle") == "on"
    watched_move = form_data.get("watched_move") == "on"

    # Parse numeric values with defaults
    number_episodes = int(form_data.get("number_episodes") or 6)
    days_to_monitor = int(form_data.get("days_to_monitor") or 183)
    watchlist_episodes = int(form_data.get("watchlist_episodes") or 3)
    watchlist_retention_days = int(form_data.get("watchlist_retention_days") or 0)
    cache_retention_hours = int(form_data.get("cache_retention_hours") or 12)
    cache_limit = form_data.get("cache_limit", "").strip()

    # Store in memory (not to disk yet)
    update_setup_state({
        "number_episodes": number_episodes,
        "days_to_monitor": days_to_monitor,
        "watchlist_toggle": watchlist_toggle,
        "watchlist_episodes": watchlist_episodes,
        "watchlist_retention_days": watchlist_retention_days,
        "cache_retention_hours": cache_retention_hours,
        "watched_move": watched_move,
        "cache_limit": cache_limit,
        # Set other defaults
        "firststart": False,
        "debug": False,
        "max_concurrent_moves_cache": 5,
        "max_concurrent_moves_array": 2,
        "exit_if_active_session": False,
        "cache_eviction_mode": "none",
        "notification_type": "system"
    })

    return RedirectResponse(url="/setup?step=6", status_code=303)


@router.post("/setup/complete", response_class=HTMLResponse)
def setup_complete_post(request: Request):
    """Handle setup completion - write all settings to disk"""
    settings_service = get_settings_service()

    # Get existing settings and merge with setup state
    settings = settings_service.get_all()
    settings.update(_setup_state)

    # Mark setup as complete
    settings["firststart"] = False

    # Write everything to disk
    settings_service._save_raw(settings)

    # Clear the in-memory setup state
    clear_setup_state()

    # Redirect to dashboard
    return RedirectResponse(url="/", status_code=303)


# OAuth endpoints for Plex authentication
@router.post("/setup/oauth/start")
def oauth_start(request: Request):
    """Start Plex OAuth flow - returns auth URL"""
    settings_service = get_settings_service()
    settings = settings_service.get_all()

    client_id = get_or_create_client_id(settings)

    # Store client ID in memory
    update_setup_state({"plexcache_client_id": client_id})

    headers = {
        'Accept': 'application/json',
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

    try:
        response = requests.post(
            'https://plex.tv/api/v2/pins',
            headers=headers,
            data={'strong': 'true'},
            timeout=30
        )
        response.raise_for_status()
        pin_data = response.json()
    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})

    pin_id = pin_data.get('id')
    pin_code = pin_data.get('code')

    if not pin_id or not pin_code:
        return JSONResponse({"success": False, "error": "Invalid response from Plex"})

    # Store pin for polling
    _oauth_state[client_id] = {
        "pin_id": pin_id,
        "pin_code": pin_code,
        "created": time.time()
    }

    auth_url = f"https://app.plex.tv/auth#?clientID={client_id}&code={pin_code}&context%5Bdevice%5D%5Bproduct%5D={PLEXCACHE_PRODUCT_NAME}"

    return JSONResponse({
        "success": True,
        "auth_url": auth_url,
        "client_id": client_id
    })


@router.get("/setup/oauth/poll")
def oauth_poll(client_id: str = Query(...)):
    """Poll for OAuth completion"""
    if client_id not in _oauth_state:
        return JSONResponse({"success": False, "error": "Invalid client ID"})

    state = _oauth_state[client_id]
    pin_id = state["pin_id"]

    headers = {
        'Accept': 'application/json',
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

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
            # Clean up state
            del _oauth_state[client_id]
            return JSONResponse({
                "success": True,
                "complete": True,
                "token": auth_token
            })

        return JSONResponse({
            "success": True,
            "complete": False
        })

    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})


@router.post("/setup/test-connection")
def test_plex_connection(
    plex_url: str = Form(...),
    plex_token: str = Form(...)
):
    """Test Plex connection with provided credentials"""
    try:
        from plexapi.server import PlexServer
        plex = PlexServer(plex_url, plex_token, timeout=10)

        return JSONResponse({
            "success": True,
            "server_name": plex.friendlyName,
            "version": plex.version
        })
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": str(e)
        })


@router.post("/setup/prefetch-users")
def prefetch_users(
    plex_url: str = Form(...),
    plex_token: str = Form(...)
):
    """Prefetch Plex users in background to speed up step 4"""
    settings_service = get_settings_service()

    try:
        from plexapi.server import PlexServer
        plex = PlexServer(plex_url, plex_token, timeout=15)

        users = []
        try:
            account = plex.myPlexAccount()
            for user in account.users():
                # Try to get token for this user
                try:
                    token = user.get_token(plex.machineIdentifier)
                    has_access = token is not None
                except Exception:
                    has_access = False

                users.append({
                    "title": user.title,
                    "id": getattr(user, "id", None),
                    "thumb": getattr(user, "thumb", ""),
                    "is_home": getattr(user, "home", False),
                    "has_access": has_access
                })
        except Exception:
            pass

        # Cache the prefetched users
        settings_service._prefetched_users = users

        return JSONResponse({"success": True, "count": len(users)})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})


@router.post("/setup/prefetch-tokens")
def prefetch_tokens():
    """Prefetch user tokens in background while user is on step 4.

    This speeds up the step 4 -> step 5 transition by fetching all user
    tokens in parallel while the user is selecting which users to enable.
    """
    # Get Plex credentials from setup state
    plex_url = _setup_state.get("PLEX_URL", "")
    plex_token = _setup_state.get("PLEX_TOKEN", "")

    if not plex_url or not plex_token:
        return JSONResponse({"success": False, "error": "No Plex credentials in setup state"})

    try:
        from plexapi.server import PlexServer
        plex = PlexServer(plex_url, plex_token, timeout=15)
        machine_id = plex.machineIdentifier

        prefetched_tokens = {}
        token_count = 0

        try:
            account = plex.myPlexAccount()
            for plex_user in account.users():
                username = plex_user.title

                # Get user token
                try:
                    token = plex_user.get_token(machine_id)
                    if token:
                        # Get user ID and UUID
                        user_id = getattr(plex_user, "id", None)
                        user_uuid = None
                        thumb = getattr(plex_user, "thumb", "")
                        if thumb and "/users/" in thumb:
                            try:
                                user_uuid = thumb.split("/users/")[1].split("/")[0]
                            except (IndexError, AttributeError):
                                pass

                        is_home = getattr(plex_user, "home", False)

                        prefetched_tokens[username] = {
                            "title": username,
                            "id": user_id,
                            "uuid": user_uuid,
                            "token": token,
                            "is_local": bool(is_home)
                        }
                        token_count += 1
                except Exception:
                    # Skip users we can't get tokens for
                    pass
        except Exception:
            pass

        # Store prefetched tokens in setup state
        update_setup_state({"_prefetched_user_tokens": prefetched_tokens})

        return JSONResponse({"success": True, "count": token_count})

    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})


@router.post("/setup/discover-servers")
def discover_servers(plex_token: str = Form(...)):
    """Discover Plex servers associated with the authenticated account"""
    settings_service = get_settings_service()
    settings = settings_service.get_all()
    client_id = get_or_create_client_id(settings)

    headers = {
        'Accept': 'application/json',
        'X-Plex-Token': plex_token,
        'X-Plex-Product': PLEXCACHE_PRODUCT_NAME,
        'X-Plex-Version': PLEXCACHE_PRODUCT_VERSION,
        'X-Plex-Client-Identifier': client_id,
    }

    try:
        # Query Plex.tv for user's servers
        response = requests.get(
            'https://plex.tv/api/v2/resources',
            headers=headers,
            params={'includeHttps': 1, 'includeRelay': 0},
            timeout=30
        )
        response.raise_for_status()
        resources = response.json()

        servers = []
        for resource in resources:
            if resource.get('provides') == 'server':
                # Get connection URLs
                connections = resource.get('connections', [])

                # Prefer local connections, then remote
                local_url = None
                remote_url = None

                for conn in connections:
                    uri = conn.get('uri', '')
                    is_local = conn.get('local', False)

                    if is_local and not local_url:
                        local_url = uri
                    elif not is_local and not remote_url:
                        remote_url = uri

                # Use local if available, otherwise remote
                best_url = local_url or remote_url

                if best_url:
                    servers.append({
                        'name': resource.get('name', 'Unknown'),
                        'url': best_url,
                        'local_url': local_url,
                        'remote_url': remote_url,
                        'owned': resource.get('owned', False)
                    })

        return JSONResponse({
            "success": True,
            "servers": servers
        })

    except requests.RequestException as e:
        return JSONResponse({
            "success": False,
            "error": str(e)
        })


@router.get("/setup/import/detect")
def detect_import_files():
    """Detect available import files in /config/import/"""
    from web.services import get_import_service

    import_service = get_import_service()
    summary = import_service.detect_import_files()

    return JSONResponse({
        "success": True,
        "has_import_files": summary.has_import_files,
        "has_settings": summary.has_settings,
        "has_data": summary.has_data,
        "has_exclude_file": summary.has_exclude_file,
        "timestamps_count": summary.timestamps_count,
        "ondeck_count": summary.ondeck_count,
        "watchlist_count": summary.watchlist_count,
        "exclude_entries_count": summary.exclude_entries_count,
        "detected_cache_prefix": summary.detected_cache_prefix,
        "errors": summary.errors
    })


@router.post("/setup/import/execute")
async def execute_import(request: Request):
    """Execute the import operation"""
    from web.services import get_import_service

    form_data = await request.form()
    cli_cache_prefix = form_data.get("cli_cache_prefix", "/mnt/cache_downloads/")
    docker_cache_prefix = form_data.get("docker_cache_prefix", "/mnt/cache/")

    import_service = get_import_service()
    success, message, imported_settings = import_service.perform_import(
        cli_cache_prefix=cli_cache_prefix,
        docker_cache_prefix=docker_cache_prefix,
        import_settings=True,
        import_data=True
    )

    if success and imported_settings:
        # Store imported settings in setup state for verification
        # User needs to verify Plex URL which may differ in Docker
        update_setup_state(imported_settings)

    # Redirect to Step 2 to verify Plex connection (URL often differs CLI vs Docker)
    return JSONResponse({
        "success": success,
        "message": message,
        "imported_settings": bool(imported_settings),
        "redirect": "/setup?step=2" if success and imported_settings else None
    })
