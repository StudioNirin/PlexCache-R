"""Authentication routes for PlexCache-D Web UI."""

import logging
import time
import uuid

import requests
from fastapi import APIRouter, Request, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from web.config import templates, PLEXCACHE_PRODUCT_VERSION
from web.services.auth_service import get_auth_service

logger = logging.getLogger(__name__)

router = APIRouter()

# OAuth constants (same as settings.py)
PLEXCACHE_PRODUCT_NAME = "PlexCache-D"


def _is_safe_redirect(url: str) -> bool:
    """Validate that redirect target is a local path (prevents open redirect)."""
    if not url or not url.startswith("/"):
        return False
    if url.startswith("//") or url.startswith("/\\"):
        return False
    return True


def _get_client_ip(request: Request) -> str:
    """Extract client IP from request (respects X-Forwarded-For)."""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, next: str = "/"):
    """Render login page."""
    auth_service = get_auth_service()
    safe_next = next if _is_safe_redirect(next) else "/"

    # If auth is disabled, redirect to home
    if not auth_service.is_auth_enabled():
        return RedirectResponse(url="/", status_code=302)

    # If already authenticated, redirect
    session_token = request.cookies.get("plexcache_session")
    if session_token and auth_service.validate_session(session_token):
        return RedirectResponse(url=safe_next, status_code=302)

    settings = auth_service._load_settings()
    password_enabled = settings.get("auth_password_enabled", False)

    return templates.TemplateResponse(
        request,
        "auth/login.html",
        {
            "next_url": safe_next,
            "password_enabled": password_enabled,
            "error": None,
        },
    )


@router.post("/login/oauth/start")
def oauth_start():
    """Start Plex OAuth flow for login."""
    from web.services import get_settings_service
    settings_service = get_settings_service()

    # Reuse client ID from settings
    raw = settings_service._load_raw()
    client_id = raw.get("plexcache_client_id", "")
    if not client_id:
        client_id = str(uuid.uuid4())
        raw["plexcache_client_id"] = client_id
        settings_service._save_raw(raw)

    headers = {
        "Accept": "application/json",
        "X-Plex-Product": PLEXCACHE_PRODUCT_NAME,
        "X-Plex-Version": PLEXCACHE_PRODUCT_VERSION,
        "X-Plex-Client-Identifier": client_id,
    }

    try:
        response = requests.post(
            "https://plex.tv/api/v2/pins",
            headers=headers,
            data={"strong": "true"},
            timeout=30,
        )
        response.raise_for_status()
        pin_data = response.json()
    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})

    pin_id = pin_data.get("id")
    pin_code = pin_data.get("code")

    if not pin_id or not pin_code:
        return JSONResponse({"success": False, "error": "Invalid response from Plex"})

    auth_url = (
        f"https://app.plex.tv/auth#?clientID={client_id}"
        f"&code={pin_code}"
        f"&context%5Bdevice%5D%5Bproduct%5D={PLEXCACHE_PRODUCT_NAME}"
    )

    return JSONResponse({
        "success": True,
        "auth_url": auth_url,
        "client_id": client_id,
        "pin_id": pin_id,
    })


@router.get("/login/oauth/poll")
def oauth_poll(request: Request, client_id: str = Query(...), pin_id: int = Query(...)):
    """Poll for OAuth completion, then validate Plex identity."""
    auth_service = get_auth_service()

    headers = {
        "Accept": "application/json",
        "X-Plex-Product": PLEXCACHE_PRODUCT_NAME,
        "X-Plex-Version": PLEXCACHE_PRODUCT_VERSION,
        "X-Plex-Client-Identifier": client_id,
    }

    try:
        response = requests.get(
            f"https://plex.tv/api/v2/pins/{pin_id}",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        pin_status = response.json()

        auth_token = pin_status.get("authToken")
        if not auth_token:
            return JSONResponse({"success": True, "complete": False})

        # Validate against admin identity
        result = auth_service.validate_plex_login(auth_token)
        if result is None:
            client_ip = _get_client_ip(request)
            auth_service.record_login_attempt(client_ip, False)
            return JSONResponse({
                "success": True,
                "complete": True,
                "authenticated": False,
                "error": "Only the Plex server owner can sign in",
            })

        # Create session
        client_ip = _get_client_ip(request)
        auth_service.record_login_attempt(client_ip, True)
        session_token = auth_service.create_session(
            plex_id=result["account_id"],
            username=result["username"],
            remember_me=False,
        )

        ttl = auth_service.get_session_ttl(False)
        resp = JSONResponse({
            "success": True,
            "complete": True,
            "authenticated": True,
            "ttl": ttl,
        })
        resp.set_cookie(
            key="plexcache_session",
            value=session_token,
            max_age=ttl,
            httponly=True,
            samesite="lax",
            secure=str(request.url.scheme) == "https",
            path="/",
        )
        return resp

    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})


@router.post("/login/password")
def password_login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    remember_me: bool = Form(False),
    next_url: str = Form("/"),
):
    """Password-based login (rate limited)."""
    auth_service = get_auth_service()
    client_ip = _get_client_ip(request)
    safe_next = next_url if _is_safe_redirect(next_url) else "/"

    # Check rate limit
    allowed, retry_after = auth_service.check_rate_limit(client_ip)
    if not allowed:
        settings = auth_service._load_settings()
        return templates.TemplateResponse(
            request,
            "auth/login.html",
            {
                "next_url": safe_next,
                "password_enabled": settings.get("auth_password_enabled", False),
                "error": f"Too many login attempts. Try again in {retry_after} seconds.",
            },
            status_code=429,
        )

    # Validate credentials
    if auth_service.validate_password(username, password):
        auth_service.record_login_attempt(client_ip, True)
        settings = auth_service._load_settings()
        session_token = auth_service.create_session(
            plex_id=settings.get("auth_admin_plex_id", "password-user"),
            username=username,
            remember_me=remember_me,
        )

        response = RedirectResponse(url=safe_next, status_code=302)
        response.set_cookie(
            key="plexcache_session",
            value=session_token,
            max_age=auth_service.get_session_ttl(remember_me),
            httponly=True,
            samesite="lax",
            secure=str(request.url.scheme) == "https",
            path="/",
        )
        return response

    auth_service.record_login_attempt(client_ip, False)
    settings = auth_service._load_settings()
    return templates.TemplateResponse(
        request,
        "auth/login.html",
        {
            "next_url": safe_next,
            "password_enabled": settings.get("auth_password_enabled", False),
            "error": "Invalid username or password",
        },
        status_code=401,
    )


@router.post("/logout")
def logout(request: Request):
    """Destroy session and redirect to login."""
    auth_service = get_auth_service()
    session_token = request.cookies.get("plexcache_session")
    if session_token:
        auth_service.destroy_session(session_token)

    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie(key="plexcache_session", path="/")
    return response


# --- Self-Service Auth Link (Tier 2b) ---

@router.get("/link", response_class=HTMLResponse)
def link_page(request: Request):
    """Render self-service auth page for shared users to link their Plex account."""
    from web.services import get_settings_service
    settings_service = get_settings_service()
    raw = settings_service._load_raw()

    if not raw.get("auth_link_enabled", False):
        return templates.TemplateResponse(
            request,
            "auth/link.html",
            {"enabled": False, "error": None},
        )

    return templates.TemplateResponse(
        request,
        "auth/link.html",
        {"enabled": True, "error": None},
    )


@router.post("/link/oauth/start")
def link_oauth_start():
    """Start Plex OAuth flow for self-service user token linking."""
    from web.services import get_settings_service
    settings_service = get_settings_service()

    # Check feature is enabled
    raw = settings_service._load_raw()
    if not raw.get("auth_link_enabled", False):
        return JSONResponse({"success": False, "error": "Self-service auth is not enabled"})

    # Reuse client ID from settings
    client_id = raw.get("plexcache_client_id", "")
    if not client_id:
        client_id = str(uuid.uuid4())
        raw["plexcache_client_id"] = client_id
        settings_service._save_raw(raw)

    headers = {
        "Accept": "application/json",
        "X-Plex-Product": PLEXCACHE_PRODUCT_NAME,
        "X-Plex-Version": PLEXCACHE_PRODUCT_VERSION,
        "X-Plex-Client-Identifier": client_id,
    }

    try:
        response = requests.post(
            "https://plex.tv/api/v2/pins",
            headers=headers,
            data={"strong": "true"},
            timeout=30,
        )
        response.raise_for_status()
        pin_data = response.json()
    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})

    pin_id = pin_data.get("id")
    pin_code = pin_data.get("code")

    if not pin_id or not pin_code:
        return JSONResponse({"success": False, "error": "Invalid response from Plex"})

    auth_url = (
        f"https://app.plex.tv/auth#?clientID={client_id}"
        f"&code={pin_code}"
        f"&context%5Bdevice%5D%5Bproduct%5D={PLEXCACHE_PRODUCT_NAME}"
    )

    return JSONResponse({
        "success": True,
        "auth_url": auth_url,
        "client_id": client_id,
        "pin_id": pin_id,
    })


@router.get("/link/oauth/poll")
def link_oauth_poll(request: Request, client_id: str = Query(...), pin_id: int = Query(...)):
    """Poll for OAuth completion, then match user and save token."""
    from web.services import get_settings_service
    settings_service = get_settings_service()

    headers = {
        "Accept": "application/json",
        "X-Plex-Product": PLEXCACHE_PRODUCT_NAME,
        "X-Plex-Version": PLEXCACHE_PRODUCT_VERSION,
        "X-Plex-Client-Identifier": client_id,
    }

    try:
        # Check PIN status
        response = requests.get(
            f"https://plex.tv/api/v2/pins/{pin_id}",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        pin_status = response.json()

        auth_token = pin_status.get("authToken")
        if not auth_token:
            return JSONResponse({"success": True, "complete": False})

        # Get the Plex username for this token
        user_response = requests.get(
            "https://plex.tv/api/v2/user",
            headers={
                "Accept": "application/json",
                "X-Plex-Token": auth_token,
            },
            timeout=15,
        )
        user_response.raise_for_status()
        user_data = user_response.json()
        plex_username = user_data.get("username") or user_data.get("title", "")

        if not plex_username:
            return JSONResponse({
                "success": True,
                "complete": True,
                "linked": False,
                "error": "Could not determine Plex username",
            })

        # Match against configured users and save token
        saved, matched_name = settings_service.save_user_token_by_username(plex_username, auth_token)

        if saved:
            logger.info(f"Self-service auth: token saved for user '{matched_name}'")
            return JSONResponse({
                "success": True,
                "complete": True,
                "linked": True,
                "username": matched_name,
            })
        else:
            logger.warning(f"Self-service auth: no matching user for '{plex_username}'")
            return JSONResponse({
                "success": True,
                "complete": True,
                "linked": False,
                "error": f"'{plex_username}' is not in the PlexCache user list. Ask the server admin to add you first.",
            })

    except requests.RequestException as e:
        return JSONResponse({"success": False, "error": str(e)})
