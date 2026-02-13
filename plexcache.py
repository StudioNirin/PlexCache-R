#!/usr/bin/env python3
"""PlexCache-R - Plex media caching automation for Unraid.

This is the unified entry point for PlexCache-R. It provides:
- Automatic first-run setup when no configuration exists
- Manual setup access via --setup flag
- Web UI via --web flag
- Normal caching operation

Usage:
    python plexcache.py              # Run caching (auto-setup if needed)
    python plexcache.py --setup      # Run setup wizard
    python plexcache.py --web        # Start web UI
    python plexcache.py --dry-run    # Simulate without moving files
    python plexcache.py --verbose    # Enable debug logging
    python plexcache.py --help       # Show help
"""
import sys
import os


def get_help_text():
    """Generate help text with the actual Python command being used."""
    # Get the actual python command (e.g., python, python3, python3.11)
    python_cmd = os.path.basename(sys.executable)

    return f"""
PlexCache-R - Plex media caching automation for Unraid

Usage: {python_cmd} plexcache.py [OPTIONS]

Options:
  --setup               Run the setup wizard to configure PlexCache
  --web                 Start the web UI (FastAPI server)
  --dry-run             Simulate operations without moving files
  --verbose, -v         Enable debug-level logging
  --quiet               Only notify on errors (suppress info messages)
  --show-priorities     Display cache priority scores for all cached files
  --show-mappings       Display path mapping configuration and status
  --restore-plexcached  Emergency restore of .plexcached backup files

Web UI Options (use with --web):
  --host HOST           Host to bind to (default: 127.0.0.1)
  --port PORT           Port to listen on (default: 5000)
  --reload              Enable auto-reload for development

Examples:
  {python_cmd} plexcache.py                     Run caching (auto-setup on first run)
  {python_cmd} plexcache.py --setup             Configure or reconfigure settings
  {python_cmd} plexcache.py --web               Start web UI on localhost:5000
  {python_cmd} plexcache.py --web --port 8080   Start web UI on custom port
  {python_cmd} plexcache.py --dry-run --verbose Test run with full debug output
  {python_cmd} plexcache.py --show-priorities   See which files would be evicted first

Documentation: https://github.com/StudioNirin/PlexCache-R
"""


def run_web_ui():
    """Start the web UI server."""
    # Parse web-specific arguments
    host = '127.0.0.1'
    port = 5000
    reload_enabled = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--host' and i + 1 < len(args):
            host = args[i + 1]
            i += 2
        elif args[i] == '--port' and i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                print(f"Error: Invalid port number: {args[i + 1]}")
                return 1
            i += 2
        elif args[i] == '--reload':
            reload_enabled = True
            i += 1
        else:
            i += 1

    # Check for required dependencies
    try:
        import uvicorn
    except ImportError:
        print("Error: Web UI dependencies not installed.")
        print("")
        print("Install with:")
        print("  pip install fastapi uvicorn[standard] jinja2 python-multipart websockets aiofiles")
        print("")
        print("Or install all requirements:")
        print("  pip install -r requirements.txt")
        return 1

    try:
        from web.main import app  # noqa: F401 - verify import works
    except ImportError as e:
        print(f"Error: Failed to import web application: {e}")
        print("")
        print("Make sure you're running from the PlexCache-R directory.")
        return 1

    print("=" * 60)
    print("  PlexCache-R Web UI")
    print("=" * 60)
    print(f"  URL: http://{host}:{port}")
    print(f"  Reload: {'Enabled' if reload_enabled else 'Disabled'}")
    print("=" * 60)
    print("")
    print("Press Ctrl+C to stop the server")
    print("")

    uvicorn.run(
        "web.main:app",
        host=host,
        port=port,
        reload=reload_enabled,
        log_level="info"
    )
    return 0


def main():
    """Main entry point for PlexCache-R."""
    # Check for help flags
    if "--help" in sys.argv or "-h" in sys.argv or "--h" in sys.argv:
        print(get_help_text())
        return 0

    # Check for --setup flag (explicit setup request)
    if "--setup" in sys.argv:
        from core.setup import run_setup
        run_setup()
        return 0

    # Check for --web flag (start web UI)
    if "--web" in sys.argv:
        return run_web_ui()

    # Get project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    settings_path = os.path.join(script_dir, "plexcache_settings.json")

    # Auto-run setup if no settings file exists (first-run experience)
    if not os.path.exists(settings_path):
        print("No configuration found. Starting setup wizard...")
        print()
        from core.setup import run_setup
        run_setup()
        return 0

    # Normal operation - run the caching application
    from core.app import main as app_main
    return app_main()


if __name__ == "__main__":
    sys.exit(main() or 0)
