#!/usr/bin/env python3
"""PlexCache-R - Plex media caching automation for Unraid.

This is the unified entry point for PlexCache-R. It provides:
- Automatic first-run setup when no configuration exists
- Manual setup access via --setup flag
- Normal caching operation

Usage:
    python plexcache.py              # Run caching (auto-setup if needed)
    python plexcache.py --setup      # Run setup wizard
    python plexcache.py --dry-run    # Simulate without moving files
    python plexcache.py --verbose    # Enable debug logging
    python plexcache.py --help       # Show help
"""
import sys
import os


def main():
    """Main entry point for PlexCache-R."""
    # Check for --setup flag (explicit setup request)
    if "--setup" in sys.argv:
        from core.setup import run_setup
        run_setup()
        return 0

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
