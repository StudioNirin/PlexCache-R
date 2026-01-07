#!/usr/bin/env python3
"""
PlexCache-R Web UI

Starts the FastAPI web interface for PlexCache-R management.

Usage:
    python plexcache_web.py              # Start on port 5000
    python plexcache_web.py --port 8080  # Custom port
    python plexcache_web.py --host 0.0.0.0  # Listen on all interfaces
    python plexcache_web.py --reload     # Enable auto-reload for development
"""

import argparse
import sys
from pathlib import Path

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def main():
    parser = argparse.ArgumentParser(
        description='PlexCache-R Web UI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python plexcache_web.py                    # Start on localhost:5000
    python plexcache_web.py --port 8080        # Use custom port
    python plexcache_web.py --host 0.0.0.0     # Listen on all interfaces
    python plexcache_web.py --reload           # Auto-reload on code changes
        """
    )
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port to listen on (default: 5000)'
    )
    parser.add_argument(
        '--reload',
        action='store_true',
        help='Enable auto-reload for development'
    )
    args = parser.parse_args()

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
        sys.exit(1)

    try:
        from web.main import app
    except ImportError as e:
        print(f"Error: Failed to import web application: {e}")
        print("")
        print("Make sure you're running from the PlexCache-R directory.")
        sys.exit(1)

    print("=" * 60)
    print("  PlexCache-R Web UI")
    print("=" * 60)
    print(f"  URL: http://{args.host}:{args.port}")
    print(f"  Reload: {'Enabled' if args.reload else 'Disabled'}")
    print("=" * 60)
    print("")
    print("Press Ctrl+C to stop the server")
    print("")

    uvicorn.run(
        "web.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()
