#!/usr/bin/env python3
"""
DEPRECATED: This entry point is deprecated.
Use 'python plexcache.py' instead.

This wrapper is kept for backwards compatibility during the transition period.
"""
import sys
import warnings

# Issue deprecation warning
warnings.warn(
    "plexcache_app.py is deprecated. Use 'python plexcache.py' instead.",
    DeprecationWarning,
    stacklevel=2
)
print("WARNING: plexcache_app.py is deprecated. Use 'python plexcache.py' instead.",
      file=sys.stderr)
print(file=sys.stderr)

# Forward to the new location
from core.app import main
sys.exit(main())
