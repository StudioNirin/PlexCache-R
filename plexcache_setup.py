#!/usr/bin/env python3
"""
DEPRECATED: This entry point is deprecated.
Use 'python plexcache.py --setup' instead.

This wrapper is kept for backwards compatibility during the transition period.
"""
import sys
import warnings

# Issue deprecation warning
warnings.warn(
    "plexcache_setup.py is deprecated. Use 'python plexcache.py --setup' instead.",
    DeprecationWarning,
    stacklevel=2
)
print("WARNING: plexcache_setup.py is deprecated. Use 'python plexcache.py --setup' instead.",
      file=sys.stderr)
print(file=sys.stderr)

# Forward to the new location
from core.setup import run_setup
run_setup()
