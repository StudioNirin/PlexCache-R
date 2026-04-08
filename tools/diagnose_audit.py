#!/usr/bin/env python3
"""
Diagnose the cost of a single ``MaintenanceService.run_full_audit()`` call.

Context
-------
StudioNirin/PlexCache-R#136 reports that the web UI dashboard stalls and CPU
spikes for hours on Unraid. ``WebCacheService`` runs ``run_full_audit()`` every
5 minutes (web/services/web_cache.py:48), and the audit does a recursive walk
of every cache + array directory plus ``os.path.exists`` probes for every
unprotected cache file (web/services/maintenance_service.py:534+). On a large
library with spun-down array disks, a single cycle can take tens of minutes.

This script answers:
- How long does one audit take on *this* server?
- Where does the time go (cache walk vs. array walk vs. per-file probes)?
- How many array ``os.path.exists`` calls does the audit fan out to?
- How big are the cache/exclude/timestamp sets?
- How many unprotected / orphaned / duplicate entries does it find?

Usage
-----
Run inside the PlexCache-R Docker container (or on a native install from the
project root):

    docker exec -it plexcache python3 /app/tools/diagnose_audit.py

Or download-and-run (no repo clone needed):

    docker exec -it plexcache sh -c \\
        'wget -qO /tmp/diag.py https://raw.githubusercontent.com/Brandon-Haney/PlexCache-R/dev/tools/diagnose_audit.py \\
         && python3 /tmp/diag.py'

The script is read-only. It imports the running app's ``MaintenanceService``
and calls ``run_full_audit()`` exactly once, instrumenting ``os.path.exists``
and ``os.walk`` to count probes. It does not modify any file, setting, or
tracker.
"""

import os
import sys
import time
from collections import Counter

# Resolve the PlexCache-R project root. The script may be run from:
#   - tools/ inside a checkout  -> parent dir
#   - /tmp (after curl) in the Docker container -> /app
#   - the project root directly -> cwd
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_candidates = [
    os.path.dirname(SCRIPT_DIR) if os.path.basename(SCRIPT_DIR) == "tools" else SCRIPT_DIR,
    os.getcwd(),
    "/app",
]
PROJECT_ROOT = None
for _c in _candidates:
    if _c and os.path.isdir(os.path.join(_c, "web")) and os.path.isdir(os.path.join(_c, "core")):
        PROJECT_ROOT = _c
        break
if PROJECT_ROOT is None:
    PROJECT_ROOT = _candidates[0]  # fall through; import will error with a clear message
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# ``web.config`` derives DATA_DIR from the cwd — make sure it points at the app root.
try:
    os.chdir(PROJECT_ROOT)
except OSError:
    pass


def _human_bytes(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.2f} {unit}"
        n /= 1024


def _human_duration(seconds):
    if seconds < 60:
        return f"{seconds:.2f}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{int(m)}m {s:.1f}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h {int(m)}m {s:.0f}s"


class _Counters:
    """Instrument os.path.exists and os.walk to measure audit fan-out."""

    def __init__(self):
        self.exists_calls = 0
        self.exists_hits = 0
        self.exists_time = 0.0
        self.walk_calls = 0
        self.walk_dirs = 0
        self.walk_files = 0
        self.walk_time = 0.0
        self.walk_tops = Counter()  # top dirs walked, for hot-path attribution

        self._orig_exists = os.path.exists
        self._orig_walk = os.walk

    def install(self):
        real_exists = self._orig_exists
        real_walk = self._orig_walk

        def wrapped_exists(path):
            self.exists_calls += 1
            t0 = time.perf_counter()
            result = real_exists(path)
            self.exists_time += time.perf_counter() - t0
            if result:
                self.exists_hits += 1
            return result

        def wrapped_walk(top, *args, **kwargs):
            self.walk_calls += 1
            self.walk_tops[top] += 1
            t0 = time.perf_counter()
            for root, dirs, files in real_walk(top, *args, **kwargs):
                self.walk_dirs += 1
                self.walk_files += len(files)
                yield root, dirs, files
            self.walk_time += time.perf_counter() - t0

        os.path.exists = wrapped_exists
        os.walk = wrapped_walk

    def uninstall(self):
        os.path.exists = self._orig_exists
        os.walk = self._orig_walk


def _phase(label, fn):
    print(f"  [{label}] running...", flush=True)
    t0 = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - t0
    print(f"  [{label}] done in {_human_duration(elapsed)}", flush=True)
    return result, elapsed


def main():
    print("=" * 72)
    print("PlexCache-R audit diagnostic")
    print("=" * 72)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Python:       {sys.version.split()[0]}")
    print(f"Started:      {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        from web.services.maintenance_service import get_maintenance_service
    except Exception as exc:
        print(f"ERROR: could not import maintenance_service: {exc}")
        print("Make sure you're running this from the PlexCache-R project root")
        print("or inside the container (/app).")
        return 2

    service = get_maintenance_service()

    # --- Phase 1: measure the cheap pieces on their own ---------------------
    print("Phase A — input sizes (independent of full audit):")
    cache_files, cache_elapsed = _phase("get_cache_files", service.get_cache_files)
    exclude_files, exclude_elapsed = _phase("get_exclude_files", service.get_exclude_files)
    timestamp_files, ts_elapsed = _phase("get_timestamp_files", service.get_timestamp_files)
    print()
    print(f"  cache files (walked):      {len(cache_files):,}")
    print(f"  exclude list entries:      {len(exclude_files):,}")
    print(f"  timestamp entries:         {len(timestamp_files):,}")
    unprotected_count_est = len(cache_files - exclude_files)
    print(f"  unprotected (cache\\excl):  {unprotected_count_est:,}")
    print()

    # --- Phase 2: full audit, instrumented ----------------------------------
    print("Phase B — instrumented run_full_audit():")
    counters = _Counters()
    counters.install()
    t0 = time.perf_counter()
    try:
        results = service.run_full_audit()
    finally:
        counters.uninstall()
    total_elapsed = time.perf_counter() - t0
    print(f"  total:                     {_human_duration(total_elapsed)}")
    print()

    # --- Phase 3: report ----------------------------------------------------
    print("Audit results:")
    print(f"  cache_file_count:          {results.cache_file_count:,}")
    print(f"  exclude_entry_count:       {results.exclude_entry_count:,}")
    print(f"  timestamp_entry_count:     {results.timestamp_entry_count:,}")
    print(f"  unprotected_files:         {len(results.unprotected_files):,}")
    print(f"  orphaned_plexcached:       {len(results.orphaned_plexcached):,}")
    print(f"  stale_exclude_entries:     {len(results.stale_exclude_entries):,}")
    print(f"  stale_timestamp_entries:   {len(results.stale_timestamp_entries):,}")
    print(f"  duplicates:                {len(results.duplicates):,}")
    print(f"  health_status:             {results.health_status}")
    print()

    print("Filesystem fan-out during audit:")
    print(f"  os.path.exists calls:      {counters.exists_calls:,}")
    print(f"    hits (True):             {counters.exists_hits:,}")
    print(f"    total time in exists:    {_human_duration(counters.exists_time)}")
    if counters.exists_calls:
        avg_ms = (counters.exists_time / counters.exists_calls) * 1000
        print(f"    avg per call:            {avg_ms:.2f} ms")
    print(f"  os.walk invocations:       {counters.walk_calls:,}")
    print(f"    directories visited:     {counters.walk_dirs:,}")
    print(f"    files seen by walks:     {counters.walk_files:,}")
    print(f"    total time in walks:     {_human_duration(counters.walk_time)}")
    print()

    if counters.walk_tops:
        print("Top-level walk targets:")
        for top, count in counters.walk_tops.most_common(10):
            print(f"  {count}x  {top}")
        print()

    # --- Phase 4: interpretation --------------------------------------------
    print("Interpretation:")
    interval = 300  # WebCacheService.REFRESH_INTERVAL_SECONDS
    ratio = total_elapsed / interval
    if total_elapsed > interval:
        print(f"  ⚠ audit ({_human_duration(total_elapsed)}) exceeds the 5-minute")
        print(f"    background refresh interval. Each cycle starts behind the")
        print(f"    previous one — the refresh thread is saturated. This matches")
        print(f"    issue #136 (dashboard stuck loading, high CPU).")
    elif total_elapsed > interval * 0.5:
        print(f"  ⚠ audit consumes {ratio * 100:.0f}% of the 5-minute refresh")
        print(f"    window. Heavy concurrent web traffic or cold array disks")
        print(f"    could easily push this over the limit.")
    else:
        print(f"  ✓ audit ({_human_duration(total_elapsed)}) is well under the")
        print(f"    5-minute background refresh interval. No saturation on this")
        print(f"    run. If the array was spun up during this test, a cold run")
        print(f"    can be much slower — consider re-running after the disks")
        print(f"    have been idle for 30+ minutes.")

    if counters.exists_calls > 5000:
        print(f"  ⚠ {counters.exists_calls:,} os.path.exists calls — each one can")
        print(f"    spin an Unraid array disk. A single pre-built array file set")
        print(f"    would reduce this to O(1) lookups.")

    if counters.walk_files > 500000:
        print(f"  ⚠ os.walk visited {counters.walk_files:,} files. Consider")
        print(f"    pruning excluded directories or caching the walk result")
        print(f"    between refresh cycles.")

    print()
    print("Paste this entire output into the GitHub issue for comparison.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
