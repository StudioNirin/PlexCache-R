# PlexCache-D V3.0: Automate Plex Media Management
### Updated 13/2/26

# Notice - Update to new Dockerised version. For instructions and discussion - https://github.com/StudioNirin/PlexCache-D/discussions/81 

## Current Bugs / Todo List

Now moved to a discussion page [HERE](https://github.com/StudioNirin/PlexCache-D/discussions/16)

## Overview
Automate Plex media management: Efficiently transfer media from the On Deck/Watchlist to the cache, and seamlessly move watched media back to their respective locations.
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements. Hopefully fixed and improved anyway, time will tell!

PlexCache efficiently transfers media from the On Deck/Watchlist to the cache and moves watched media back to their respective locations. This Python script reduces energy consumption by minimizing the need to spin up the array/hard drive(s) when watching recurrent media like TV series. It achieves this by moving the media from the OnDeck and watchlist for the main user and/or other users. For TV shows/anime, it also fetches the next specified number of episodes.

## Features
The original PlexCache app only worked for local users for most features, due to API limitations. V1 of Plexcache-r had some similar limitations, but a lot of those have now been fixed.

- Fetch a specified number of episodes from the "onDeck" for the main user and other users (Local/Remote).
- Skip fetching onDeck media for specified users (Local/Remote).
- Fetch a specified number of episodes from the "watchlist" for the main user and other users (Local/Remote).
- Skip fetching watchlist media for specified users (Local/Remote).
- (New v2) - .plexcached backup system, so files are not moved off the array and are instead archived to prevent unecessary move operations.
- Search only the specified libraries.
- Check for free space before moving any file.
- (New v2) - Cache retention policies, with automatic removals based on age/priority settings.
- (New v3) - **Web UI Dashboard** - Browser-based interface for monitoring and configuration.
- (New v3) - **Scheduled Runs** - Automatic execution with interval or cron scheduling.
- (New v3) - **Enhanced Webhooks** - Discord and Slack rich message formatting with granular notification levels (Summary, Activity, Errors, Warnings).
- (New v3) - **Stop Button** - Abort running operations gracefully from the Web UI.
- (New v3) - **Smart Error Handling** - Migration stops early on critical errors (disk full, permissions).
- (New v3) - **Async Maintenance** — Background thread execution for maintenance actions (restore, sync, protect, delete, fix) with real-time progress.
- (New v3) - **Parallel File Operations** — Concurrent file moves/copies with configurable worker count.
- (New v3) - **Cache Health Audit** — Detect unprotected files, orphaned backups, stale entries with one-click fixes.
- (New v3) - **ZFS Support** — Automatic detection of ZFS pool-only shares with correct path resolution.
- (New v3) - **Min Free Space** — Safety floor setting to prevent caching when cache drive space is low.
- (New v3) - **Docker Support** — Official container with Unraid template, auto-setup, and path translation.
- (New v3) - **Byte-Level Progress** — Smooth progress bar updates every 10MB during file copies, with ETA from copy rate.
- Move watched media present on the cache drive back to the array.
- Move respective subtitles along with the media moved to or from the cache.
- Filter media older than a specified number of days.
- Run in debug mode for testing.
- Use of a log file for easy debugging.
- Use caching system to avoid wastful memory usage and cpu cycles.
- Use of multitasking to optimize file transfer time.
- Exit the script if any active session or skip the currently playing media.
- Send Webhook/Unraid notifications with configurable trigger levels.
- (New v2) - Unraid Mover exclusion file. This file also allows for manual custom entries. 



  
### Project Structure

```
PlexCache-D/
├── plexcache.py              # Unified entry point (CLI, Web UI, setup wizard)
├── core/                     # Core application modules
│   ├── app.py                # Main orchestrator (PlexCacheApp class)
│   ├── setup.py              # Interactive setup wizard
│   ├── config.py             # Configuration management (dataclasses, JSON settings)
│   ├── logging_config.py     # Logging, rotation, Unraid/webhook notification handlers
│   ├── system_utils.py       # OS detection, path conversions, file utilities
│   ├── plex_api.py           # Plex server interactions (OnDeck, Watchlist, RSS feeds)
│   └── file_operations.py    # File moving, filtering, subtitles, timestamp tracking
├── web/                      # Web UI (FastAPI + HTMX)
│   ├── main.py               # FastAPI application (lifespan, middleware, error handlers)
│   ├── config.py             # Web configuration + shared Jinja2 templates instance
│   ├── dependencies.py       # Shared instances
│   ├── routers/              # Route handlers (dashboard, cache, settings, logs, maintenance, operations, setup)
│   ├── services/             # Business logic layer
│   │   ├── maintenance_runner.py  # Background maintenance thread runner
│   │   ├── operation_runner.py    # Background operation runner + activity feed
│   │   ├── cache_service.py       # Cache analysis and storage stats
│   │   └── ...                    # Scheduler, settings, import services
│   ├── templates/            # Jinja2 templates (Plex theme)
│   └── static/               # CSS, JS assets
├── docker/                   # Docker support
│   ├── Dockerfile            # Multi-stage container build
│   ├── docker-entrypoint.sh  # Container startup script
│   └── plexcache-d.xml       # Unraid Community Apps template
├── tools/                    # Diagnostic utilities
│   └── audit_cache.py        # Cache diagnostic tool
├── data/                     # Runtime tracking files (auto-created, JSON)
├── logs/                     # plexcache.log (rotating, 10MB, 5 backups)
├── plexcache_settings.json   # User configuration
└── plexcache_cached_files.txt  # Tracked cache files (Unraid mover exclude list)
```

## Web UI (New in V3.0)

PlexCache-D now includes a browser-based dashboard for monitoring and configuration.

**Start the Web UI:**
```bash
python3 plexcache.py --web               # Start on localhost:5000
python3 plexcache.py --web --host 0.0.0.0  # Listen on all interfaces
python3 plexcache.py --web --port 8080     # Custom port
```

**Features:**
- **Setup Wizard** - Guided first-run configuration with Plex OAuth support
- **Dashboard** - Real-time cache stats, Plex connection status, recent activity feed
- **Cached Files** - Sortable file browser with filters, eviction controls
- **Storage** - Drive analytics, breakdowns by source, largest/oldest files
- **Maintenance** - Cache health audit, unprotected file detection, one-click fixes
- **Settings** - Full configuration UI with Plex OAuth, library selection, user toggles, test connection
- **Schedule** - Automatic runs with interval or cron expressions
- **Logs** - Real-time log viewer with search, filters, and live streaming
- **Stop Button** - Abort running operations gracefully (stops after current file completes)
- **Operations** — Run Now with real-time progress banner, ETA, and stop button
- **Activity Feed** — Recent file operations with persistent history
- **Maintenance History** — Persistent log of past maintenance actions

**Tech Stack:** FastAPI, HTMX, Jinja2, Plex-inspired dark theme

> **Note:** When running via Docker, the default port is **5757**. When running via CLI, the default port is **5000**.

## Docker Installation (Recommended for Unraid)

PlexCache-D is available as a Docker container, ideal for Unraid users.

**Container Registry:** `ghcr.io/studionirin/plexcache-d`

### Quick Start

```bash
docker run -d \
  --name plexcache-d \
  -p 5757:5757 \
  -v /mnt/user/appdata/plexcache:/config \
  -v /mnt/cache:/mnt/cache \
  -v /mnt/user0:/mnt/user0 \
  -v /mnt/user:/mnt/user \
  -e PUID=99 \
  -e PGID=100 \
  -e TZ=America/Los_Angeles \
  ghcr.io/studionirin/plexcache-d:latest
```

### Unraid Installation

1. Go to **Docker** → **Add Container**
2. Set **Repository**: `ghcr.io/studionirin/plexcache-d`
3. Add required volume mappings:
   - `/config` → `/mnt/user/appdata/plexcache`
   - `/mnt/cache` → `/mnt/cache` (read-write)
   - `/mnt/user0` → `/mnt/user0` (read-write)
   - `/mnt/user` → `/mnt/user` (read-write)
4. Set **WebUI**: `http://[IP]:[PORT:5757]`
5. Click **Apply**

> **Important:** All media paths (`/mnt/cache`, `/mnt/user0`, `/mnt/user`) must be **read-write** for PlexCache-D to move files between cache and array.

### First Run

Open `http://[YOUR_IP]:5757` - the Setup Wizard will guide you through:
- Plex connection (OAuth or manual token)
- Library selection with cacheable options
- User selection for OnDeck/Watchlist monitoring
- Caching behavior configuration

**Important:** Volume paths for `/mnt/cache`, `/mnt/user0`, and `/mnt/user` must match exactly between container and host for Plex path resolution.

See `docker/UNRAID_SETUP.md` for detailed Unraid setup instructions including CA Mover Tuning integration.

## Installation and Setup

There are three ways to run PlexCache-D, depending on your preference:

| | Docker | Manual + Web UI | Manual + CLI |
|---|---|---|---|
| **Best for** | Unraid users who prefer containers | Unraid/Linux users who prefer native installs | Lightweight, script-only usage |
| **Web dashboard** | Yes (always on) | Yes (always on) | No |
| **Scheduling** | Built-in (via Web UI) | Built-in (via Web UI) | External (cron / User Scripts) |
| **Auto-start** | Docker restart policy | systemd or User Scripts plugin | cron `@reboot` or User Scripts |
| **Default port** | 5757 | 5000 | N/A |
| **Setup** | Web UI wizard on first visit | Web UI wizard on first visit | `--setup` CLI wizard |

### Option 1: Docker (Recommended)

See the [Docker Installation](#docker-installation-recommended-for-unraid) section above.

### Option 2: Manual Install + Web UI

This runs PlexCache-D as a persistent web server with dashboard, scheduler, and all V3.0 features.

**Prerequisites:**
- Python 3.9+
- Git (to clone the repo)

**Install:**
```bash
cd /mnt/user/appdata
git clone https://github.com/StudioNirin/PlexCache-D.git
cd PlexCache-D
pip3 install -r requirements.txt
```

**Start the Web UI:**
```bash
python3 plexcache.py --web --host 0.0.0.0           # Listen on all interfaces, port 5000
python3 plexcache.py --web --host 0.0.0.0 --port 8080  # Custom port
```

Then open `http://[YOUR_IP]:5000` in your browser. On first run, the Setup Wizard will guide you through configuration.

#### Auto-Start on Boot (Unraid — User Scripts Plugin)

The easiest way to auto-start on Unraid without Docker:

1. Install **User Scripts** from Community Apps (if not already installed)
2. Go to **Settings** → **User Scripts** → **Add New Script**
3. Name it `PlexCache-D Web UI`
4. Click the script name, then **Edit Script** and paste:

```bash
#!/bin/bash
cd /mnt/user/appdata/PlexCache-D
nohup python3 plexcache.py --web --host 0.0.0.0 --port 5000 > /dev/null 2>&1 &
```

5. Set the schedule to **At Startup of Array**
6. Click **Apply**

> **Tip:** To stop the server, find the process with `ps aux | grep plexcache` and `kill` it, or use the User Scripts **Stop** button if available.

#### Auto-Start on Boot (Generic Linux — systemd)

For non-Unraid Linux systems, create a systemd service:

```bash
sudo nano /etc/systemd/system/plexcache-d.service
```

```ini
[Unit]
Description=PlexCache-D Web UI
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/PlexCache-D
ExecStart=/usr/bin/python3 plexcache.py --web --host 0.0.0.0 --port 5000
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable plexcache-d
sudo systemctl start plexcache-d
```

Check status with `sudo systemctl status plexcache-d` and logs with `journalctl -u plexcache-d -f`.

### Option 3: Manual Install + CLI Only

This is the original V1/V2 mode — no web server, no dashboard. PlexCache-D runs once, performs caching/eviction, and exits. You schedule it externally.

**Install** (same as Option 2):
```bash
cd /mnt/user/appdata
git clone https://github.com/StudioNirin/PlexCache-D.git
cd PlexCache-D
pip3 install -r requirements.txt
```

**First-time setup:**
```bash
python3 plexcache.py --setup
```

**Run manually:**
```bash
python3 plexcache.py                     # Normal run
python3 plexcache.py --dry-run --verbose # Test run with full debug output
```

#### Schedule with Cron

```bash
crontab -e
```

```
# Run PlexCache-D every 6 hours
0 */6 * * * cd /mnt/user/appdata/PlexCache-D && python3 plexcache.py >> /mnt/user/appdata/PlexCache-D/logs/cron.log 2>&1
```

#### Schedule with Unraid User Scripts

1. Install **User Scripts** from Community Apps
2. **Add New Script** → name it `PlexCache-D`
3. **Edit Script:**

```bash
#!/bin/bash
cd /mnt/user/appdata/PlexCache-D
python3 plexcache.py
```

4. Set schedule (e.g., **Custom** → `0 */6 * * *` for every 6 hours)
5. Click **Apply**

---

For additional help, check the [Wiki](https://github.com/StudioNirin/PlexCache-D/wiki) for detailed guides. If something doesn't make sense or doesn't work, please open a new issue. But don't be upset if the answer is in the Wiki and we mock you for not reading it thoroughly first.

## Notes

This script might be compatible with other systems, especially Linux-based ones, although I have primarily tested it on Unraid with plex as docker container. While I cannot support every case, it's worth checking the GitHub issues to see if your specific case has already been discussed. Particularly worth checking the original Bexem repo issues page.
I will still try to help out, but please note that I make no promises in providing assistance for every scenario.
**It is highly advised to use the setup script.**

## Known Limitations

### Remote/Network Storage

**The `.plexcached` backup system does NOT work with remote or network-attached storage** (e.g., Synology NAS mounted via SMB/NFS).

Why this is a problem:
- The Unraid mover only moves files on the local array, not remote mounts
- `.plexcached` backups on remote storage won't protect against anything
- Remote NAS is typically "always-on" anyway, so there's no array spinup savings

**Recommendation:** In the setup wizard or settings, set libraries on remote storage as **non-cacheable** (`enabled: false` in path_mappings). This prevents PlexCache-D from attempting to manage files it cannot properly protect.

### Dynamix File Integrity False Positives

If you use the **Dynamix File Integrity** plugin on Unraid, you may see "SHA256 hash key mismatch" errors for files managed by PlexCache-D. **These are false positives, not actual corruption.**

Why this happens:
- Dynamix records hashes using the original filename (e.g., `movie.mkv`)
- PlexCache-D renames array files to `.plexcached` (e.g., `movie.mkv.plexcached`)
- Dynamix can't find the original filename and reports it as corrupted/missing

**Your files are intact.** The rename operation does not modify file contents. You can verify by comparing MD5/SHA256 hashes of the cache copy and `.plexcached` backup - they will match.

**Recommendations:**
- Exclude `*.plexcached` files from Dynamix scanning
- Or rebuild the Dynamix hash database after PlexCache-D has been running
- Or exclude PlexCache-D managed directories from integrity scanning

## Disclaimer

This script comes without any warranties, guarantees, or magic powers. By using this script, you accept that you're responsible for any consequences that may result. The author will not be held liable for data loss, corruption, or any other problems you may encounter. So, it's on you to make sure you have backups and test this script thoroughly before you unleash its awesome power.

## Acknowledgments

It seems we all owe a debt of thanks to someone called brimur[^3] for providing the script that served as the foundation and inspiration for this project. That was long before my time on it though, the first iteration I saw was by bexem[^4], who also has my thanks. But the biggest contributor to this continuation of the project was by bbergle[^5], who put in all the work on refactoring and cleaning up all the code into bite-sized chunks that were understandable to a novice like myself. All I did then was go through it all and try and make the wierd janky Plex API actually kinda work, for what I needed it to do anyway!

And my first personal thankyou to [Brandon-Haney](https://github.com/Brandon-Haney) who has contributed a whole bunch of updates. I haven't yet merged them as of writing this, but he's gone through basically every file so I figured he deserved a pre-emptive thanks!


[^1]: Remote users do not have individual watchlists accessible by the API. It's unfortunately not a thing. So instead I am using the available RSS feed as a workaround. The downside of this is... 
[^2]: ...that it is an all-or-nothing proposal for remote users. Local users can still be toggled on a per-user basis.
[^3]: [brimur/preCachePlexOnDeckEpiosodes.py](https://gist.github.com/brimur/95277e75ca399d5d52b61e6aa192d1cd)
[^4]: https://github.com/bexem/PlexCache
[^5]: https://github.com/BBergle/PlexCache




