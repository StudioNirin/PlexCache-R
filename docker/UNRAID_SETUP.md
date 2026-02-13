# PlexCache-D Unraid Setup Guide

This guide covers installing and configuring PlexCache-D on Unraid.

> **IMPORTANT:** If you're currently running the CLI version of PlexCache via User Scripts or cron, **disable those scheduled runs first** to avoid conflicts. Running both the Docker scheduler and CLI scripts simultaneously can cause race conditions and duplicate file operations.

## Overview

PlexCache-D automatically caches your frequently-accessed Plex media (OnDeck and Watchlist items) to your cache drive. This reduces array spinups and improves playback performance by keeping actively-watched content on fast storage.

## Prerequisites

- Unraid 6.9 or later
- Plex Media Server running (accessible from Docker)
- Cache drive configured
- Docker service enabled

## Installation

### Option 1: Community Apps (Recommended)

1. Open the **Apps** tab in Unraid
2. Search for "PlexCache-D"
3. Click **Install**
4. Configure the paths (see below)
5. Click **Apply**

### Option 2: Docker Template (Quick Install)

1. Download [plexcache-d.xml](https://raw.githubusercontent.com/StudioNirin/PlexCache-D/v3.0/docker/plexcache-d.xml) (will move to StudioNirin repo after upstream merge)
2. Place it in `/boot/config/plugins/dockerMan/templates-user/` on your Unraid server
3. Go to **Docker** → **Add Container** → Select "plexcache-d" from the template dropdown
4. Adjust paths for your setup and click **Apply**

### Option 3: Manual Docker Installation

1. Go to **Docker** tab → **Add Container**
2. Set the following:
   - **Repository**: `ghcr.io/studionirin/plexcache-d`
   - **Network Type**: Bridge
   - **WebUI**: `http://[IP]:[PORT:5757]`

3. Add the required path mappings (see Configuration below)
4. Click **Apply**

## Configuration

### Required Volume Mappings

| Container Path | Host Path | Mode | Description |
|---------------|-----------|------|-------------|
| `/config` | `/mnt/user/appdata/plexcache` | rw | Config, data, logs, exclude file |
| `/mnt/cache` | `/mnt/cache` | rw | Your cache drive (destination for cached files) |
| `/mnt/user0` | `/mnt/user0` | rw | Array-only view (for .plexcached backups) |
| `/mnt/user` | `/mnt/user` | rw | Merged share (source for caching operations) |

**Important**:
- All media paths (`/mnt/cache`, `/mnt/user0`, `/mnt/user`) must be **read-write** for PlexCache-D to move files between cache and array
- These paths **must match exactly** between container and host for Plex path resolution to work correctly

### Docker Path Translation (Host Cache Path)

> **If your cache mount differs between host and container** (e.g., host `/mnt/cache_downloads` → container `/mnt/cache`), you **must** configure **Host Cache Path** in **Settings → Paths** for each path mapping.
>
> This ensures the mover exclude file contains paths that the Unraid mover recognizes. Without this, the mover may incorrectly move your cached files back to the array.

**Example Path Mapping:**

| Plex Path | Cache Path | Array Path | Host Cache Path |
|-----------|------------|------------|-----------------|
| `/data/media/movies` | `/mnt/cache/media/movies` | `/mnt/user0/media/movies` | `/mnt/cache_downloads/media/movies` |
| `/data/media/tv` | `/mnt/cache/media/tv` | `/mnt/user0/media/tv` | `/mnt/cache_downloads/media/tv` |

- **Plex Path**: The path Plex reports for your media (check Plex library settings)
- **Cache Path**: Where the file lives on your cache drive (inside the container)
- **Array Path**: Where the `.plexcached` backup lives on the array (inside the container)
- **Host Cache Path**: The actual host path for the mover exclude file (only needed if different from Cache Path)

### Optional Volume Mappings (Unraid Notifications)

To enable native Unraid notifications from the Docker container, add these optional mounts:

| Container Path | Host Path | Mode | Description |
|---------------|-----------|------|-------------|
| `/usr/local/emhttp` | `/usr/local/emhttp` | ro | Unraid's notify script and PHP includes |
| `/tmp/notifications` | `/tmp/notifications` | rw | Unraid's notification queue |

**Both mounts are required** for Unraid notifications to work. Without them, use Discord/Slack webhooks instead (which work without any extra mounts).

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PUID` | 99 | User ID (99 = nobody) |
| `PGID` | 100 | Group ID (100 = users) |
| `TZ` | America/Los_Angeles | Your timezone |
| `LOG_LEVEL` | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |

### Port Configuration

| Port | Default | Description |
|------|---------|-------------|
| Web UI | 5757 | PlexCache-D web interface |

## First Run Setup

1. After starting the container, open the Web UI at `http://[UNRAID_IP]:5757`
2. The Setup Wizard will launch automatically
3. Follow the 6-step wizard:

### Step 1: Welcome
Introduction to PlexCache-D features.

### Step 2: Plex Connection
- Click **Sign in with Plex** for OAuth (recommended) - automatically discovers your server
- Or manually enter Plex URL and token
- Test connection before proceeding

### Step 3: Libraries & Paths
- Set your **Cache Drive Location** (e.g., `/mnt/cache`)
- Select libraries to monitor for OnDeck/Watchlist
- Check **Cacheable** for libraries on your Unraid array (uncheck for remote/network storage)
- Path Mappings section is for advanced users only (Docker path remapping)

### Step 4: Users
- Enable **Monitor Other Users** to cache shared users' content
- Use **Select All** to quickly select all users
- Configure skip options per user (Skip OnDeck, Skip Watchlist)
- Remote users need RSS URL for watchlist support (see info box)

### Step 5: Behavior
- Number of episodes to cache from OnDeck
- Watchlist settings and retention
- Cache retention hours

### Step 6: Complete
Review your configuration and click **Complete Setup**.

**Note:** Settings are stored in memory during the wizard. Nothing is saved until you complete the final step - you can safely abandon the wizard without creating a partial configuration.

## Mover Integration (Optional but Recommended)

PlexCache-D writes a list of cached files to prevent the Unraid mover from moving them back to the array. To enable this:

### Using CA Mover Tuning Plugin

1. Install **CA Mover Tuning** from Community Apps (if not already installed)
2. Go to **Settings** → **Mover Tuning**
3. Set **File exclusion path** to:
   ```
   /mnt/user/appdata/plexcache/plexcache_cached_files.txt
   ```
4. Click **Apply**

Now the Unraid mover will skip files that PlexCache-D has cached.

### How It Works

```
PlexCache-D writes: /config/plexcache_cached_files.txt
    ↓ (mapped to host)
Host path: /mnt/user/appdata/plexcache/plexcache_cached_files.txt
    ↓ (CA Mover Tuning reads)
Mover skips listed files
```

## Scheduling

PlexCache-D includes a built-in scheduler. Configure it via the Web UI:

1. Go to **Settings** → **Schedule**
2. Enable the scheduler
3. Choose a schedule (presets available or custom cron)
4. Recommended: Every 4 hours (`0 */4 * * *`)

The scheduler runs automatically - no need for User Scripts or external cron jobs.

## Cache Settings

Configure cache behavior via **Settings** → **Cache**:

| Setting | Description |
|---------|-------------|
| **Cache Limit** | Maximum drive usage for caching (e.g., `500GB` or `75%`) |
| **Eviction Mode** | `Smart` (priority-based), `FIFO` (oldest first), or `None` (disabled) |
| **Eviction Threshold** | When to start evicting (% of cache limit) |
| **Minimum Priority** | Only evict files below this score (OnDeck ~90, Watchlist ~70) |
| **Cache Retention** | Hours to keep files before considering for eviction |
| **Watchlist Retention** | Days to keep watchlist items cached |

## Notifications

PlexCache-D supports multiple notification methods. Configure via **Settings** → **Notifications**.

### Notification Types

| Type | Description |
|------|-------------|
| **Webhook** | Discord, Slack, or generic webhooks (recommended for Docker) |
| **Unraid** | Native Unraid notifications (requires optional volume mounts) |
| **Both** | Send to both Unraid and webhook |

### Notification Levels

You can select multiple levels for fine-grained control:

| Level | Description |
|-------|-------------|
| **Summary** | Send summary after every run |
| **Activity** | Send summary only when files are actually moved |
| **Errors** | Notify when errors occur |
| **Warnings** | Notify when warnings occur |

**Recommended:** Use **Activity** to only get notified when PlexCache-D actually does something.

### Webhook Setup (Discord/Slack)

1. Create a webhook in your Discord/Slack channel
2. Paste the URL in **Settings** → **Notifications** → **Webhook URL**
3. Select your notification levels
4. Click **Test** to verify

### Unraid Notifications in Docker

By default, Docker containers cannot access Unraid's notification system. To enable native Unraid notifications:

1. Add the optional volume mounts (see [Optional Volume Mappings](#optional-volume-mappings-unraid-notifications) above)
2. Restart the container
3. In **Settings** → **Notifications**, select **Both** or **Unraid**
4. Select your notification levels

If the mounts are not configured, PlexCache-D will gracefully fall back to webhook-only notifications.

## Manual Operations

### Run Now
Click the **Run Now** button in the Web UI to trigger an immediate cache operation.

### CLI Access
```bash
# Dry run (preview without moving files)
docker exec plexcache-d python3 plexcache.py --dry-run

# Verbose output
docker exec plexcache-d python3 plexcache.py --verbose

# Show cache priorities
docker exec plexcache-d python3 plexcache.py --show-priorities
```

## Web UI Features

- **Dashboard**: Status overview, recent activity, Plex connection status
- **Cached Files**: Browse all cached files with filters and search
- **Storage**: Drive analytics and cache breakdown
- **Maintenance**: Health audit and one-click fixes
- **Settings**: Configuration management
- **Logs**: Real-time log viewer

## Troubleshooting

### Container Won't Start

1. Check Docker logs: `docker logs plexcache-d`
2. Verify all paths exist on the host
3. Ensure PUID/PGID have proper permissions

### Plex Connection Failed

1. Verify Plex URL is accessible from the container
2. Check the Plex token is valid
3. Ensure Plex is running and network allows connection

### Files Not Being Cached

1. Verify library paths in Plex match the container mounts
2. Check that the cache drive has space
3. Review logs in Web UI for errors

### Mover Moving Cached Files

1. Confirm CA Mover Tuning is configured with the correct path
2. Check the exclude file exists: `/mnt/user/appdata/plexcache/plexcache_cached_files.txt`
3. Verify the file contains your cached media paths

### Permission Issues / "Path not writable" Error

1. **Verify all volume mappings are read-write** - Do NOT use `:ro` for media paths
2. Ensure PUID/PGID match your media file ownership (usually 99:100 on Unraid)
3. Check the appdata folder permissions
4. Verify container can read/write to `/mnt/cache`, `/mnt/user0`, AND `/mnt/user`

If you see "Path /mnt/user/... is not writable", check your Docker container configuration:
- `/mnt/user` must be mapped as read-write (not read-only)

### "File not found" Warnings

- This usually means Plex has stale metadata for a renamed/upgraded file
- Trigger a Plex library refresh/scan for the affected library
- The warning is harmless - PlexCache skips files that don't exist

## Migrating from CLI/User Scripts

If you were running PlexCache-D via User Scripts or the CLI version:

### Option A: Import Folder (Recommended)

1. Copy your CLI files to the import folder:
   ```bash
   mkdir -p /mnt/user/appdata/plexcache/import
   cp /path/to/plexcache_settings.json /mnt/user/appdata/plexcache/import/
   cp /path/to/plexcache_cached_files.txt /mnt/user/appdata/plexcache/import/
   cp -r /path/to/data /mnt/user/appdata/plexcache/import/
   ```

2. Start the container and access the Web UI

3. Go to **Settings** → **Import** - the wizard will detect your import files and offer to migrate them automatically

4. Verify your Plex connection (URL may differ in Docker)

### Option B: Direct Copy

1. **Copy existing config directly**:
   ```bash
   mkdir -p /mnt/user/appdata/plexcache/data
   cp /path/to/plexcache_settings.json /mnt/user/appdata/plexcache/
   cp -r /path/to/data/* /mnt/user/appdata/plexcache/data/
   ```

2. **Install Docker container** (see Installation above)

3. **Disable User Script schedule** - the container scheduler handles this now

4. **Verify operation** via Web UI dashboard

## Files and Directories

After installation, your `/mnt/user/appdata/plexcache` folder will contain:

```
/mnt/user/appdata/plexcache/
├── plexcache_settings.json              # Configuration
├── plexcache_cached_files.txt            # Cached files list
├── data/
│   ├── timestamps.json                   # Cache timestamps
│   ├── ondeck_tracker.json               # OnDeck tracking
│   ├── watchlist_tracker.json            # Watchlist tracking
│   └── user_tokens.json                  # User auth tokens
├── logs/
│   └── plexcache.log                     # Application logs
└── import/                               # Drop CLI files here for migration
```

## Support

- **Issues**: [GitHub Issues](https://github.com/StudioNirin/PlexCache-D/issues)
- **Documentation**: [GitHub Repository](https://github.com/StudioNirin/PlexCache-D)

## Version Info

To check the running version:
```bash
docker exec plexcache-d python3 -c "from core import __version__; print(__version__)"
```
