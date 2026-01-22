# PlexCache-R Unraid Setup Guide

This guide covers installing and configuring PlexCache-R on Unraid.

## Overview

PlexCache-R automatically caches your frequently-accessed Plex media (OnDeck and Watchlist items) to your cache drive. This reduces array spinups and improves playback performance by keeping actively-watched content on fast storage.

## Prerequisites

- Unraid 6.9 or later
- Plex Media Server running (accessible from Docker)
- Cache drive configured
- Docker service enabled

## Installation

### Option 1: Community Apps (Recommended)

1. Open the **Apps** tab in Unraid
2. Search for "PlexCache-R"
3. Click **Install**
4. Configure the paths (see below)
5. Click **Apply**

### Option 2: Manual Docker Installation

1. Go to **Docker** tab → **Add Container**
2. Set the following:
   - **Repository**: `brandonhaney/plexcache-r`
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
- All media paths (`/mnt/cache`, `/mnt/user0`, `/mnt/user`) must be **read-write** for PlexCache-R to move files between cache and array
- These paths **must match exactly** between container and host for Plex path resolution to work correctly

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
| Web UI | 5757 | PlexCache-R web interface |

## First Run Setup

1. After starting the container, open the Web UI at `http://[UNRAID_IP]:5757`
2. The Setup Wizard will launch automatically
3. Follow the 6-step wizard:

### Step 1: Welcome
Introduction to PlexCache-R features.

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

PlexCache-R writes a list of cached files to prevent the Unraid mover from moving them back to the array. To enable this:

### Using CA Mover Tuning Plugin

1. Install **CA Mover Tuning** from Community Apps (if not already installed)
2. Go to **Settings** → **Mover Tuning**
3. Set **File exclusion path** to:
   ```
   /mnt/user/appdata/plexcache/plexcache_mover_files_to_exclude.txt
   ```
4. Click **Apply**

Now the Unraid mover will skip files that PlexCache-R has cached.

### How It Works

```
PlexCache-R writes: /config/plexcache_mover_files_to_exclude.txt
    ↓ (mapped to host)
Host path: /mnt/user/appdata/plexcache/plexcache_mover_files_to_exclude.txt
    ↓ (CA Mover Tuning reads)
Mover skips listed files
```

## Scheduling

PlexCache-R includes a built-in scheduler. Configure it via the Web UI:

1. Go to **Settings** → **Schedule**
2. Enable the scheduler
3. Choose a schedule (presets available or custom cron)
4. Recommended: Every 4 hours (`0 */4 * * *`)

The scheduler runs automatically - no need for User Scripts or external cron jobs.

## Notifications

PlexCache-R supports multiple notification methods. Configure via **Settings** → **Notifications**.

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

**Recommended:** Use **Activity** to only get notified when PlexCache-R actually does something.

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

If the mounts are not configured, PlexCache-R will gracefully fall back to webhook-only notifications.

## Manual Operations

### Run Now
Click the **Run Now** button in the Web UI to trigger an immediate cache operation.

### CLI Access
```bash
# Dry run (preview without moving files)
docker exec plexcache-r python3 plexcache.py --dry-run

# Verbose output
docker exec plexcache-r python3 plexcache.py --verbose

# Show cache priorities
docker exec plexcache-r python3 plexcache.py --show-priorities
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

1. Check Docker logs: `docker logs plexcache-r`
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
2. Check the exclude file exists: `/mnt/user/appdata/plexcache/plexcache_mover_files_to_exclude.txt`
3. Verify the file contains your cached media paths

### Permission Issues / "Path not writable" Error

1. **Verify all volume mappings are read-write** - Do NOT use `:ro` for media paths
2. Ensure PUID/PGID match your media file ownership (usually 99:100 on Unraid)
3. Check the appdata folder permissions
4. Verify container can read/write to `/mnt/cache`, `/mnt/user0`, AND `/mnt/user`

If you see "Path /mnt/user/... is not writable", check your Docker container configuration:
- `/mnt/user` must be mapped as read-write (not read-only)

## Migrating from User Scripts

If you were running PlexCache-R via User Scripts:

1. **Backup existing config**:
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
├── plexcache_mover_files_to_exclude.txt # Mover exclude list
├── data/
│   ├── timestamps.json                   # Cache timestamps
│   ├── ondeck_tracker.json               # OnDeck tracking
│   ├── watchlist_tracker.json            # Watchlist tracking
│   └── user_tokens.json                  # User auth tokens
└── logs/
    └── plexcache.log                     # Application logs
```

## Support

- **Issues**: [GitHub Issues](https://github.com/StudioNirin/PlexCache-R/issues)
- **Documentation**: [GitHub Repository](https://github.com/StudioNirin/PlexCache-R)

## Version Info

To check the running version:
```bash
docker exec plexcache-r python3 -c "from core import __version__; print(__version__)"
```
