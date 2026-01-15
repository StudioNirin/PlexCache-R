#!/bin/bash
set -e

# PlexCache-R Docker Entrypoint
# Runs as root to handle files with any ownership

echo "----------------------------------------"
echo "  PlexCache-R Docker Container"
echo "----------------------------------------"

# Ensure config directory structure exists
mkdir -p /config/data /config/logs /config/import

# Set up symlinks for config and data persistence
# Settings file
if [ ! -L "/app/plexcache_settings.json" ]; then
    if [ -f "/app/plexcache_settings.json" ] && [ ! -f "/config/plexcache_settings.json" ]; then
        mv /app/plexcache_settings.json /config/plexcache_settings.json
    fi
    rm -f /app/plexcache_settings.json 2>/dev/null || true
    ln -sf /config/plexcache_settings.json /app/plexcache_settings.json
fi

# Data directory (timestamps, trackers, etc.)
if [ ! -L "/app/data" ]; then
    if [ -d "/app/data" ] && [ ! -d "/config/data" ]; then
        cp -r /app/data/* /config/data/ 2>/dev/null || true
    fi
    rm -rf /app/data 2>/dev/null || true
    ln -sf /config/data /app/data
fi

# Logs directory
if [ ! -L "/app/logs" ]; then
    rm -rf /app/logs 2>/dev/null || true
    ln -sf /config/logs /app/logs
fi

# Mover exclude files
if [ ! -L "/app/plexcache_mover_files_to_exclude.txt" ]; then
    rm -f /app/plexcache_mover_files_to_exclude.txt 2>/dev/null || true
    ln -sf /config/plexcache_mover_files_to_exclude.txt /app/plexcache_mover_files_to_exclude.txt
fi

if [ ! -L "/app/unraid_mover_exclusions.txt" ]; then
    rm -f /app/unraid_mover_exclusions.txt 2>/dev/null || true
    ln -sf /config/unraid_mover_exclusions.txt /app/unraid_mover_exclusions.txt
fi

echo "Configuration directory: /config"
echo "Data directory: /config/data"
echo "Logs directory: /config/logs"
echo "Import directory: /config/import"
echo ""

# Set timezone
if [ -n "${TZ}" ]; then
    export TZ
    echo "Timezone: ${TZ}"
fi

echo ""
echo "Configuration:"
echo "  Web Port: ${WEB_PORT:-5757}"
echo "  Log Level: ${LOG_LEVEL:-INFO}"
echo ""

if [ -f "/config/plexcache_settings.json" ]; then
    echo "Config file: Found"
else
    echo "Config file: Not found (configure via Web UI)"
fi

echo ""
echo "Starting PlexCache-R Web UI..."
echo "----------------------------------------"

# Start the web application
exec python3 /app/plexcache.py --web --host 0.0.0.0 --port ${WEB_PORT:-5757}
