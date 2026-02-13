# PlexCache-D Dockerfile
# Intelligent Plex media caching for Unraid
FROM python:3.11-slim

# Labels for Unraid/OCI
LABEL maintainer="Brandon Haney"
LABEL org.opencontainers.image.title="PlexCache-D"
LABEL org.opencontainers.image.description="Intelligent Plex media caching for Unraid"
LABEL org.opencontainers.image.url="https://github.com/StudioNirin/PlexCache-D"
LABEL org.opencontainers.image.source="https://github.com/StudioNirin/PlexCache-D"
LABEL org.opencontainers.image.vendor="Brandon Haney"

# Build args for version tracking
ARG GIT_COMMIT=unknown
ARG IMAGE_TAG=latest

# Environment defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=America/Los_Angeles \
    WEB_PORT=5757 \
    LOG_LEVEL=INFO \
    GIT_COMMIT=$GIT_COMMIT \
    IMAGE_TAG=$IMAGE_TAG

# Install system dependencies
# php-cli is needed for Unraid's notify script (optional mount for Unraid notifications)
RUN apt-get update && apt-get install -y --no-install-recommends \
    tini \
    tzdata \
    curl \
    php-cli \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (from project root context)
COPY core/ ./core/
COPY web/ ./web/
COPY tools/ ./tools/
COPY plexcache.py .

# Copy entrypoint script and make executable
COPY docker/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Create directories for config persistence
RUN mkdir -p /config /app/logs /app/data && \
    chmod -R 777 /config /app/logs /app/data

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${WEB_PORT}/api/health || exit 1

# Expose web UI port
EXPOSE 5757

# Use tini as init system
ENTRYPOINT ["/usr/bin/tini", "--"]

# Start script handles user switching and app startup
CMD ["/docker-entrypoint.sh"]
