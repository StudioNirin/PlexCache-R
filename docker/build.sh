#!/bin/bash
# PlexCache-R Docker Build Script
#
# Usage:
#   ./docker/build.sh              # Build with default tag
#   ./docker/build.sh v3.0.0       # Build with specific version tag
#   ./docker/build.sh latest dev   # Build with multiple tags

set -e

# Configuration
IMAGE_NAME="ghcr.io/studionirin/plexcache-d"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default tag
VERSION="${1:-latest}"

# Get git commit hash
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build from project root with docker/Dockerfile
echo "Building PlexCache-R Docker image..."
echo "  Image: ${IMAGE_NAME}:${VERSION}"
echo "  Commit: ${GIT_COMMIT}"
echo "  Context: ${PROJECT_ROOT}"
echo ""

cd "$PROJECT_ROOT"

# Fix line endings for Linux compatibility (Windows creates CRLF)
sed -i 's/\r$//' docker/docker-entrypoint.sh

# Build the image with version tracking build args
docker build \
    -f docker/Dockerfile \
    --build-arg GIT_COMMIT="${GIT_COMMIT}" \
    --build-arg IMAGE_TAG="${VERSION}" \
    -t "${IMAGE_NAME}:${VERSION}" \
    .

# Add additional tags if provided
shift || true
for tag in "$@"; do
    echo "Adding tag: ${IMAGE_NAME}:${tag}"
    docker tag "${IMAGE_NAME}:${VERSION}" "${IMAGE_NAME}:${tag}"
done

echo ""
echo "Build complete!"
echo "  Image: ${IMAGE_NAME}:${VERSION}"
echo ""
echo "To run:"
echo "  docker run -d -p 5757:5757 ${IMAGE_NAME}:${VERSION}"
echo ""
echo "Or use docker-compose:"
echo "  cd docker && docker-compose up -d"
