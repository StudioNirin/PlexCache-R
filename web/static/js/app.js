/**
 * PlexCache-R Web UI JavaScript
 * Minimal JS - primarily for WebSocket handling
 */

// WebSocket Log Viewer class
class LogViewer {
    constructor(containerId, wsUrl) {
        this.container = document.getElementById(containerId);
        this.wsUrl = wsUrl;
        this.ws = null;
        this.autoScroll = true;
        this.connected = false;
    }

    connect() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            return;
        }

        try {
            this.ws = new WebSocket(this.wsUrl);

            this.ws.onopen = () => {
                this.connected = true;
                console.log('WebSocket connected');
                this.appendSystemMessage('Connected to log stream');
            };

            this.ws.onmessage = (event) => {
                this.appendLog(event.data);
            };

            this.ws.onclose = () => {
                this.connected = false;
                console.log('WebSocket disconnected');
                // Attempt reconnect after 3 seconds
                setTimeout(() => this.connect(), 3000);
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };
        } catch (e) {
            console.error('Failed to create WebSocket:', e);
        }
    }

    disconnect() {
        if (this.ws) {
            this.ws.close();
            this.ws = null;
        }
    }

    appendLog(line) {
        if (!this.container) return;

        const codeElement = this.container.querySelector('code') || this.container;
        const lineElement = document.createElement('div');
        lineElement.textContent = line;

        // Determine log level
        let level = 'INFO';
        if (line.includes('ERROR')) {
            lineElement.className = 'log-error';
            level = 'ERROR';
        } else if (line.includes('WARNING')) {
            lineElement.className = 'log-warning';
            level = 'WARNING';
        } else if (line.includes('INFO')) {
            lineElement.className = 'log-info';
            level = 'INFO';
        } else if (line.includes('DEBUG')) {
            lineElement.className = 'log-debug';
            level = 'DEBUG';
        }

        // Apply filter if active
        const filterSelect = document.getElementById('log-level-filter');
        if (filterSelect && filterSelect.value !== 'all') {
            const levelHierarchy = { 'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3 };
            const lineValue = levelHierarchy[level] || 1;
            const filterValue = levelHierarchy[filterSelect.value] || 0;
            // "Debug Only" shows exact match, others show >= level
            if (filterSelect.value === 'DEBUG') {
                if (level !== 'DEBUG') {
                    lineElement.style.display = 'none';
                }
            } else if (lineValue < filterValue) {
                lineElement.style.display = 'none';
            }
        }

        codeElement.appendChild(lineElement);

        if (this.autoScroll) {
            this.container.scrollTop = this.container.scrollHeight;
        }
    }

    appendSystemMessage(message) {
        if (!this.container) return;

        const codeElement = this.container.querySelector('code') || this.container;
        const lineElement = document.createElement('div');
        lineElement.textContent = `--- ${message} ---`;
        lineElement.style.color = 'var(--pico-muted-color)';
        lineElement.style.fontStyle = 'italic';
        codeElement.appendChild(lineElement);
    }

    setAutoScroll(enabled) {
        this.autoScroll = enabled;
    }

    setFilter(level) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({ action: 'filter', level: level }));
        }
    }

    clear() {
        if (this.container) {
            const codeElement = this.container.querySelector('code');
            if (codeElement) {
                codeElement.innerHTML = '';
            }
        }
    }
}

// Global log viewer instance
let logViewer = null;

// Initialize WebSocket log viewer when on logs page
document.addEventListener('DOMContentLoaded', function() {
    const logContent = document.getElementById('log-content');
    const liveUpdatesCheckbox = document.getElementById('live-updates');
    const autoScrollCheckbox = document.getElementById('auto-scroll');

    if (logContent && liveUpdatesCheckbox) {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${wsProtocol}//${window.location.host}/logs/ws`;

        logViewer = new LogViewer('log-content', wsUrl);

        // Toggle live updates
        liveUpdatesCheckbox.addEventListener('change', function() {
            if (this.checked) {
                logViewer.connect();
            } else {
                logViewer.disconnect();
            }
        });

        // Toggle auto-scroll
        if (autoScrollCheckbox) {
            autoScrollCheckbox.addEventListener('change', function() {
                logViewer.setAutoScroll(this.checked);
            });
        }
    }
});

// HTMX event handlers
document.addEventListener('htmx:afterSwap', function(event) {
    // Auto-scroll log content after HTMX swap
    if (event.detail.target.id === 'log-content') {
        const autoScrollCheckbox = document.getElementById('auto-scroll');
        if (autoScrollCheckbox && autoScrollCheckbox.checked) {
            event.detail.target.scrollTop = event.detail.target.scrollHeight;
        }
    }
});

// Handle HTMX errors
document.addEventListener('htmx:responseError', function(event) {
    const alertContainer = document.getElementById('alert-container');
    if (alertContainer) {
        alertContainer.innerHTML = `
            <article class="alert alert-error">
                Request failed: ${event.detail.xhr.status} ${event.detail.xhr.statusText}
                <button class="close" onclick="this.parentElement.remove()">&times;</button>
            </article>
        `;
    }
});

// Utility: Format file size
function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Utility: Format duration
function formatDuration(seconds) {
    if (seconds < 60) return `${seconds.toFixed(1)}s`;
    const minutes = Math.floor(seconds / 60);
    const secs = seconds % 60;
    return `${minutes}m ${secs.toFixed(0)}s`;
}
