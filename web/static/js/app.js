/**
 * PlexCache-D Web UI JavaScript
 * Shared utilities and HTMX error handling
 */

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

// Handle showAlert event from HX-Trigger response header
document.addEventListener('showAlert', function(event) {
    const detail = event.detail || {};
    const type = detail.type || 'warning';
    const message = detail.message || 'Something went wrong';
    const alertContainer = document.getElementById('alert-container');
    if (alertContainer) {
        alertContainer.innerHTML = `
            <div class="alert alert-${type}" id="hx-trigger-alert">
                <i data-lucide="${type === 'success' ? 'check-circle' : type === 'error' ? 'alert-circle' : 'alert-triangle'}"></i>
                <span>${message}</span>
            </div>
        `;
        lucide.createIcons();
        setTimeout(function() {
            var el = document.getElementById('hx-trigger-alert');
            if (el) el.remove();
        }, 5000);
    }
});


