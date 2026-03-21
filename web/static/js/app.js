/**
 * PlexCache-D Web UI JavaScript
 * Shared utilities and HTMX error handling
 */

// Handle HTMX errors
document.addEventListener('htmx:responseError', function(event) {
    var alertContainer = document.getElementById('alert-container');
    if (alertContainer) {
        var article = document.createElement('article');
        article.className = 'alert alert-error';
        article.textContent = 'Request failed: ' + event.detail.xhr.status + ' ' + event.detail.xhr.statusText;
        var btn = document.createElement('button');
        btn.className = 'close';
        btn.textContent = '\u00d7';
        btn.onclick = function() { article.remove(); };
        article.appendChild(btn);
        alertContainer.innerHTML = '';
        alertContainer.appendChild(article);
    }
});

// Handle showAlert event from HX-Trigger response header
document.addEventListener('showAlert', function(event) {
    var detail = event.detail || {};
    var type = detail.type || 'warning';
    var message = detail.message || 'Something went wrong';
    var alertContainer = document.getElementById('alert-container');
    if (alertContainer) {
        var safeType = ['success', 'error', 'warning', 'info'].indexOf(type) !== -1 ? type : 'warning';
        var iconName = safeType === 'success' ? 'check-circle' : safeType === 'error' ? 'alert-circle' : 'alert-triangle';

        var div = document.createElement('div');
        div.className = 'alert alert-' + safeType;
        div.id = 'hx-trigger-alert';

        var icon = document.createElement('i');
        icon.setAttribute('data-lucide', iconName);
        div.appendChild(icon);

        var span = document.createElement('span');
        span.textContent = message;
        div.appendChild(span);

        alertContainer.innerHTML = '';
        alertContainer.appendChild(div);
        lucide.createIcons();
        setTimeout(function() {
            var el = document.getElementById('hx-trigger-alert');
            if (el) el.remove();
        }, 5000);
    }
});
