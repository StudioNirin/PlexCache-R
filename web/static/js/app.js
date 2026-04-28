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

// Auto-dismiss alerts marked with `.alert-auto-dismiss`.
// Centralized here so every success/info action-result alert — regardless of
// which router or template emits it — fades out after 4s without each caller
// duplicating the setTimeout. Runs on initial load and after every HTMX swap.
function _pcScheduleAutoDismiss(root) {
    var scope = root || document;
    var alerts = scope.querySelectorAll('.alert-auto-dismiss');
    for (var i = 0; i < alerts.length; i++) {
        var el = alerts[i];
        if (el.dataset.autoDismissScheduled === '1') continue;
        el.dataset.autoDismissScheduled = '1';
        (function(alert) {
            setTimeout(function() {
                alert.classList.add('alert-fade-out');
                setTimeout(function() { if (alert.parentNode) alert.remove(); }, 300);
            }, 4000);
        })(el);
    }
}
document.addEventListener('DOMContentLoaded', function() { _pcScheduleAutoDismiss(document); });
document.addEventListener('htmx:afterSettle', function(e) { _pcScheduleAutoDismiss(e.target); });

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

// Path-mapping view/edit toggle. Shared by settings/libraries, settings/paths,
// and the path_mapping_card partial (which is HTMX-swapped — relying on the
// globally-loaded function avoids redefining it per card render).
function toggleEditMode(index) {
    var view = document.getElementById('mapping-view-' + index);
    var edit = document.getElementById('mapping-edit-' + index);
    if (!view || !edit) return;
    if (view.style.display === 'none') {
        view.style.display = 'block';
        edit.style.display = 'none';
    } else {
        view.style.display = 'none';
        edit.style.display = 'block';
    }
    lucide.createIcons();
}
