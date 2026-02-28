/**
 * PathBrowser — lightweight directory autocomplete for path inputs.
 *
 * Self-attaches to all elements with class="path-browse-input".
 * Fetches directory listings from GET /api/browse?path=...
 * Re-initializes on htmx:afterSwap and <details> toggle to survive
 * HTMX partial swaps and lazy-revealed form sections.
 *
 * Uses `var` (not const) to survive HTMX re-declarations per project convention.
 */
var PathBrowser = PathBrowser || {
    _debounceTimers: new WeakMap(),
    _dropdowns: new WeakMap(),
    _lastResults: new WeakMap(),
    DEBOUNCE_MS: 150,
    MAX_VISIBLE: 20,

    init: function() {
        document.querySelectorAll('.path-browse-input').forEach(function(input) {
            if (input._pathBrowserAttached) return;
            input._pathBrowserAttached = true;
            PathBrowser._attach(input);
        });
    },

    _attach: function(input) {
        // Create dropdown container
        var dropdown = document.createElement('div');
        dropdown.className = 'path-browser-dropdown';
        dropdown.style.display = 'none';

        // Position relative to parent
        var wrapper = input.parentElement;
        if (getComputedStyle(wrapper).position === 'static') {
            wrapper.style.position = 'relative';
        }
        wrapper.appendChild(dropdown);
        PathBrowser._dropdowns.set(input, dropdown);

        // Input handler (debounced)
        input.addEventListener('input', function() {
            PathBrowser._debounce(input, function() {
                PathBrowser._onInput(input);
            });
        });

        // Re-show dropdown on focus if we have cached results
        input.addEventListener('focus', function() {
            var last = PathBrowser._lastResults.get(input);
            if (last && input.value) {
                // Re-compute what to show based on current value
                var value = input.value;
                var browsePath, filterPrefix;
                if (value.endsWith('/')) {
                    browsePath = value;
                    filterPrefix = '';
                } else {
                    var lastSlash = value.lastIndexOf('/');
                    browsePath = value.substring(0, lastSlash + 1);
                    filterPrefix = value.substring(lastSlash + 1).toLowerCase();
                }

                if (last.basePath === browsePath) {
                    var dirs = last.directories;
                    if (filterPrefix) {
                        dirs = dirs.filter(function(d) {
                            return d.toLowerCase().startsWith(filterPrefix);
                        });
                    }
                    if (dirs.length > 0) {
                        PathBrowser._show(input, browsePath, dirs.slice(0, PathBrowser.MAX_VISIBLE));
                        return;
                    }
                }
                // Cache miss — fetch fresh
                PathBrowser._onInput(input);
            } else if (input.value && input.value.startsWith('/')) {
                // No cache, but has a value — fetch
                PathBrowser._onInput(input);
            }
        });

        // Keyboard navigation
        input.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                PathBrowser._hide(input);
            } else if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
                var dd = PathBrowser._dropdowns.get(input);
                if (dd && dd.style.display !== 'none') {
                    e.preventDefault();
                    PathBrowser._navigate(dd, e.key === 'ArrowDown' ? 1 : -1);
                }
            } else if (e.key === 'Enter') {
                var dd = PathBrowser._dropdowns.get(input);
                if (dd && dd.style.display !== 'none') {
                    var active = dd.querySelector('.path-browser-item-active');
                    if (active) {
                        e.preventDefault();
                        active.click();
                    }
                }
            }
        });

        // Hide on blur (with delay for click registration)
        input.addEventListener('blur', function() {
            setTimeout(function() {
                PathBrowser._hide(input);
            }, 200);
        });
    },

    _debounce: function(input, fn) {
        var existing = PathBrowser._debounceTimers.get(input);
        if (existing) clearTimeout(existing);
        PathBrowser._debounceTimers.set(input, setTimeout(fn, PathBrowser.DEBOUNCE_MS));
    },

    _onInput: function(input) {
        var value = input.value;
        if (!value || !value.startsWith('/')) {
            PathBrowser._hide(input);
            return;
        }

        // Determine browse path and filter prefix
        var browsePath, filterPrefix;
        if (value.endsWith('/')) {
            browsePath = value;
            filterPrefix = '';
        } else {
            var lastSlash = value.lastIndexOf('/');
            browsePath = value.substring(0, lastSlash + 1);
            filterPrefix = value.substring(lastSlash + 1).toLowerCase();
        }

        if (!browsePath) {
            PathBrowser._hide(input);
            return;
        }

        // Check if we already have cached results for this base path
        var last = PathBrowser._lastResults.get(input);
        if (last && last.basePath === browsePath) {
            var dirs = last.directories;
            if (filterPrefix) {
                dirs = dirs.filter(function(d) {
                    return d.toLowerCase().startsWith(filterPrefix);
                });
            }
            if (dirs.length === 0) {
                PathBrowser._hide(input);
            } else {
                PathBrowser._show(input, browsePath, dirs.slice(0, PathBrowser.MAX_VISIBLE));
            }
            return;
        }

        fetch('/api/browse?path=' + encodeURIComponent(browsePath))
            .then(function(response) {
                if (!response.ok) {
                    PathBrowser._hide(input);
                    return null;
                }
                return response.json();
            })
            .then(function(data) {
                if (!data || !data.directories) {
                    PathBrowser._hide(input);
                    return;
                }

                // Cache the full result set for this base path
                PathBrowser._lastResults.set(input, {
                    basePath: browsePath,
                    directories: data.directories
                });

                var dirs = data.directories;
                if (filterPrefix) {
                    dirs = dirs.filter(function(d) {
                        return d.toLowerCase().startsWith(filterPrefix);
                    });
                }

                if (dirs.length === 0) {
                    PathBrowser._hide(input);
                    return;
                }

                PathBrowser._show(input, browsePath, dirs.slice(0, PathBrowser.MAX_VISIBLE));
            })
            .catch(function() {
                PathBrowser._hide(input);
            });
    },

    _show: function(input, basePath, dirs) {
        var dropdown = PathBrowser._dropdowns.get(input);
        if (!dropdown) return;

        dropdown.innerHTML = '';
        dirs.forEach(function(dir) {
            var item = document.createElement('div');
            item.className = 'path-browser-item';
            item.textContent = dir + '/';
            item.addEventListener('mousedown', function(e) {
                e.preventDefault(); // Prevent blur
                input.value = basePath + dir + '/';
                // Clear cache so next input triggers a fresh fetch for the new directory
                PathBrowser._lastResults.delete(input);
                PathBrowser._hide(input);
                // Trigger input event to re-fetch
                input.dispatchEvent(new Event('input', { bubbles: true }));
            });
            dropdown.appendChild(item);
        });

        dropdown.style.display = 'block';
    },

    _hide: function(input) {
        var dropdown = PathBrowser._dropdowns.get(input);
        if (dropdown) {
            dropdown.style.display = 'none';
        }
    },

    _navigate: function(dropdown, direction) {
        var items = dropdown.querySelectorAll('.path-browser-item');
        if (!items.length) return;

        var activeIdx = -1;
        items.forEach(function(item, i) {
            if (item.classList.contains('path-browser-item-active')) {
                activeIdx = i;
                item.classList.remove('path-browser-item-active');
            }
        });

        var newIdx = activeIdx + direction;
        if (newIdx < 0) newIdx = items.length - 1;
        if (newIdx >= items.length) newIdx = 0;

        items[newIdx].classList.add('path-browser-item-active');
        items[newIdx].scrollIntoView({ block: 'nearest' });
    }
};

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', function() {
    PathBrowser.init();
});

// Re-initialize after HTMX swaps
document.addEventListener('htmx:afterSwap', function() {
    PathBrowser.init();
});

// Re-initialize when <details> elements are toggled open (lazy-revealed forms)
document.addEventListener('toggle', function(e) {
    if (e.target.open) {
        PathBrowser.init();
    }
}, true);
