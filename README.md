# PlexCache-R V3.0: Automate Plex Media Management
### Updated 1/7/26

## Current Bugs / Todo List

Now moved to a discussion page [HERE](https://github.com/StudioNirin/PlexCache-R/discussions/16)

## Overview
Automate Plex media management: Efficiently transfer media from the On Deck/Watchlist to the cache, and seamlessly move watched media back to their respective locations.
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements. Hopefully fixed and improved anyway, time will tell!

PlexCache efficiently transfers media from the On Deck/Watchlist to the cache and moves watched media back to their respective locations. This Python script reduces energy consumption by minimizing the need to spin up the array/hard drive(s) when watching recurrent media like TV series. It achieves this by moving the media from the OnDeck and watchlist for the main user and/or other users. For TV shows/anime, it also fetches the next specified number of episodes.

## Features
The original PlexCache app only worked for local users for most features, due to API limitations. V1 of Plexcache-r had some similar limitations, but a lot of those have now been fixed.

- Fetch a specified number of episodes from the "onDeck" for the main user and other users (Local/Remote).
- Skip fetching onDeck media for specified users (Local/Remote).
- Fetch a specified number of episodes from the "watchlist" for the main user and other users (Local/Remote).
- Skip fetching watchlist media for specified users (Local/Remote).
- (New v2) - .plexcached backup system, so files are not moved off the array and are instead archived to prevent unecessary move operations.
- Search only the specified libraries.
- Check for free space before moving any file.
- (New v2) - Cache retention policies, with automatic removals based on age/priority settings.
- (New v3) - **Web UI Dashboard** - Browser-based interface for monitoring and configuration.
- (New v3) - **Scheduled Runs** - Automatic execution with interval or cron scheduling.
- (New v3) - **Enhanced Webhooks** - Discord and Slack rich message formatting.
- Move watched media present on the cache drive back to the array.
- Move respective subtitles along with the media moved to or from the cache.
- Filter media older than a specified number of days.
- Run in debug mode for testing.
- Use of a log file for easy debugging.
- Use caching system to avoid wastful memory usage and cpu cycles.
- Use of multitasking to optimize file transfer time.
- Exit the script if any active session or skip the currently playing media.
- Send Webhook messages according to set log level (untested).
- (New v2) - Unraid Mover exclusion file. This file also allows for manual custom entries. 



  
### Project Structure

```
PlexCache-R/
├── plexcache.py              # CLI entry point
├── plexcache_web.py          # Web UI entry point
├── core/                     # Core application modules
│   ├── app.py                # Main orchestrator (PlexCacheApp class)
│   ├── setup.py              # Interactive setup wizard
│   ├── config.py             # Configuration management (dataclasses)
│   ├── logging_config.py     # Logging, rotation, notification handlers
│   ├── system_utils.py       # OS detection, path conversions
│   ├── plex_api.py           # Plex server interactions
│   └── file_operations.py    # File moving, filtering, subtitles
├── web/                      # Web UI (FastAPI + HTMX)
│   ├── main.py               # FastAPI application
│   ├── routers/              # Route handlers
│   ├── services/             # Business logic
│   ├── templates/            # Jinja2 templates (Plex theme)
│   └── static/               # CSS, JS assets
├── tools/                    # Diagnostic utilities
│   └── audit_cache.py        # Cache diagnostic tool
├── data/                     # Runtime tracking files (auto-created)
└── logs/                     # Log files
```

## Web UI (New in V3.0)

PlexCache-R now includes a browser-based dashboard for monitoring and configuration.

**Start the Web UI:**
```bash
python3 plexcache_web.py                 # Start on localhost:5000
python3 plexcache_web.py --host 0.0.0.0  # Listen on all interfaces
python3 plexcache_web.py --port 8080     # Custom port
```

**Features:**
- **Dashboard** - Real-time cache stats, Plex connection status, recent activity feed
- **Cached Files** - Sortable file browser with filters, eviction controls
- **Storage** - Drive analytics, breakdowns by source, largest/oldest files
- **Settings** - Full configuration UI with Plex OAuth, library selection, user toggles
- **Schedule** - Automatic runs with interval or cron expressions
- **Logs** - Real-time log viewer with search, filters, and live streaming

**Tech Stack:** FastAPI, HTMX, Jinja2, Plex-inspired dark theme

## Installation and Setup

Please check out our [Wiki section](https://github.com/StudioNirin/PlexCache-R/wiki) for the step-by-step guide on how to setup PlexCache on your system. The WIKI should cover basically everything. If something doesn't make sense, or doesn't work, please open a new issue for it. But don't be upset if the answer is in the WIKI and we mock you for not reading it thoroughly first. 

## Notes

This script might be compatible with other systems, especially Linux-based ones, although I have primarily tested it on Unraid with plex as docker container. While I cannot support every case, it's worth checking the GitHub issues to see if your specific case has already been discussed. Particularly worth checking the original Bexem repo issues page. 
I will still try to help out, but please note that I make no promises in providing assistance for every scenario.
**It is highly advised to use the setup script.**

## Disclaimer

This script comes without any warranties, guarantees, or magic powers. By using this script, you accept that you're responsible for any consequences that may result. The author will not be held liable for data loss, corruption, or any other problems you may encounter. So, it's on you to make sure you have backups and test this script thoroughly before you unleash its awesome power.

## Acknowledgments

It seems we all owe a debt of thanks to someone called brimur[^3] for providing the script that served as the foundation and inspiration for this project. That was long before my time on it though, the first iteration I saw was by bexem[^4], who also has my thanks. But the biggest contributor to this continuation of the project was by bbergle[^5], who put in all the work on refactoring and cleaning up all the code into bite-sized chunks that were understandable to a novice like myself. All I did then was go through it all and try and make the wierd janky Plex API actually kinda work, for what I needed it to do anyway!

And my first personal thankyou to [Brandon-Haney](https://github.com/Brandon-Haney) who has contributed a whole bunch of updates. I haven't yet merged them as of writing this, but he's gone through basically every file so I figured he deserved a pre-emptive thanks!


[^1]: Remote users do not have individual watchlists accessible by the API. It's unfortunately not a thing. So instead I am using the available RSS feed as a workaround. The downside of this is... 
[^2]: ...that it is an all-or-nothing proposal for remote users. Local users can still be toggled on a per-user basis.
[^3]: [brimur/preCachePlexOnDeckEpiosodes.py](https://gist.github.com/brimur/95277e75ca399d5d52b61e6aa192d1cd)
[^4]: https://github.com/bexem/PlexCache
[^5]: https://github.com/BBergle/PlexCache




