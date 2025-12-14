# PlexCache: Automate Plex Media Management
### Updated 12/13/25

## Current Bugs / Todo List

Now moved to a discussion page [HERE](https://github.com/StudioNirin/PlexCache-R/discussions/16)

## Overview
Automate Plex media management: Efficiently transfer media from the On Deck/Watchlist to the cache, and seamlessly move watched media back to their respective locations.
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements. Hopefully fixed and improved anyway, time will tell!

PlexCache efficiently transfers media from the On Deck/Watchlist to the cache and moves watched media back to their respective locations. This Python script reduces energy consumption by minimizing the need to spin up the array/hard drive(s) when watching recurrent media like TV series. It achieves this by moving the media from the OnDeck and watchlist for the main user and/or other users. For TV shows/anime, it also fetches the next specified number of episodes.

## Features
#### I have added tags to these features to distinguish ones which work for different types of users:  
**Local**: Users on the local or Home account.  
**Remote**: Users that are remote, so friends that you have shared libraries with.  
The original PlexCache app only worked for local users for most features, due to API limitations.

- Fetch a specified number of episodes from the "onDeck" for the main user and other users (Local/Remote).
- Skip fetching onDeck media for specified users (Local/Remote).
- Fetch a specified number of episodes from the "watchlist" for the main user and other users (Local/Remote[^1]).
- Skip fetching watchlist media for specified users (Local/Remote[^2]).
- Search only the specified libraries.
- Check for free space before moving any file.
- Move watched media present on the cache drive back to the array.
- Move respective subtitles along with the media moved to or from the cache.
- Filter media older than a specified number of days.
- Run in debug mode for testing.
- Use of a log file for easy debugging.
- Use caching system to avoid wastful memory usage and cpu cycles.
- Use of multitasking to optimize file transfer time.
- Exit the script if any active session or skip the currently playing media.
- Send Webhook messages according to set log level (untested).



  
### Core Modules

- **`config.py`**: Configuration management with dataclasses for type safety
- **`logging_config.py`**: Logging setup, rotation, and notification handlers
- **`system_utils.py`**: OS detection, path conversions, and file utilities
- **`plex_api.py`**: Plex server interactions and cache management
- **`file_operations.py`**: File moving, filtering, and subtitle operations
- **`plexcache_app.py`**: Main application orchestrator



## Installation and Setup

Please check out our [Wiki section](https://github.com/bexem/PlexCache/wiki) for the step-by-step guide on how to setup PlexCache on your system. The WIKI should cover basically everything. If something doesn't make sense, or doesn't work, please open a new issue for it. But don't be upset if the answer is in the WIKI and we mock you for not reading it thoroughly first. 

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




