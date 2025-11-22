# PlexCache: Automate Plex Media Management - Updated 11/25

Automate Plex media management: Efficiently transfer media from the On Deck/Watchlist to the cache, and seamlessly move watched media back to their respective locations.
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements.

## Overview

PlexCache efficiently transfers media from the On Deck/Watchlist to the cache and moves watched media back to their respective locations. This Python script reduces energy consumption by minimizing the need to spin up the array/hard drive(s) when watching recurrent media like TV series. It achieves this by moving the media from the OnDeck and watchlist for the main user and/or other users. For TV shows/anime, it also fetches the next specified number of episodes.

## Features

- Fetch a specified number of episodes from the "onDeck" for the main user and other users.
- Skip fetching onDeck media for specified users.
- Fetch a specified number of episodes from the "watchlist" for the main user and other users.
- Skip fetching watchlist media for specified users.
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
- Send Webhook messages according to set log level.
- Find your missing unicorn.
  
### Core Modules

- **`config.py`**: Configuration management with dataclasses for type safety
- **`logging_config.py`**: Logging setup, rotation, and notification handlers
- **`system_utils.py`**: OS detection, path conversions, and file utilities
- **`plex_api.py`**: Plex server interactions and cache management
- **`file_operations.py`**: File moving, filtering, and subtitle operations
- **`plexcache_app.py`**: Main application orchestrator


## Installation

AlienTech42 has already done a really helpful video on the original PlexCache installation, and for now it's the best resource. 
The install process is pretty much the same for PlexCache-R. However there are some settings in the setup.py that
are either in a different place, or are completely removed/altered/added. So don't follow the video religiously!
https://www.youtube.com/watch?v=9oAnJJY8NH0

1. Put the files from this Repo into a known folder on your Unraid server. I use the following:
   ```bash
   /mnt/user/appdata/plexcache/plexcache_app.py
   ```
   I'll keep using this in my examples, but make sure to use your own path.
   
2. Open up the Unraid Terminal, and install dependencies:
```bash
cd ../mnt/user/appdata/plexcache
pip3 install -r requirements.txt
```
Note: You'll need python installed for this to work. There's a community app for that. 

3. Run the setup script to configure PlexCache:
```bash
python3 plexcache_setup.py
```
Each of the questions should pretty much explain themselves, but I'll keep working on them. 
Or I'll add a guide list on here sometime. 

4. Run the main application:
```bash
python3 plexcache_app.py
```
However you wouldn't really want to run it manually every time, and the dependencies will disappear every time you reset the server. 
So I recommend making the following UserScript:
```bash
#!/bin/bash
cd /mnt/user/appdata/plexcache
pip3 install -r requirements.txt
python3 /mnt/user/appdata/plexcache/plexcache_app.py --skip-cache
```
And set it on a cron job to run whenever you want. I run it once a day at midnight ( 0 0 * * * )


### Command Line Options

- `--debug`: Run in debug mode (no files will be moved)
- `--skip-cache`: Skip using cached data and fetch fresh from Plex



## Migration from Original

The refactored version maintained full compatibility with the original.
HOWEVER - This Redux version DOES NOT maintain full compatibility. 
I did make some vague efforts at the start, but there were so many things that didn't work properly that it just wasn't feasible. 
So while the files used are the same, you -will- need to delete your `plexcache_settings.json` and run a new setup to create a new one. 

1. **Different Configuration**: Uses the same `plexcache_settings.json` file, but the fields have changed
2. **Added Functionality**: All original features still exist, but now also work (where possible) for remote users, not just local. 
3. **Same Output**: Logging and notifications work identically
4. **Same Performance**: No performance degradation. Hopefully. Don't quote me on this. 


## Setup

Please check out our [Wiki section](https://github.com/bexem/PlexCache/wiki) for the step-by-step guide on how to setup PlexCache on your system. 

## Notes

This script should be compatible with other systems, especially Linux-based ones, although I have primarily tested it on Unraid with plex as docker container running on Unraid. Work has been done to improve Windows interoperability.
While I cannot  support every case, it's worth checking the GitHub issues to see if your specific case has already been discussed.
I will still try to help out, but please note that I make no promises in providing assistance for every scenario.
**It is highly advised to use the setup script.**

## Disclaimer

This script comes without any warranties, guarantees, or magic powers. By using this script, you accept that you're responsible for any consequences that may result. The author will not be held liable for data loss, corruption, or any other problems you may encounter. So, it's on you to make sure you have backups and test this script thoroughly before you unleash its awesome power.

## Acknowledgments

It seems we all owe a debt of thanks to someone called brimur[^1] for providing the script that served as the foundation and inspiration for this project. That was long before my time on it though, the first iteration I saw was by bexem[^2], who also has my thanks. But the biggest contributor to this continuation of the project was by bbergle[^3], who put in all the work on refactoring and cleaning up all the code into bite-sized chunks that were understandable to a novice like myself. All I did then was go through it all and try and make the wierd janky Plex API actually kinda work, for what I needed it to do anyway!

[^1]: [brimur/preCachePlexOnDeckEpiosodes.py](https://gist.github.com/brimur/95277e75ca399d5d52b61e6aa192d1cd)
[^2]: https://github.com/bexem/PlexCache
[^3]: https://github.com/BBergle/PlexCache



## Changelog

- **11/25 - Handling of script_folder link**: Old version had a hardcoded link to the script folder instead of using the user-defined setting.
- **11/25 - Adding logic so a 401 error when looking for watched-media doesn't cause breaking errors**: Seems it's only possible to get 'watched files' data from home users and not remote friends, and the 401 error would stop the script working? Added some logic to plex_api.py.
- **11/25 - Ended up totally changing several functions, and adding some new ones, to fix all the issues with remote users and watchlists and various other things**: So the changelog became way too difficult to maintain at this point cos it was just a bunch of stuff. Hence this changing to a new version of PlexCache. 
