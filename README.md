# PlexCache: Automate Plex Media Management
### Updated 11/25

## Current Bugs / Todo List

1. Some of the Episode/Show logic looks for 'Season X' as a folder marker, but with sonarr some episodes are also in 'Specials' so the logic needs to be updated to account for it. Maybe add 'Series X' as another option too.
2. Add in a setup protection so people don't use "/" as their root. Or fix the error that occurs if they do that. Either way. 
3. Make it so the Exclude file has a marker, so that a user can manually add in entries above that marker that are never altered (or read) by plexcache. I believe Mover Tuner only allows for a single Exclude file, so this will allow this file to function in that way for users that need it. 
4. Figure out a way to have the exclude file also have files that were already on cache and not moved from array. As at present only files that are moved by the script, get added to the exclude file. This part is easy, but the issue comes in with the current option of "watched files get moved back to array" as you might want those files to stay on cache (if they're recent downloads) so other users can watch them. Tbh it might be better to remove that option, and just have the files removed from the exclude list and remain on cache, and have the users use Mover Tuner to have a lot more customisation for their options for when space gets cleared and files moved. I'm not sure having this script doing the moving back to the array really adds anything. Though I guess it's more for people using this on non-unraid environments and still want some kind of exclude file.  
   Two options:
   1. Have the exclude file be correct, including 'already cached files', and recommend that unraid users have the move-back-after-watched turned off and let Mover handle it.
   2. Have the exclude file be correct, but mark 'already cached files' differently in the file, so instead of being moved back after watched those specific files remain in cache until mover moves them.
   I think 1 is the easiest and tbh probably the solution best for most people. 
   
5.Figure out if its possible/worthwhile to copy files from array to cache, instead of moving them. Leave behind the original file (marked filename.archive or similar). When watched, the cache would only have to delete the cached file and rename the array file, instead of needing to do a longer and more wearing second move. Will need some logic for what to do with files that were already on the cache (as in todo item 3 above) as they won't have .archive files and people may want them to remain on cache longer and be moved by the Mover due to them likely being new media etc.  
Alternatively we could make use of .plexignore files, but I'm not sure how they would interact with series episodes. Easier with movies. 


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



## Changelog

- **11/25 - Handling of script_folder link**: Old version had a hardcoded link to the script folder instead of using the user-defined setting.
- **11/25 - Adding logic so a 401 error when looking for watched-media doesn't cause breaking errors**: Seems it's only possible to get 'watched files' data from home users and not remote friends, and the 401 error would stop the script working? Added some logic to plex_api.py.
- **11/25 - Ended up totally changing several functions, and adding some new ones, to fix all the issues with remote users and watchlists and various other things**: So the changelog became way too difficult to maintain at this point cos it was just a bunch of stuff. Hence this changing to a new version of PlexCache.
- **28/11/25** - Fixed the setup.py script to automatically set the correct plex root directory for media, and the correct media folder paths. These can still be manually corrected if your setup is different/unique. 
