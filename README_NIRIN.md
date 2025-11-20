# PlexCache - Updated 11/2025
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements.


### Core Modules

- **`config.py`**: Configuration management with dataclasses for type safety
- **`logging_config.py`**: Logging setup, rotation, and notification handlers
- **`system_utils.py`**: OS detection, path conversions, and file utilities
- **`plex_api.py`**: Plex server interactions and cache management
- **`file_operations.py`**: File moving, filtering, and subtitle operations
- **`plexcache_app.py`**: Main application orchestrator


## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the setup script to configure PlexCache:
```bash
python plexcache_setup.py
```

3. Run the main application:
```bash
python plexcache_app.py
```

## Usage

### Command Line Options

- `--debug`: Run in debug mode (no files will be moved)
- `--skip-cache`: Skip using cached data and fetch fresh from Plex


### Examples - My current UserScript in UnRaid. 
```bash
#!/bin/bash
cd /mnt/user/appdata/plexcache
pip3 install -r requirements.txt
python3 /mnt/user/appdata/plexcache/plexcache_app.py --skip-cache
```
Reasons for this, is that python dependencies by default don't survive past a system reboot, so you have to install them every time you reboot the system. 
This is probably best done via a separate user script, but my way works for now. 


## Migration from Original

The refactored version maintained full compatibility with the original.
HOWEVER - This Redux version DOES NOT maintain full compatibility. 
I did make some vague efforts at the start, but there were so many things that didn't work properly that it just wasn't feasible. 
So while the files used are the same, you -will- need to delete your `plexcache_settings.json` and run a new setup to create a new one. 

1. **Different Configuration**: Uses the same `plexcache_settings.json` file, but the fields have changed
2. **Added Functionality**: All original features still exist, but now also work (where possible) for remote users, not just local. 
3. **Same Output**: Logging and notifications work identically
4. **Same Performance**: No performance degradation. Hopefully. Don't quote me on this. 



## Changelog

- **11/25 - Handling of script_folder link**: Old version had a hardcoded link to the script folder instead of using the user-defined setting.
- **11/25 - Adding logic so a 401 error when looking for watched-media doesn't cause breaking errors**: Seems it's only possible to get 'watched files' data from home users and not remote friends, and the 401 error would stop the script working? Added some logic to plex_api.py.
- **11/25 - Ended up totally changing several functions, and adding some new ones, to fix all the issues with remote users and watchlists and various other things**: So the changelog became way too difficult to maintain at this point cos it was just a bunch of stuff. Hence this changing to a new version of PlexCache. 
