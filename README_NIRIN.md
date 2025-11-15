# PlexCache - Updated 11/2025
An updated version of the "PlexCache-Refactored" script with various bugfixes and improvements.

## Changelog

- **11/25 - Handling of script_folder link**: Old version had a hardcoded link to the script folder instead of using the user-defined setting.
- **11/25 - Adding delay to user watchlist requests**: To try and prevent "Failed to switch to user Skipping...Rate limit exceeded." errors with multiple users.
- **11/25 - Adding logic so a 401 error when looking for watched-media doesn't cause breaking errors**: Seems it's only possible to get 'watched files' data from home users and not remote friends, and the 401 error would stop the script working? Added some logic to plex_api.py.

