#!/bin/sh
# Turn the encoded password from config/db.ini back into plain text.
# Base64 only hides the password from a quick look at the file. It is NOT
# encryption: protect config/db.ini with file permissions (chmod 600).
printf '%s' "$1" | base64 -d
