# OpenVMS FTP Sync
This is a script to synchronize a (failing) OpenVMS server to local POSIX-style filesystems.

This does translate the versioning file system for OpenVMS to Unix-style filenames

This was tested against HP OpenVMS 8.3, not sure about the details of other FTP servers on VMS

## Use in Cronicle/Docker etc
Output JSON adapted to run as a plugin in Cronicle with rudimentary progress

Reads keys from config.json and override them with environment variables prefixed with VMSFTP_

# Usage
## From config.json
./sync.py 

## From environment variables
export VMSFTP_USERNAME="your_username"
export VMSFTP_PASSWORD="your_password"
export VMSFTP_HOSTNAME="your_hostname"
export VMSFTP_SOURCE="your_source_directory"
export VMSFTP_DESTINATION="your_destination_directory"
export VMSFTP_DEBUG="true"
export VMSFTP_RECURSIVE="true"
export VMSFTP_TRY_NLST="false"
export VMSFTP_TIMEOUT_LIST="60"
export VMSFTP_TIMEOUT_NLST="60"
./sync.py

# Configuration
Username: your_username
Password: your_password
Hostname: your_hostname
Source: your_source_directory in Unix format (eg. /dk0/your_directory translates to dk0:[your_directory])
Destination: your_destination_directory (local)
Debug: true/false - prints more information
Recursive: true/false - syncs recursively
Try NLST: true/false - fall back to use NLST if LIST fails. LIST gives more information such as datetime, but may fail on large directories.
Timeout LIST: 60 - timeout in seconds for LIST command
Timeout NLST: 60 - timeout in seconds for NLST command
