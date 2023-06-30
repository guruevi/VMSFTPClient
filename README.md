# OpenVMS FTP Sync
This is a script to synchronize a (failing) OpenVMS server to local POSIX-style filesystems.

This does translate the versioning file system for OpenVMS to Unix-style filenames

This was tested against OpenVMS 8.3, not sure about the details of other FTP servers on VMS

## Use in Cronicle/Docker etc
Output JSON adapted to run as a plugin in Cronicle with rudimentary progress

Reads keys from config.json and override them with environment variables prefixed with VMSFTP_