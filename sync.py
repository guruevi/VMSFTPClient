#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
See README.md for more information
"""
import ftplib
import json
from time import mktime
from os import path, makedirs, utime, environ, rename
from datetime import datetime
from signal import alarm, signal, SIGALRM


# from pprint import pprint


def timeout_handler(*_):
    """Handle timeouts"""
    raise ftplib.error_temp("Command timed out")


def set_timeout(timeout: int):
    """Set the timeout for the next command connection"""
    signal(SIGALRM, timeout_handler)
    alarm(timeout)


def unset_timeout():
    """Disable the timeout for the next command connection"""
    alarm(0)


def change_dir(directory: str, ftp: ftplib.FTP):
    global CURRENT_DIRECTORY

    if CURRENT_DIRECTORY == directory:
        return True

    if not CURRENT_DIRECTORY:
        CURRENT_DIRECTORY = ROOT_DIRECTORY

    print(f"Changing directories to {directory}")
    set_timeout(5)
    try:
        ftp.cwd(directory)
    except ftplib.error_temp:
        print("Timeout changing directories")
        return False
    except ftplib.error_perm:
        print("Invalid directory")
        return False
    finally:
        unset_timeout()

    CURRENT_DIRECTORY = directory
    return True


def download(file_obj: dict, ftp: ftplib.FTP):
    """Download a file from the ftp server."""

    # Remove root_directory from the parent_directory
    path_without_root = file_obj["parent_directory"].replace(ROOT_DIRECTORY, "")
    destination_path = path.join(DESTINATION, *path_without_root.split("/"))
    destination_file = path.join(destination_path, file_obj["name"])
    destination_file_temp = f"{destination_file}.part"

    if int(file_obj["version"]) > 1:
        destination_file = f"{destination_file}_v{file_obj['version']}"

    # Check if the file already exists
    if path.exists(destination_file):
        if path.getmtime(destination_file) == file_obj["creation"]:
            print(f"File {file_obj['name']} already exists with same date")
            return

    try:
        makedirs(destination_path, exist_ok=True)
    except PermissionError:
        print(json.dumps({"complete": 1, "code": 535, "description": "Permission error"}))
        exit(535)
    except OSError as e:
        print(e)
        print(json.dumps({"complete": 1, "code": 553, "description": "Directory name error"}))
        exit(553)

    # We can make empty directories now
    if file_obj["type"] == "dir":
        return

    if not change_dir(file_obj["parent_directory"], ftp):
        return

    print(f"Downloading {file_obj['name']} - v{file_obj['version']}")
    try:
        ftp.retrbinary(f'RETR {file_obj["name"]};{file_obj["version"]}', open(destination_file_temp, 'wb').write)
        # rename the file to the correct name
        rename(destination_file_temp, destination_file)
    except ftplib.error_temp:
        print("Temporary error downloading file")
        return
    except ftplib.error_perm:
        print("Invalid file")
        return

    # Set timestamp on destination_file
    utime(destination_file, (file_obj["creation"], file_obj["creation"]))

    print(f"Downloaded {file_obj['name']} to {destination_file}")


def parse_list_output(line: str, list_of_files: list):
    """Parse the output of the list command."""
    global CURRENT_DIRECTORY
    global PREVIOUS_LINE

    if not line or line.startswith("Directory") or line.startswith("Total"):
        return

    if "(" not in line and ")" not in line:
        PREVIOUS_LINE = line
        return

    if PREVIOUS_LINE:
        line = PREVIOUS_LINE + line
        PREVIOUS_LINE = ""

    lines = line.split()
    filename_details = lines[0].split(";")

    filename = filename_details[0]
    version = filename_details[1]

    filetype = "file"
    if ".DIR;" in lines[0]:
        filetype = "dir"
        filename = filename.replace('.DIR', '')

    # This is irrelevant as it doesn't match Unix type blocks
    # block_size = int(lines[1].split("/")[0])

    if len(lines) > 1:
        creation_time = mktime(datetime.strptime(f"{lines[2]} {lines[3]}", "%d-%b-%Y %H:%M:%S").timetuple())
    else:
        # Set creation time to 1970 to indicate but also don't re-download
        creation_time = 0

    list_of_files.append(
        {
            "parent_directory": CURRENT_DIRECTORY,
            "name": filename,
            "version": version,
            # "bytes": block_size * 512,
            "creation": creation_time,
            "type": filetype
        }
    )


def fetch_dirs(directory: str, ftp: ftplib.FTP):
    global PREVIOUS_LINE

    if not change_dir(directory, ftp):
        return

    print(f"Scanning {directory}")

    # List the directory and callback the function to parse each line
    list_of_files = []

    def parse_list(line: str):
        """ This is a helper function to keep list_of_files local to this instance of fetch_dirs"""
        parse_list_output(line, list_of_files)

    try_nlst = False
    # Run the DIR command for 5m before timing out
    set_timeout(60)
    try:
        ftp.dir(parse_list)
    except ftplib.error_perm:
        print("Invalid directory")
        return
    except ftplib.error_temp:
        print("Timeout listing directory, trying NLST")
        try_nlst = True
    finally:
        unset_timeout()
        PREVIOUS_LINE = ""

    # The server is braindead and large directories time out. NLST is faster but doesn't give metadata information.
    if try_nlst and config['try_nlst']:
        signal(SIGALRM, timeout_handler)
        set_timeout(120)
        try:
            close_connection(ftp)
            ftp = open_connection()
            change_dir(directory, ftp)
            files = ftp.nlst()
        except ftplib.error_temp:
            print("Timeout listing directory")
            return
        finally:
            unset_timeout()
            PREVIOUS_LINE = ""

        for f in files:
            parse_list_output(f, list_of_files)

    ALL_FILES.extend(list_of_files)

    close_connection(ftp)

    # Loop through the list of files and query every subdirectory
    for file_obj in list_of_files:
        if file_obj["type"] != "file" and config['recursive']:
            connection = open_connection()
            fetch_dirs(f"{file_obj['parent_directory']}/{file_obj['name']}", connection)


def open_connection():
    # Open ftp connection
    try:
        ftp = ftplib.FTP(config["hostname"], timeout=60)
        ftp.login(config["username"], config["password"])
    except ConnectionRefusedError:
        print(json.dumps({"complete": 1, "code": 10061, "description": "Connection Refused"}))
        exit(10061)
    except ftplib.error_perm:
        print(json.dumps({"complete": 1, "code": 430, "description": "Connection Refused"}))
        exit(430)

    return ftp


def close_connection(ftp: ftplib.FTP):
    global CURRENT_DIRECTORY

    CURRENT_DIRECTORY = ""
    ftp.close()


# Open config.json
try:
    config = json.load(open("config.json"))
except FileNotFoundError:
    print("default config.json not found")
    exit(1)

# Environment variables overwrite the keys in config
for key in config:
    config[key] = environ.get(f"VMSFTP_{key.upper()}", config[key])

# Set variables
ROOT_DIRECTORY = config["source"]
CURRENT_DIRECTORY = ""
DESTINATION = config["destination"]
PREVIOUS_LINE = ""
ALL_FILES = []
signal(SIGALRM, timeout_handler)

fetch_dirs(ROOT_DIRECTORY, open_connection())

counter = 0
file_count = len(ALL_FILES)
print(f"Found {file_count} file objects")

conn = open_connection()
for file in ALL_FILES:
    download(file, conn)

    percentage = round(counter / file_count, 1)
    print(json.dumps({"progress": percentage}))
    counter += 1

close_connection(conn)

print(json.dumps({"progress": 1}))
print(json.dumps({"complete": 1, "code": 0}))
