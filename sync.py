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
from typing import List, Dict


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
    global PREVIOUS_LINE

    PREVIOUS_LINE = ""

    if CURRENT_DIRECTORY == directory:
        return True

    if not CURRENT_DIRECTORY:
        CURRENT_DIRECTORY = ROOT_DIRECTORY

    print_debug(f"Changing directories to {directory}")

    try:
        ftp.cwd(directory)
    except ftplib.error_temp:
        print("Timeout changing directories")
        return False
    except ftplib.error_perm:
        print("Invalid directory")
        return False

    CURRENT_DIRECTORY = directory
    return True


def print_debug(param):
    """Print debug information"""
    if DEBUG:
        print(param)


def download(file_obj: dict, ftp: ftplib.FTP) -> bool:
    """Download a file from the ftp server."""

    print_debug(file_obj)

    if int(file_obj["version"]) > 1:
        destination_file = f'{file_obj["name"]}_v{file_obj["version"]}{file_obj["type"]}'
    else:
        destination_file = f'{file_obj["name"]}{file_obj["type"]}'

    # Remove root_directory from the parent
    path_without_root = file_obj["parent"].replace(ROOT_DIRECTORY, "")
    destination_path = path.join(DESTINATION, *path_without_root.split("/"))
    destination = path.join(destination_path, destination_file).replace('.DIR', '')
    destination_part = f"{destination}.part"

    # Check if the file already exists
    if path.exists(destination):
        if path.getmtime(destination) == file_obj["creation"]:
            print_debug(f"File {file_obj['name']} already exists with same date")
            return True

    try:
        makedirs(destination_path, exist_ok=True)
    except PermissionError:
        print(json.dumps({"complete": 1, "code": 535, "description": "Permission error"}))
        exit(535)
    except OSError as e:
        print(e)
        print(json.dumps({"complete": 1, "code": 553, "description": "Directory name error"}))
        exit(553)

    # We make an empty directory
    if file_obj["type"] == ".DIR":
        makedirs(destination, exist_ok=True)
        print_debug(f"Created directory {destination}")
        # Set timestamp on directory
        utime(destination, (file_obj["creation"], file_obj["creation"]))
        # We are done now
        return True

    if not change_dir(file_obj["parent"], ftp):
        return False

    # Open a filepointer for writing
    if file_obj["type"] in [".TXT", ".LOG", ".CSV"]:
        fp = open(destination_part, 'w')
        bin_mode = False
    else:
        fp = open(destination_part, 'wb')
        bin_mode = True

    def write_callback_nl(data):
        fp.write(data)
        fp.write('\n')

    print(f"Downloading {file_obj['name']} - v{file_obj['version']}")
    try:
        if bin_mode:
            ftp.retrbinary(f'RETR {file_obj["name"]}{file_obj["type"]};{file_obj["version"]}', fp.write)
        else:
            ftp.retrlines(f'RETR {file_obj["name"]}{file_obj["type"]};{file_obj["version"]}', write_callback_nl)
    except ftplib.error_temp:
        print("Temporary error downloading file")
        return False
    except ftplib.error_perm:
        print("Cannot download file (no longer exists?)")
        return False

    fp.close()

    # rename the file to the correct name
    rename(destination_part, destination)

    # Set timestamp on destination_file
    utime(destination, (file_obj["creation"], file_obj["creation"]))

    print_debug(f"Downloaded {file_obj['name']} to {destination}")
    return True


def parse_list_output(line: str, curr_dir: str):
    """Parse the output of the list command."""
    global PREVIOUS_LINE

    print_debug(line)

    if not line or line.startswith("Directory") or line.startswith("Total"):
        return

    if "(" not in line and ")" not in line:
        PREVIOUS_LINE = line
        return

    if PREVIOUS_LINE:
        line = PREVIOUS_LINE + line
        PREVIOUS_LINE = ""

    lines = line.split()
    filename_w_version = lines[0].split(";")

    filename = filename_w_version[0]
    version = filename_w_version[1]

    if len(lines) > 1:
        creation_time = mktime(datetime.strptime(f"{lines[2]} {lines[3]}", "%d-%b-%Y %H:%M:%S").timetuple())
    else:
        # Set creation time to 1970 to indicate it was completed but no times were present
        creation_time = 0

    filename_parts = path.splitext(filename)

    obj = {
        "parent": curr_dir,
        "name": filename_parts[0],
        "version": version,
        "creation": creation_time,
        "type": filename_parts[1]
    }
    print_debug(obj)
    return obj


def fetch_dirs(directory: str, ftp: ftplib.FTP):
    """ Fetch a list of files from the ftp server. """
    if not change_dir(directory, ftp):
        return

    print(f"Scanning {directory}")

    # Temporary list of files for this call
    list_of_files: list[dict[str, str | float | int] | None] = []

    def parse_list(line: str):
        """ This is a helper function to keep list_of_files and directory local to this instance of fetch_dirs"""
        parsed = parse_list_output(line, directory)
        if parsed:
            list_of_files.append(parsed)

    try_nlst = False

    # Run the DIR command
    set_timeout(CONFIG.get("timeout_list", 60))
    try:
        ftp.dir(parse_list)
    except ftplib.error_perm:
        print("Invalid directory")
        return
    except ftplib.error_temp:
        print("Timeout listing directory with LIST")
        try_nlst = CONFIG.get("try_nlst", False)
    finally:
        unset_timeout()

    # The server is braindead and large directories time out. NLST is faster but doesn't give metadata information.
    if try_nlst:
        set_timeout(CONFIG.get("timeout_nlst", 60))
        try:
            change_dir(directory, ftp)
            files = ftp.nlst()
            for f in files:
                list_of_files.append(parse_list_output(f, directory))
        except ftplib.error_temp:
            print("Timeout listing directory with NLST")
            return
        finally:
            unset_timeout()

    ALL_FILES.extend(list_of_files)

    print_debug(list_of_files)
    # Loop through the list of files and query every subdirectory
    for file_obj in list_of_files:
        print_debug(file_obj)
        if file_obj["type"] == ".DIR" and CONFIG.get("recursive", True):
            fetch_dirs(f"{file_obj['parent']}/{file_obj['name']}", ftp)


def open_connection():
    # Open ftp connection
    try:
        ftp = ftplib.FTP(CONFIG["hostname"], timeout=60)
        ftp.login(CONFIG["username"], CONFIG["password"])
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


def parse_config():
    # Open CONFIG.json
    try:
        c = json.load(open("config.json"))
    except FileNotFoundError:
        print("config.json not found")
        c = {}

    # Environment variables overwrite the keys in CONFIG
    for key in ["hostname",
                "username",
                "password",
                "source",
                "destination",
                "debug",
                "recursive",
                "try_nlst",
                "timeout_list",
                "timeout_nlst"]:
        env_value = environ.get(f"VMSFTP_{key.upper()}", None)
        if env_value is not None:
            c[key] = env_value
    # Make sure all the required keys are present
    for key in ["hostname", "username", "password", "source", "destination"]:
        if key not in c:
            print(f"Missing {key} in config.json")
            exit(1)

    return c


CONFIG = parse_config()

# Set variables
ROOT_DIRECTORY = CONFIG["source"]
CURRENT_DIRECTORY = ""
DESTINATION = CONFIG["destination"]
DEBUG = CONFIG.get("debug", False)
signal(SIGALRM, timeout_handler)

PREVIOUS_LINE = ""
ALL_FILES = []
connection = open_connection()
fetch_dirs(ROOT_DIRECTORY, connection)

file_count = len(ALL_FILES)
print(f"Found {file_count} file objects")

counter = 0
for file in ALL_FILES:
    download(file, connection)

    print(json.dumps({"progress": round(counter / file_count, 2)}))
    counter += 1

close_connection(connection)

print(json.dumps({"progress": 1}))
print(json.dumps({"complete": 1, "code": 0}))
