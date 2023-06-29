#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
See README.md for more information
"""
import ftplib
import json
from time import mktime
from os import path, makedirs, utime
from datetime import datetime


def change_dir(path: list, ftp):
    ftp.cwd(path)


def download(file_obj: dict):
    """Download a file from the ftp server."""
    global CURRENT_DIRECTORY

    # Remove root_directory from the parent_directory
    path_without_root = file_obj["parent_directory"].replace(ROOT_DIRECTORY, "")
    destination_path = path.join(DESTINATION, *path_without_root.split("/"))
    destination_file = path.join(destination_path, file_obj["name"])

    # Check if the file already exists
    if path.exists(destination_file) and \
            path.getmtime(destination_file) == file_obj["creation"]:
        print(f"File {file_obj['name']} already exists with same date")
        return

    try:
        makedirs(destination_path, exist_ok=True)
    except PermissionError:
        print(json.dumps({"complete": 1, "code": 535, "description": "Permission error"}))
        exit(1)
    except OSError:
        print(json.dumps({"complete": 1, "code": 553, "description": "Directory name error"}))
        exit(553)

    # Switch to a new directory if necessary on the remote
    if CURRENT_DIRECTORY != file_obj["parent_directory"]:
        ftp.cwd(file_obj["parent_directory"])
        CURRENT_DIRECTORY = file_obj["parent_directory"]

    print(f"Downloading {file_obj['name']}")
    ftp.retrbinary("RETR " + file_obj["name"], open(destination_file, 'wb').write)

    # Set timestamp on destination_file
    timestamp: datetime = file_obj["creation"]
    utime(destination_file, (timestamp, timestamp))

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

    block_size = int(lines[1].split("/")[0])

    creation_time = datetime.strptime(f"{lines[2]} {lines[3]}", "%d-%b-%Y %H:%M:%S")

    list_of_files.append(
        {
            "parent_directory": CURRENT_DIRECTORY,
            "name": filename,
            "version": version,
            "bytes": block_size * 512,
            "creation": mktime(creation_time.timetuple()),
            "type": filetype,
            "downloaded": 0
        }
    )


def fetch_dirs():
    global CURRENT_DIRECTORY
    global PREVIOUS_LINE

    ftp.cwd(CURRENT_DIRECTORY)

    # List the directory and callback the function to parse each line
    list_of_files = []
    PREVIOUS_LINE = ""

    def parse_list(line: str):
        """ This is a helper function to keep list_of_files local to this instance of fetch_dirs"""
        parse_list_output(line, list_of_files)

    ftp.dir(parse_list)
    ALL_FILES.extend(list_of_files)

    # Loop through the list of files and query every subdirectory
    for file_obj in list_of_files:
        if file_obj["type"] != "file":
            CURRENT_DIRECTORY = f"{file_obj['parent_directory']}/{file_obj['name']}"
            fetch_dirs()


# Open config.sample.json
config = json.load(open("config.json"))
ROOT_DIRECTORY = CURRENT_DIRECTORY = config["source"]
DESTINATION = config["destination"]
PREVIOUS_LINE = ""
ALL_FILES = []

# Open ftp connection
try:
    ftp = ftplib.FTP(config["hostname"])
    ftp.login(config["username"], config["password"])
except ConnectionRefusedError:
    print(json.dumps({"complete": 1, "code": 10061, "description": "Connection Refused"}))
    exit(10061)
except ftplib.error_perm:
    print(json.dumps({"complete": 1, "code": 430, "description": "Connection Refused"}))
    exit(430)

fetch_dirs()

counter = 0
file_count = len(ALL_FILES)
print(f"Found {file_count} file objects")

for file in ALL_FILES:
    download(file)
    # Format percentage with 2 decimal places
    percentage = "{:.2f}".format(counter / file_count * 100)
    print(json.dumps({"progress": percentage}))
    counter += 1
print(json.dumps({"progress": 1}))
ftp.close()
print(json.dumps({"complete": 1, "code": 0}))
