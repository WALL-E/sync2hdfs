#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""sync folder to hdfs using http restful api.

Usage:
  sync2hdfs.py [-hvq] [--force-upload] [--base-url=url] [--username=name] [--max-recursive=max] PATH...
  sync2hdfs.py --version

Arguments:
  PATH  destination path

Options:
  --base-url=url       WebHDFS REST API Endpoint
  --username=name      the authenticated user is the username specified in the user.name query parameter
  --max-recursive=max  max files each run
  -h --help            show this help message and exit
  --version            show version and exit
  -v --verbose         print status messages
  -q --quiet           report only file names

"""

import os
import sys
import signal
import requests
from docopt import docopt

ROOT = os.path.dirname(__file__)
sys.path.append(ROOT)

base_url = "http://10.19.16.30:14000/webhdfs/v1/lijie"
username = "lijie"
max_recursive = 1000
force_upload = False
quiet = False

# Constant
HttpStatusOk = 200
HttpStatusCreated = 201

# Statistics
stats = {
    "scan": 0,
    "upload_ok": 0,
    "upload_fail": 0,
    "existed": 0,
}
is_exited = False


def is_hdfs_exist(filename):
    url = base_url + os.sep + filename + "?op=GETFILESTATUS&" + "user.name=" + "username"
    response = requests.get(url)
    if int(response.status_code) == HttpStatusOk:
        return True
    else:
        return False


def get_hdfs_path(root, src):
    dst = src.replace(root, "")
    if dst[0] == os.sep:
        dst = dst[1:]
    return dst


def hdfs_mkdirs(dir):
    url = base_url + os.sep + dir + "?op=MKDIRS&" + "user.name=" + "username"
    # print ("mkdir url:", url)
    response = requests.put(url)
    if int(response.status_code) == HttpStatusOk:
        print ("%s mkdir ok" % (dir))
    else:
        print ("%s mkdir failed" % (dir))


def hdfs_upload(root, filename):
    data = open(filename, "rb").read()
    dst = get_hdfs_path(root, filename)
    url = base_url + os.sep + dst + "?op=CREATE&" + "user.name=" + "username" + "&data=true"
    headers = {"Content-Type": "application/octet-stream"}
    response = requests.put(url, data=data, headers=headers)
    # print ("upload url:", url)
    if int(response.status_code) == HttpStatusCreated:
        # print ("%s upload ok" % (filename))
        stats["upload_ok"] = stats["upload_ok"] + 1
        return True
    else:
        # print ("%s upload failed" % (filename))
        stats["upload_fail"] = stats["upload_fail"] + 1
        return False


def recursive(root, dir):
    try:
        files = os.listdir(dir)
    except OSError, msg:
        print (msg)
        sys.exit(1)
    files.sort()
    for f in files:
        if is_exited:
            return
        # ignore
        if f[0] == ".":
            continue
        if stats["scan"] >= max_recursive:
            sys.exit(1)
        path = dir + os.sep + f
        if os.path.isdir(path):
            if quiet:
                print("[d] %s" % (path))
            dst = get_hdfs_path(root, path)
            if not is_hdfs_exist(dst):
                hdfs_mkdirs(dst)
            recursive(root, path)
        elif os.path.isfile(path):
            stats["scan"] = stats["scan"] + 1
            if not force_upload and is_hdfs_exist(get_hdfs_path(root, path)):
                stats["existed"] = stats["existed"] + 1
                if quiet:
                    print("[f][existed] %s" % (path))
                continue
            else:
                if quiet:
                    print("[f][uploading] %s" % (path))
            hdfs_upload(root, path)
        else:
            if quiet:
                print ("[?][unknow] %s" % (path))
            sys.exit(1)


def usage():
    print ("Usage:")
    print ("  %s directory-to-sync" % (sys.argv[0]))


def onsignal_term(signum, frame):
    global is_exited
    print ("Receive [%s] signal" % (signum))
    is_exited = True


def rebuild_options(arguments):
    global force_upload
    global base_url
    global username
    global max_recursive
    global quiet
    if arguments["--force-upload"]:
        force_upload = True
    if arguments["--base-url"]:
        base_url = arguments["--base-url"]
    if arguments["--username"]:
        username = arguments["--username"]
    if arguments["--max-recursive"]:
        max_recursive = arguments["--max-recursive"]
    if arguments["--quiet"]:
        quiet = arguments["--quiet"]


def main():
    signal.signal(signal.SIGINT, onsignal_term)

    arguments = docopt(__doc__, version='1.0.0rc1')
    rebuild_options(arguments)

    for src in arguments["PATH"]:
        recursive(src, src)

    if arguments["--verbose"]:
        print("#")
        print("# Config:")
        print("#")
        print("max_recursive:[%s]" % (max_recursive))
        print("#")
        print("# Statistics:")
        print("#")
        print(stats)

if __name__ == '__main__':
    main()
