#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import signal
import requests

ROOT = os.path.dirname(__file__)
sys.path.append(ROOT)

from config import *

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
    url = base_url + os.sep + filename + "?op=GETFILESTATUS&" + auth_str
    response = requests.get(url)
    if int(response.status_code) == HttpStatusOk:
        return True
    else:
        return False


def get_hdfs_path(root, src):
    dst = src.replace(root, "")
    if dst[0] == "/":
        dst = dst[1:]
    return dst


def hdfs_mkdirs(dir):
    url = base_url + os.sep + dir + "?op=MKDIRS&" + auth_str
    # print ("mkdir url:", url)
    response = requests.put(url)
    if int(response.status_code) == HttpStatusOk:
        print ("%s mkdir ok" % (dir))
    else:
        print ("%s mkdir failed" % (dir))


def hdfs_upload(root, filename):
    data = open(filename, "rb").read()
    dst = get_hdfs_path(root, filename)
    url = base_url + os.sep + dst + "?op=CREATE&" + auth_str + "&data=true"
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
        # 忽略隐藏文件
        if f[0] == ".":
            continue
        if stats["scan"] >= max_recursive_file:
            sys.exit(1)
        path = dir + os.sep + f
        if os.path.isdir(path):
            print ("[d] %s" %(path))
            hdfs_mkdirs(get_hdfs_path(root, path))
            recursive(root, path)
        elif os.path.isfile(path):
            stats["scan"] = stats["scan"] + 1
            if force_upload or is_hdfs_exist(get_hdfs_path(root, path)):
                stats["existed"] = stats["existed"] + 1
                print ("[f][existed] %s" %(path))
                continue
            else:
                print ("[f][uploading] %s" %(path))
            hdfs_upload(root, path)
        else:
            print ("[?][unknow]", path)
            sys.exit(1)


def usage():
    print ("Usage:")
    print ("  %s directory-to-sync" % (sys.argv[0]))


def onsignal_term(signum, frame):
    global is_exited
    print ("Receive [%s] signal" % (signum))
    is_exited = True


def main():
    signal.signal(signal.SIGINT, onsignal_term)
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)
    src = sys.argv[1]
    recursive(src, src)
    print ("#")
    print ("# Config:")
    print ("#")
    print ("max_recursive_file:[%s]" % (max_recursive_file))
    print ("#")
    print ("# Statistics:")
    print ("#")
    print (stats)

if __name__ == '__main__':
    main()
