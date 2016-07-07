#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import signal
import requests

# Constant
HttpStatusOk = 200
HttpStatusCreated = 201

# Config
base_url = "http://10.19.16.30:14000/webhdfs/v1/lijie"
auth_str = "user.name=lijie"
max_recursive_file = 1000

# Statistics
stats = {
    "uploaded": 0,
    "processed": 0,
}
is_exited = False


def is_hdfs_exist(path_and_filename):
    if path_and_filename[0] == "/":
        path_and_filename = path_and_filename[1:]
    url = base_url + os.sep + path_and_filename + "?op=GETFILESTATUS&" + auth_str
    # print ("is_hdfs_exist url:", url)
    response = requests.get(url)
    if int(response.status_code) == HttpStatusOk:
        return True
    else:
        return False


def get_hdfs_path(root, path_and_filename):
    return path_and_filename.replace(root, "")


def hdfs_mkdirs(dir):
    if dir[0] == "/":
        dir = dir[1:]
    url = base_url + os.sep + dir + "?op=MKDIRS&" + auth_str
    # print ("mkdir url:", url)
    response = requests.put(url)
    if int(response.status_code) == HttpStatusOk:
        print ("%s mkdir ok" % (dir))
    else:
        print ("%s mkdir failed" % (dir))


def hdfs_upload(root, path_and_filename):
    files = {'file': open(path_and_filename, 'rb')}
    path_and_filename = get_hdfs_path(root, path_and_filename)
    if path_and_filename[0] == "/":
        path_and_filename = path_and_filename[1:]
    if is_hdfs_exist(path_and_filename):
        print ("%s existed" % (path_and_filename))
        return False
    url = base_url + os.sep + path_and_filename + "?op=CREATE&" + auth_str + "&data=true"
    headers = {"Content-Type": "application/octet-stream"}
    response = requests.put(url, files=files, headers=headers)
    # print ("upload url:", url)
    if int(response.status_code) == HttpStatusCreated:
        print ("%s upload ok" % (path_and_filename))
        return True
    else:
        print ("%s upload failed" % (path_and_filename))
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
        if stats["processed"] >= max_recursive_file:
            sys.exit(1)
        path = dir + os.sep + f
        if os.path.isdir(path):
            print ("[d] %s" %(path))
            hdfs_mkdirs(get_hdfs_path(root, path))
            recursive(root, path)
        elif os.path.isfile(path):
            if is_hdfs_exist(get_hdfs_path(root, path)):
                stats["uploaded"] = stats["uploaded"] + 1
                print ("[f][uploaded] %s" %(path))
                continue
            else:
                print ("[f][uploading] %s" %(path))
            hdfs_upload(dir, path)
            stats["processed"] = stats["processed"] + 1
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
