#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import requests

#
# curl -i -X PUT "http://10.19.16.30:14000/webhdfs/v1/lijie/o2o_admin?op=MKDIRS&user.name=lijie"
#
# curl -i  "http://10.19.16.30:14000/webhdfs/v1/lijie?op=LISTSTATUS&user.name=lijie"
#
# curl -i --header "Content-Type:application/octet-stream"  -X PUT -T app_o2o_admin.2016-02-01.log "http://10.19.16.30:14000/webhdfs/v1/lijie/o2o_admin/app_o2o_admin.2016-02-01.log?op=CREATE&user.name=lijie&data=true"
#

config = {
    "total": 0,
    "count": 0,
    "recursive_max": 5000
}

root = "/apps/logs/apps"
base_url = "http://10.19.16.30:14000/webhdfs/v1/lijie"
auth_str = "user.name=lijie"


def is_hdfs_exist(path_and_filename):
    if path_and_filename[0] == "/":
        path_and_filename = path_and_filename[1:]
    url = base_url + "/" + path_and_filename + "/" + "?op=GETFILESTATUS&" + auth_str
    # print "is_hdfs_exist url:", url
    response = requests.get(url)
    if int(response.status_code) == 200:
        return True
    else:
        return False


def get_hdfs_path(path_and_filename):
    return path_and_filename.replace(root, "")


def hdfs_mkdirs(dir):
    if dir[0] == "/":
        dir = dir[1:]
    url = base_url + "/" + dir + "?op=MKDIRS&" + auth_str
    # print "mkdir url:", url
    response = requests.put(url)
    if int(response.status_code) == 200:
        print "%s mkdir ok" % (dir)
    else:
        print "%s mkdir failed" % (dir)


def hdfs_upload(path_and_filename):
    files = {'file': open(path_and_filename, 'rb')}
    path_and_filename = get_hdfs_path(path_and_filename)
    if path_and_filename[0] == "/":
        path_and_filename = path_and_filename[1:]
    if is_hdfs_exist(path_and_filename):
        print "%s existed" % (path_and_filename)
        return False
    url = base_url + "/" + path_and_filename + "?op=CREATE&" + auth_str + "&data=true"
    headers = {"Content-Type": "application/octet-stream"}
    response = requests.put(url, files=files, headers=headers)
    # print "upload url:", url
    if int(response.status_code) == 201:
        print "%s upload ok" % (path_and_filename)
        return True
    else:
        print "%s upload failed" % (path_and_filename)
        return False


def recursive(dir):
    files = os.listdir(dir)
    files.sort()
    for f in files:
        # 忽略隐藏文件
        if f[0] == ".":
            continue
        if config["count"] >= config["recursive_max"]:
            sys.exit(1)
        path = dir + os.sep + f
        if os.path.isdir(path):
            print "[d]", path
            hdfs_mkdirs(get_hdfs_path(path))
            recursive(path)
        elif os.path.isfile(path):
            print "[f]", path
            config["total"] = config["total"] + 1
            if is_hdfs_exist(get_hdfs_path(path)):
                print "###", path
                continue
            hdfs_upload(path)
        else:
            print "[?]", path
            sys.exit(1)
        config["count"] = config["count"] + 1
        print "max:[%s], index[%s]" % (config["recursive_max"], config["count"])


def main():
    recursive(root)
    print config

if __name__ == '__main__':
    main()
