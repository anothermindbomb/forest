# coding=utf-8
"""
Walk the directory tree, starting at "source". Count the number of files within each directory

Steve 10/11/2017
"""

import os.path

source = r"H:/testdata/"
# source = r'D:\\'

if __name__ == '__main__':

    total = 0
    for root, dirs, files in os.walk(source):
        for eachdir in dirs:
            filecount = 0
            filelist = os.scandir(os.path.join(root, eachdir))
            for eachfile in filelist:
                if eachfile.is_file():
                    filecount = filecount + 1  # i only want to count files, not directories.
                    total = total + 1
            print("Directory = {}, number of files = {}".format(os.path.join(root, eachdir), filecount))

    print("Total filecount = {}".format(total))
