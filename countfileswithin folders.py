# coding=utf-8
"""
Walk the directory tree, starting at "source". Count the number of files within each directory

Steve 10/11/2017
"""

import os.path

source = r"H:/testdata/"
# source = r'D:\\'

if __name__ == '__main__':

    for root, dirs, files in os.walk(source):
        for dir in dirs:
            filecount = len(os.listdir(os.path.join(root, dir)))
            print("Directory = {}, number of files = {}".format(os.path.join(root, dir), filecount))
