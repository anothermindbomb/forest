# coding=utf-8
"""
Walk the directory tree, starting at "source" build up a dictionary of file types, with a count of each.

Steve 10/11/2017
"""

import os

# source = r"H:\testdata\F0"
source = r'D:\\'
filetypes = {}

if __name__ == '__main__':

    for root, dirs, files in os.walk(source):
        for file in files:
            filename, suffix = os.path.splitext(file)
            if filetypes.get(suffix) is None:
                filetypes[suffix] = 1
            else:
                filetypes[suffix] = filetypes.get(suffix) + 1

for eachentry in sorted(filetypes):
    print(eachentry, ":", filetypes[eachentry])
