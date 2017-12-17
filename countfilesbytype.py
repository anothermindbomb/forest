# coding=utf-8
"""
Walk the directory tree, starting at "source" build up a dictionary of file types,
with a count of each.

Steve 10/11/2017
"""

import os

# source = r"H:\testdata\F0"
SOURCE = r'P:\\'
FILETYPES = {}

if __name__ == '__main__':

    for root, dirs, files in os.walk(SOURCE):
        for file in files:
            filename, suffix = os.path.splitext(file)
            suffix = suffix.lower()
            if FILETYPES.get(suffix) is None:
                FILETYPES[suffix] = 1
            else:
                FILETYPES[suffix] = FILETYPES.get(suffix) + 1

for eachentry in sorted(FILETYPES):
    print(eachentry, ":", FILETYPES[eachentry])
