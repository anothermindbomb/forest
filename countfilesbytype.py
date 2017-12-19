# coding=utf-8
"""
Walk the directory tree, starting at "source" build up a dictionary of file types, with a count of each.

Steve 10/11/2017
"""

import os

source = r'D:/output/'
file_types = {}

if __name__ == '__main__':

    for root, dirs, files in os.walk(source):
        for file in files:
            filename, suffix = os.path.splitext(file)
            suffix = suffix.lower()  # ensure case insensitivity
            if file_types.get(suffix) is None:
                file_types[suffix] = 1
            else:
                file_types[suffix] = file_types.get(suffix) + 1

for each_entry in sorted(file_types):
    print(each_entry, ":", file_types[each_entry])
