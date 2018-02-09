# coding=utf-8
#
# This code reads the file of commands to execute and creates an sqlite3 database and driver table, for easy shipment
#  to dolphin.
#

import sqlite3
import os

database_name = 'dolphincommands'
command_filename = 'executioncommands.txt'

'''
This function creates our transaction database. It assumes that the database does not already exist, because
we always delete any existing database within the main driver loop. Bare bones...
'''


def create_database_table(db_name):
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute('''
CREATE TABLE transactions
(
  docid TEXT
    PRIMARY KEY,
  update_sql TEXT,
  del_and_link_cmd TEXT,
  is_processed BOOLEAN,
  error_message TEXT
)''')
    db.commit()
    db.close()


'''
This function reads the "executioncommands.txt" file, 3 lines at a time and inserts them into the transaction table
It assumes the commands are in "docid, sql, link" order and the docid must be unique, as it's our primary key.

Any duplicate docid, will simply cause this to error out. It's one-shot code so any duplicate docid needs
to be looked at anyway, so we don't bother with any error handling.  
'''


def insert_command_file(cmd_filename):
    db = sqlite3.connect(database_name)
    cursor = db.cursor()
    with open(cmd_filename) as f:
        for line in f:
            docid = line
            sqlcmd = f.readline()
            linkcmd = f.readline()
            cursor.execute('INSERT INTO transactions (docid, update_sql, del_and_link_cmd) VALUES (?,?,?)',
                           (docid, sqlcmd, linkcmd))
            db.commit()
    db.close()


if __name__ == '__main__':
    os.remove(database_name)  # remove it if an old one exists
    create_database_table(database_name)  # create the database.
    insert_command_file(command_filename)  # read the list of commands to be executed ad stick them in the sqlite3 table

    # we're done!
