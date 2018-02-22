# coding=utf-8
#
# This code reads the file of commands to execute and creates an sqlite3 database and driver table, for easy shipment
#  to dolphin.
#

import sqlite3
import os
import datetime

database_name = 'dolphincommands-singletest.sqlite3'
command_filename = 'executioncommands-singletest.txt'

'''
This function creates our transaction database. It assumes that the database does not already exist, because
we always delete any existing database within the main driver loop. Bare bones...
'''


def create_database_table(db_name):
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute('''
CREATE TABLE transactions (
    docid            TEXT (40)   NOT NULL,
    update_sql       TEXT (400)  NOT NULL
                                 PRIMARY KEY
                                 UNIQUE,
    del_and_link_cmd TEXT (300)  NOT NULL,
    is_processed     BOOLEAN (1)
);
''')
    db.commit()

    # cursor.execute('''CREATE UNIQUE INDEX sql_index ON transactions(docid ASC, update_sql ASC);''')

    db.close()


'''
This function reads the "executioncommands.txt" file, 3 lines at a time and inserts them into the transaction table
It assumes the commands are in "docid, sql, link" order.
'''


def insert_command_file(cmd_filename):
    count = 0
    db = sqlite3.connect(database_name)
    cursor = db.cursor()
    with open(cmd_filename, encoding='utf-8') as f:
        for line in f:
            count += 1
            docid = line
            # assert (len(docid) <= 37) # stop if we find a weird docid
            sqlcmd = f.readline().replace("\t", "")  # strip embedded tabs out
            assert(sqlcmd.startswith("UPDATE ")) # stop if we don't find an update where we expected
            linkcmd = f.readline()
            assert(linkcmd.startswith("del ")) # stop if we don't find a delete where we expected
            try:
                cursor.execute('INSERT INTO transactions (docid, update_sql, del_and_link_cmd) VALUES (?,?,?)',
                               (docid, sqlcmd, linkcmd))
                if count % 10000 == 0:
                    print("{0}: {1} records inserted and commited".format(datetime.datetime.now(), count))
                    db.commit()
            except Exception as e:
                pass
                print("{0}\n Insertion of {1} {2} {3}".format(e, docid, sqlcmd, linkcmd))
    db.commit()
    print("Data completed insertion at {0}".format(datetime.datetime.now()))
    db.close()


if __name__ == '__main__':
    print("Start insertion at {0}".format(datetime.datetime.now()))
    try:
        os.remove(database_name)  # remove it if an old one exists
    except FileNotFoundError as e:
        pass

    create_database_table(database_name)  # create the database.
    insert_command_file(command_filename)  # read the list of commands to be executed and insert into sqlite3 table

    print("Data inserted and indexes built at {0}".format(datetime.datetime.now()))
    # we're done!
