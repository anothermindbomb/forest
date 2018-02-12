# coding=utf-8
import sqlite3
import subprocess
import datetime
import time
import logging
import pyodbc

sqliteDatabase_name = "dolphincommands"
minutes_to_run = 1  # number of minutes we execute for upon each invocation.
logging_filename = 'dolphin-cleanup' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"
reportfile = datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"
# this connection string will need to be modified for the  Dolphin Database itself...
DolphinDBConnectionString = "Driver={ODBC Driver 13 for SQL Server};Server=DAYSTATE\SQLEXPRESS;Database=DolphinDB;Trusted_Connection=yes;"


def open_database(db_name):
    db = sqlite3.connect(db_name)
    return db


def open_Dolphin_database(DBConnString):
    db = pyodbc.connect(DBConnString)
    return db


def execute_dos_cmd(dos_cmd):
    rc = subprocess.call(dos_cmd, shell=True)
    return rc


def update_dolphin_database(dolphindb, sql):
    try:
        cursor = dolphindb.cursor()
        cursor.execute(sql)
        returned_output = dolphindb.commit() # we just consume the "rows updated" message
    except Exception as e:
        logging.fatal("Dolphin database Failed with {0}, sql = {1}".format(e, sql))
        returned_output = dolphindb.rollback()
        return -1
    return 0


def update_transactions(sqlitedb, docid):
    try:
        cursor = sqlitedb.cursor()
        cursor.execute("update transactions set is_processed = 1 where docid='{0}'".format(docid))
        returned_output = sqlitedb.commit() # we just consume the "rows updated" message
    except Exception as e:
        logging.fatal("Transaction database update failed with {0}, sql = {1}".format(e, sql))
        returned_output = sqlitedb.rollback()
        return -1
    return 0


def produce_run_report():
    print("producing run report")
    cursor.execute('SELECT COUNT(*) FROM transactions WHERE is_processed IS NULL;')
    records_left = cursor.fetchone()[0]
    # cursor.execute('SELECT count(*) FROM transactions WHERE transactions.error_message IS NOT NULL;')
    # total_errors = cursor.fetchone()[0]
    logging.info("{0} records left to process in future runs".format(records_left))
    # logging.info("{1} errors encountered in total".format(total_errors))
    return


# def test_dolphin_retrieval(dolphindb):  # this is only used in development so ensure SQL server connections were working
#     cursor = dolphindb.cursor()
#     cursor.execute("SELECT * FROM dolphintable")
#     output = cursor.fetchall()
#     for line in output:
#         print(line)


if __name__ == '__main__':
    logging.basicConfig(filename=logging_filename, level=logging.DEBUG, format='%(asctime)s %(message)s')
    starttime = time.time()
    endtime = time.time() + (minutes_to_run * 60)

    sqlitedb = open_database(sqliteDatabase_name)
    logging.info("Tranaction Database opened")

    dolphindb = open_Dolphin_database(DolphinDBConnectionString)
    logging.info("Dolphin Database opened")

    # test_dolphin_retrieval(dolphindb)

    cursor = sqlitedb.cursor()
    cursor.execute('SELECT * FROM transactions WHERE is_processed IS NULL;')

    while True:

        if time.time() > endtime:  # quite when we run out of time.
            break

        returned_rows = cursor.fetchmany(1000)

        if len(returned_rows) == 0:  # quit when we run out of transactions.
            break

        for row in returned_rows:

            dos_cmd_rc = -1
            sql_update_rc = -1  # we reset these to non-0 values each time around.

            docid = row[0]
            sql = row[1]
            dos_cmd = row[2]

            dos_cmd_rc = execute_dos_cmd(dos_cmd)

            # we only want to try to update the database if the del/mklink worked cleanly.
            if dos_cmd_rc == 0:
                sql_update_rc = update_dolphin_database(dolphindb, sql)

            # if both the delete/link AND the SQL update commited ok, then we mark this transaction as complete.
            if dos_cmd_rc == 0 & sql_update_rc == 0:
                update_transactions(sqlitedb, docid)

            time.sleep(0.1)  # sleep for a fraction of a second. Tune depending on what Production can handle

produce_run_report()
sqlitedb.close()
logging.info("Transaction Database closed")
dolphindb.close()
logging.info("Dolphin Database closed")
