# coding=utf-8
# Project Forest - cleanup driver.
# Steve Flynn - Data Migration.
#
# 22nd February 2018

import sqlite3
import subprocess
import datetime
import time
import logging
import pyodbc
import argparse

#  These should all be False for Production runs.
DolphinTestModeEnabled = False  # If we set this to true, then no commits will be issued to Dolphin
RelinkTestModeEnabled = False  # If we set this to true, then no file relinking will take place.
TransactionsTestModeEnabled = False  # If we set this to true, then no commits will be issued to Sqlite

TransactionDatabaseName = "dolphincommands-fulltest.sqlite3"
LoggingFilename = 'dolphin-cleanup' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"
ReportFile = datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"

parser = argparse.ArgumentParser(description='Re-links Extracted Forest Images and cleans metadata')
parser.add_argument('-d', '--dsn', required=True, default="DolphinDB", dest='DolphinDSN',
                    help='Name of the ODBC System DSN which points to the Dolphin Database')
parser.add_argument('-u', '--user', required=False, default="FlynnS", dest='DolphinUser',
                    help='SQL User to log into Dolphin')
parser.add_argument('-p', '--password', required=True, default="BadPassword", dest='DolphinUserPassword',
                    help='Password for the SQL User')
parser.add_argument('-t', '--executiontime', required=False, default=60.0, type=float, dest='MaxExecutionMinutes',
                    help='Number of minutes to run for before automatically stopping')

options = parser.parse_args()

DolphinDBConnectionString = "DSN=" + options.DolphinDSN + ";UID=" + options.DolphinUser + ";PWD=" + options.DolphinUserPassword


def open_sqlite3_database(connection: str) -> sqlite3.Connection:
    try:
        db = sqlite3.connect(connection)
    except Exception as e:
        logging.fatal("Unable to connect to transaction database. Error {0}".format(e))
        exit()
    return db


def open_dolphin_database(connection: str) -> pyodbc.Connection:
    logging.debug("Using connection string: {0}".format(connection))
    try:
        db = pyodbc.connect(connection)
    except Exception as e:
        logging.fatal("Unable to connection to Dolphin database. Error {0}".format(e))
        exit()
    return db


def execute_dos_cmd(cmd: str) -> int:
    if RelinkTestModeEnabled:
        logging.info("File Test mode is active - we would try to execute {0}".format(cmd))
        return 0
    else:
        try:
            rc = subprocess.check_call(cmd, shell=True, stdout=subprocess.DEVNULL)
            logging.debug("File delete and relink returned {0}".format(rc))
        except subprocess.CalledProcessError as e:
            logging.error("File delete and relink failed with {0}".format(e))
            rc = e.returncode
    return rc


def update_dolphin_database(dbname: pyodbc.Connection, updatesql: str) -> int:
    dolphincursor = dbname.cursor()
    try:
        if DolphinTestModeEnabled:
            logging.info("Dolphin Test mode is enabled. We would update Dolphin with {0}".format(updatesql))
        else:
            dolphincursor.execute(updatesql)
            dbname.commit()

    except Exception as e:
        logging.error("Dolphin database update failed with {0}, sql = {1}".format(e, updatesql))
        dbname.rollback()
        return -1

    dolphincursor.close()
    return 0


def update_transactions(transactiondb: sqlite3.Connection, documentid: str, sql: str) -> int:
    # SQL Server only wants to see single quotes around literals.
    # SQL Lite wants to see doubled singled quote around literals when we're searching for it, as the SQL has
    # single quote in it already.
    # Therefore, I need to ensure that any single quotes in the SQL are doubled up in order to not throw exceptions
    # within SQL server...

    sql = sql.replace("'", "''")

    try:
        transactioncursor = transactiondb.cursor()

        if TransactionsTestModeEnabled:
            logging.info("SQLite Testing enabled - we will commit nothing.")
        else:
            transactioncursor.execute(
                "update transactions set is_processed = 1 where docid='{0}' AND update_sql='{1}'".format(documentid,
                                                                                                         sql))
            transactiondb.commit()

    except Exception as e:
        logging.error("Transaction database update failed with {0}, sql = {1}".format(e, sql))
        transactiondb.rollback()
        return -1

    transactioncursor.close()
    return 0


def produce_run_report() -> None:
    print("producing run report")
    cursor.execute('SELECT COUNT(*) FROM transactions WHERE is_processed IS NULL;')
    records_left = cursor.fetchone()[0]
    logging.info("{0} records left to process in future runs".format(records_left))
    return


# this is only used in development to ensure SQL server connections were working
def test_dolphin_retrieval(dolphindb):
    cursor = dolphindb.cursor()
    cursor.execute("SELECT * FROM dolphintable")
    output = cursor.fetchall()
    for line in output:
        print(line)


if __name__ == '__main__':
    logging.basicConfig(filename=LoggingFilename, level=logging.INFO, format='%(asctime)s %(message)s')
    starttime = time.time()
    endtime = time.time() + (options.MaxExecutionMinutes * 60)

    logging.info("Execution time is set for {0} minutes.".format(options.MaxExecutionMinutes))

    if DolphinTestModeEnabled:
        logging.info("Testing is enabled - we will not commit anything to Dolphin.")

    sqlitedb = open_sqlite3_database(TransactionDatabaseName)
    logging.info("Transaction Database opened")

    dolphindb = open_dolphin_database(DolphinDBConnectionString)
    logging.info("Dolphin Database opened")

    # test_dolphin_retrieval(dolphindb)

    cursor = sqlitedb.cursor()
    cursor.execute('SELECT docid, update_sql, del_and_link_cmd FROM transactions WHERE is_processed IS NULL;')

    while True:

        if time.time() > endtime:  # quit when we run out of time.
            break

        returned_rows = cursor.fetchmany(50)

        if len(returned_rows) == 0 or returned_rows is None:  # quit when we run out of transactions.
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

            # if both the delete/link AND the SQL update committed ok, then we mark this transaction as complete.
            if dos_cmd_rc == 0 & sql_update_rc == 0:
                update_transactions(sqlitedb, docid, sql)

                # time.sleep(0.1)  # sleep for a fraction of a second. Tune depending on what Production can handle
produce_run_report()
sqlitedb.close()
logging.info("Transaction Database closed")
dolphindb.close()
logging.info("Dolphin Database closed")
