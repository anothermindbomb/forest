# coding=utf-8
# Project Forest - cleanup driver.
# Steve Flynn - Data Migration.
#
# 11th February 2018

import sqlite3
import subprocess
import datetime
import time
import logging
import pyodbc

# Dolphin ProdSupp: These should all be False for Production runs.
DolphinTestModeEnabled = True  # If we set this to true, then no commits will be issued to Dolphin
TransactionsTestModeEnabled = True  # If we set this to true, then no commits will be issued to Sqlite
RelinkTestModeEnabled = True  # If we set this to true, then no file relinking will take place.

# Dolphin ProdSupp: Adjust this value to control the number of minutes this framework should execute for
# It's in Minutes and floating point is fine.
MaxExecutionMinutes = 30.0  # number of minutes we execute for upon each invocation.

# Dolphin ProdSupp: These need to be configured for Production/UAT dolphin.
SQLDriverName = "{SQL Server Native Client 10.0}" # ODBC driver available on Dolphin.
# SQLDriverName = "{ODBC Driver 13 for SQL Server}" # Used on Daystate for initial creation.
# DolphinServerName = "DAYSTATE\SQLEXPRESS"  # Servername and Instance used for initial creation
DolphinServerName = "10.174.246.11"  # Servername for VM1
DolphinDatabaseName = "forest_fl"  # Database name for VM1.

''' Dolphin ProdSupp: You shouldn't need to adjust anything below this point.
'''

TransactionDatabaseName = "dolphincommands.sqlite3"
LoggingFilename = 'dolphin-cleanup' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"
ReportFile = datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + ".log"
DolphinDBConnectionString = "Driver=" + SQLDriverName + \
                            ";Server=" + DolphinServerName + \
                            ";Database=" + DolphinDatabaseName + \
                            ";Trusted_Connection=yes;autocommit=No"


def open_sqlite3_database(connection: str) -> sqlite3.Connection:
    try:
        db = sqlite3.connect(connection)
    except Exception as e:
        logging.fatal("Unable to connect to transaction database. Error {0}".format(e))
        exit()
    return db


def open_dolphin_database(connection: str) -> pyodbc.Connection:
    try:
        db = pyodbc.connect(connection)
    except Exception as e:
        logging.fatal("Unable to connection to Dolphin database. Error {0}".format(e))
        exit()
    return db


def execute_dos_cmd(cmd: str) -> int:
    if RelinkTestModeEnabled:
        logging.debug("File Test mode is active - we would try to execute {0}".format(cmd))
        return 0
    else:
        rc = subprocess.call(cmd, shell=True)
    return rc


def update_dolphin_database(dbname: pyodbc.Connection, updatesql: str) -> int:
    dolphincursor = dbname.cursor()
    try:
        if DolphinTestModeEnabled:
            logging.debug("Dolphin Test mode is enabled. We would update Dolphin with {0}".format(updatesql))
            dbname.rollback()
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
            logging.debug("SQLite Testing enabled - we will commit nothing.")
            # transactiondb.rollback()
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
    # cursor.execute('SELECT count(*) FROM transactions WHERE transactions.error_message IS NOT NULL;')
    # total_errors = cursor.fetchone()[0]
    logging.info("{0} records left to process in future runs".format(records_left))
    # logging.info("{1} errors encountered in total".format(total_errors))
    return


# this is only used in development so ensure SQL server connections were working
# def test_dolphin_retrieval(dolphindb):
#     cursor = dolphindb.cursor()
#     cursor.execute("SELECT * FROM dolphintable")
#     output = cursor.fetchall()
#     for line in output:
#         print(line)


if __name__ == '__main__':
    logging.basicConfig(filename=LoggingFilename, level=logging.DEBUG, format='%(asctime)s %(message)s')
    starttime = time.time()
    endtime = time.time() + (MaxExecutionMinutes * 60)

    logging.debug("Execution time is set for {0} minutes.".format(MaxExecutionMinutes))

    if DolphinTestModeEnabled:
        print("Testing is enabled - we will not commit anything to Dolphin.")
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

        returned_rows = cursor.fetchmany(500)

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

            # if both the delete/link AND the SQL update commited ok, then we mark this transaction as complete.
            if dos_cmd_rc == 0 & sql_update_rc == 0:
                update_transactions(sqlitedb, docid, sql)

                # time.sleep(0.1)  # sleep for a fraction of a second. Tune depending on what Production can handle

produce_run_report()
sqlitedb.close()
logging.info("Transaction Database closed")
dolphindb.close()
logging.info("Dolphin Database closed")
