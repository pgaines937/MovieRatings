#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'test_dds_assgn1'


def getOpenConnection(user, password, dbname):
    pass


def createDB(dbname):
    pass


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    pass


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    pass

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass

def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getOpenConnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
