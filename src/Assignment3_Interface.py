#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import sys
import math
from multiprocessing import Process, Manager

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'MovieRating'
SECOND_TABLE_NAME = 'MovieBoxOfficeCollection'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'Collection'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieID'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieID'
PROCESSES = 5
##########################################################################################################
RANGE_TABLE_PREFIX = 'rangeratingspart'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
PARTITION_METADATA_TABLE = 'partition_metadata'
PARTITION_ID = 'partition_id'
PARTITION_LOWER_LIMIT = 'partition_lower_limit'
PARTITION_UPPER_LIMIT = 'partition_upper_limit'
MAX_RATING = 5


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    rows = fetchTable(InputTable, SortingColumnName, openconnection)


    #Implement ParallelSort Here.
    pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    pass # Remove this once you are done with implementation

def fetchTable(InputTable, SortingColumnName, openconnection):
    cur = openconnection.cursor()
    # get all the rows from the ratings table
    cur.execute('SELECT * FROM ' + InputTable + ' ORDER BY ' + SortingColumnName + ' DESC')
    return cur.fetchall()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Name of the ratings table to be created in the database
    :param numberofpartitions: Number of partitions to split ratings table into
    :param openconnection: The connection to the database
    :return:Raises exception if any test fails
    """
    try:
        createpartitions(ratingstablename, numberofpartitions, openconnection)

        cur = openconnection.cursor()

        # create the range table partitions
        for partition_id in range(0, numberofpartitions):
            # Check if an existing table with the same name exists, drop it if it does, and create the table
            cur.execute('DROP TABLE IF EXISTS ' + RANGE_TABLE_PREFIX + '{0}'.format(partition_id))
            cur.execute(
                'CREATE TABLE ' + RANGE_TABLE_PREFIX + '{0}'.format(
                    partition_id) + '(' + USER_ID_COLNAME + ' INT, ' + MOVIE_ID_COLNAME + ' INT, ' + RATING_COLNAME + ' FLOAT)')

        # get all the rows from the ratings table
        cur.execute('SELECT * FROM ' + ratingstablename + ' ORDER BY ' + RATING_COLNAME + ' DESC')
        rows = cur.fetchall()

        # Insert all the rows from the ratings table into the range partitions
        for line in rows:
            rangeInsert(ratingstablename, line[0], line[1], line[2], openconnection)

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: name of the ratings table in the database
    :param userid: id of the user who rated the movie
    :param movieid: id of the movie that was rated
    :param rating: rating of the movie
    :param openconnection: database connection
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()

        # Translates the rating value to the correct partition number
        partition_id = rating_to_partition_id(cur, rating)

        # Insert the values in the range partition table
        userid_str = str(userid)
        movieid_str = str(itemid)
        rating_str = str(rating)
        cur.execute('INSERT INTO ' + RANGE_TABLE_PREFIX + '{0}'.format(
            partition_id) + ' VALUES (' + userid_str + ',' + movieid_str + ',' + rating_str + ')')

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False

# ##############

# Utilities

def createpartitions(ratingstablename, numberofpartitions, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Name of the ratings table in the database
    :param numberofpartitions: How many partitions represent the table
    :param openconnection: Database connection
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()

        # Check if an existing table with the same name exists, drop it if it does, and create the table
        cur.execute('DROP TABLE IF EXISTS ' + PARTITION_METADATA_TABLE)
        createtable = 'CREATE TABLE ' + PARTITION_METADATA_TABLE + '(' + PARTITION_ID + ' INT, ' + PARTITION_LOWER_LIMIT + ' FLOAT, ' + PARTITION_UPPER_LIMIT + ' FLOAT)'
        cur.execute(createtable)

        # Insert the correct values in the partition metadata table
        for i in range(0, numberofpartitions):
            cur.execute('SELECT * FROM ' + ratingstablename)
            partition_interval = MAX_RATING / float(numberofpartitions)
            partition_id = str(i)
            partition_lower_limit = str(i * partition_interval)
            partition_upper_limit = str((i + 1) * partition_interval)
            cur.execute(
                'INSERT INTO ' + PARTITION_METADATA_TABLE + ' VALUES (' + partition_id + ',' + partition_lower_limit + ',' + partition_upper_limit + ')')

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False

def rating_to_partition_id(cur, rating):
    """
    Converts the rating value to the partition id
    :param cur: cursor of the openconnection to the database
    :param rating: rating of the movie
    :return:Raises exception if any test fails
    """
    try:
        # Get the list of partitions from the partition metadata table
        cur.execute('SELECT * FROM ' + PARTITION_METADATA_TABLE)
        partitions = cur.fetchall()

        # Getting petition_upper_limit of the first entry in the partition table
        # The partition_interval determines the range of values for each partition
        partition_interval = partitions[0][2]
        if rating == MAX_RATING:
            partition_id = int((rating - partition_interval) / partition_interval)
        else:
            partition_id = int(rating / partition_interval)
        return partition_id
    except Exception as e:
        print('Error: {0}'.format(e))
        return False

def totalrowsinallpartitions(cur, numberofpartitions, rangepartitiontableprefix, partitionstartindex):
    """
    Converts the rating value to the partition id
    :param cur: cursor of the openconnection to the database
    :param numberofpartitions: How many partitions represent the table
    :param rangepartitiontableprefix: Self descriptive
    :param partitionstartindex: The number of the first partition (normally 0)
    :return:Raises exception if any test fails
    """
    try:
        selects = []
        for i in range(partitionstartindex, numberofpartitions + partitionstartindex):
            selects.append('SELECT * FROM {0}{1}'.format(rangepartitiontableprefix, i))
        cur.execute('SELECT COUNT(*) FROM ({0}) AS T'.format(' UNION ALL '.join(selects)))
        count = int(cur.fetchone()[0])
        return count
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
