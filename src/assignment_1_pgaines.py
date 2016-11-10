import psycopg2
import os
import sys


__author__ = 'Patrick Gaines'
# assignment_one_pgaines.py code for CSE 512 Assignment #1.
# This file contains all code to perform functions specified by the requirements in CSE512_Assignment1.pdf

DATABASE_NAME = 'test_dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'rangeratingspart'
RROBIN_TABLE_PREFIX = 'roundrobinratingspart'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
PARTITION_METADATA_TABLE = 'partition_metadata'
PARTITION_ID = 'partition_id'
PARTITION_LOWER_LIMIT = 'partition_lower_limit'
PARTITION_UPPER_LIMIT = 'partition_upper_limit'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file
MAX_RATING = 5

def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='ddsassignment2'):
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

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Name of the ratings table to be created in the database
    :param ratingsfilepath: Path of the ratings file to read in movie ratings
    :param openconnection: The connection to the database
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()

        # Check if an existing table with the same name exists, drop it if it does, and create the table
        cur.execute('DROP TABLE IF EXISTS ' + ratingstablename)
        cur.execute(
            'CREATE TABLE ' + ratingstablename + '(' + USER_ID_COLNAME + ' INT, ' + MOVIE_ID_COLNAME + ' INT, ' + RATING_COLNAME + ' FLOAT)')

        # read in the file, split the line by the delimiter, and insert each line into the table
        path = os.path.join(os.path.dirname(__file__), ratingsfilepath)
        f = open(path)
        for line in f:
            data = line.split("::")
            cur.execute('INSERT INTO ' + ratingstablename + ' VALUES (' + data[0] + ',' + data[1] + ',' + data[2] + ')')
        f.close()

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


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


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Name of the ratings table in the database
    :param numberofpartitions: How many partitions represent the table
    :param openconnection: Database connection
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()
        cur.execute('SELECT * FROM ' + ratingstablename)
        rowcount = cur.rowcount

        # Creating the round robin partition tables
        for i in range(0, (numberofpartitions)):
            table_num = i

            # Check if an existing table with the same name exists, drop it if it does, and create the table
            cur.execute('DROP TABLE IF EXISTS ' + RROBIN_TABLE_PREFIX + '{0}'.format(table_num))
            cur.execute(
                'CREATE TABLE ' + RROBIN_TABLE_PREFIX + '{0}'.format(
                    table_num) + '(' + USER_ID_COLNAME + ' INT, ' + MOVIE_ID_COLNAME + ' INT, ' + RATING_COLNAME + ' FLOAT)')

        # Inserting values into the round robin partition tables to initially create them
        for i in range(0, numberofpartitions):
            table_num = i

            partition_size = rowcount / numberofpartitions
            offset = i * partition_size
            part_str = str(partition_size)
            offset_str = str(offset)

            # fetch a chunk of rows equal to the number of partitions to be created
            cur.execute('SELECT * FROM ' + ratingstablename + ' LIMIT ' + part_str + ' OFFSET ' + offset_str)
            rows = cur.fetchall()

            # Insert one new row into each partition
            for line in rows:
                userid = str(line[0])
                movieid = str(line[1])
                rating = str(line[2])
                cur.execute('INSERT INTO ' + RROBIN_TABLE_PREFIX + '{0}'.format(
                    table_num) + ' VALUES (' + userid + ',' + movieid + ',' + rating + ')')

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param ratingstablename: Name of the ratings table in the database
    :param userid: id of the user who rated the movie
    :param movieid: id of the movie that was rated
    :param rating: rating of the movie
    :param openconnection: Database connection
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()

        # Check the partition count
        cur.execute(
            "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
                RROBIN_TABLE_PREFIX))
        numberofpartitions = int(cur.fetchone()[0])

        # Check the row count among all round robin partition tables
        rowcount = totalrowsinallpartitions(cur, numberofpartitions, RROBIN_TABLE_PREFIX, 0)

        # Derive the partition number in which the new record will be inserted
        partition_id = rowcount % numberofpartitions

        # Insert the new record into the partition
        userid_str = str(userid)
        movieid_str = str(itemid)
        rating_str = str(rating)
        cur.execute('INSERT INTO ' + RROBIN_TABLE_PREFIX + '{0}'.format(
            partition_id) + ' VALUES (' + userid_str + ',' + movieid_str + ',' + rating_str + ')')

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


def deleteTables(openconnection):
    """
    Tests the range partition function for Completness, Disjointness and Reconstruction
    :param openconnection: Database connection
    :return:Raises exception if any test fails
    """
    try:
        cur = openconnection.cursor()

        # Get the number of partitions to be deleted
        cur.execute("SELECT COUNT(*) FROM " + PARTITION_METADATA_TABLE)
        numberofpartitions = cur.fetchone()[0]

        # Check if an existing table with the same name exists, drop it if it does, and create the table
        for partition_id in range(0, numberofpartitions):
            # Check if an existing table with the same name exists, and drop it
            cur.execute('DROP TABLE IF EXISTS ' + RANGE_TABLE_PREFIX + '{0}'.format(partition_id))
            cur.execute('DROP TABLE IF EXISTS ' + RROBIN_TABLE_PREFIX + '{0}'.format(partition_id))
        # Check if the partition metadata table exists, and drop it
        cur.execute('DROP TABLE IF EXISTS ' + PARTITION_METADATA_TABLE)

        # Clean up
        cur.close()
    except Exception as e:
        print('Error: {0}'.format(e))
        return False


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
