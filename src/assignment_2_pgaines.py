import sys

__author__ = 'Patrick Gaines'
# assignment_2_pgaines.py code for CSE 512 Assignment #2.
# This file contains all code to perform functions specified by the requirements in CSE512_Assignment2.pdf

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute(
            "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{}%';".format(
                ratingsTableName))
        numberofpartitions = int(cursor.fetchone()[0])
        maxpartition = numberofpartitions - 1;

        file = open("RangeQueryOut.txt", "w")

        for partition in range(0, maxpartition):
            cursor.execute("SELECT * FROM %s%s WHERE Rating >= %f AND Rating <= %f" % (
            ratingsTableName, partition, ratingMinValue, ratingMaxValue))

            rows = cursor.fetchall()

            # Write all the rows from the ratings partitions into the output file
            for line in rows:
                file.write("{}{},{},{},{}\n".format(ratingsTableName, partition, line[0], line[1], line[2]))

        file.close()

    except Exception as e:
        print('Error: {0}'.format(e))
        sys.exit(1)
    finally:
        pass


def PointQuery(ratingsTableName, ratingValue, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute(
            "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{}%';".format(
                ratingsTableName))
        numberofpartitions = int(cursor.fetchone()[0])
        maxpartition = numberofpartitions - 1;
        file = open("PointQueryOut.txt", "w")

        for partition in range(0, maxpartition):
            cursor.execute("SELECT * FROM %s%s WHERE Rating = %f" % (ratingsTableName, partition, ratingValue))
            rows = cursor.fetchall()

            # Write all the rows from the ratings partitions into the output file
            for line in rows:
                file.write("{}{},{},{},{}\n".format(ratingsTableName, partition, line[0], line[1], line[2]))

        file.close()

    except Exception as e:
        print('Error: {0}'.format(e))
        sys.exit(1)
    finally:
        pass
