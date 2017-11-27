# Import statements
import psycopg2
import psycopg2.extras
from csv import DictReader
from config import *
import sys
import os

# the code below borrows heavily from the Chinook code from Section, pgexample_multiplerows_byname.py from class, and too many stackoverflow comments to count

print('\n     *** NEW EXECUTE ***\n')

# Write code / functions to set up database connection and cursor here.

def connect_to_db():
    try:
        conn = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        print("Success connecting to database\n")
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return conn, cur

conn, cur = connect_to_db()


# Write code / functions to create tables with the columns you want and all database setup here.

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) # insert by column name, instead of position

cur.execute("DROP TABLE IF EXISTS Sites")
cur.execute("DROP TABLE IF EXISTS States")

cur.execute("CREATE TABLE IF NOT EXISTS Sites(ID SERIAL PRIMARY KEY, Name VARCHAR(128), Type VARCHAR(128), State_ID INTEGER, Location VARCHAR(255), Description TEXT)")

cur.execute("CREATE TABLE IF NOT EXISTS States(ID Serial PRIMARY KEY, Name VARCHAR(40))")


# Write code / functions to deal with CSV files and insert data into the database here.

def csv_open(fileList):
    states_ids = states_data_insert(fileList)
    for fileName in fileList:
        dataList = []
        counter = 0
        lineReader = DictReader(open(fileName, 'r'))
        for row in lineReader:
            dataList.append(row)
            counter = counter + 1
        sites_data_insert(dataList, states_ids, fileName, counter)
    return


states_ids = {}

def states_data_insert(fileList):
    c = 1
    for state in fileList:
        filename, file_extension = os.path.splitext(state)
        state_name = str(filename)
        sql = "INSERT INTO States (Name) VALUES (%s)"
        cur.execute(sql, (state_name,))
        states_ids[str(state)] = c
        c = c+1
    return states_ids


def sites_data_insert(data, states_ids, fileName, counter):
    x = states_ids[str(fileName)]
    list_ids = []
    c = 0
    while c < counter:
        list_ids.append([x])
        c = c + 1
    cur.executemany("""INSERT INTO Sites(Name, Type, Location, Description) VALUES (%(NAME)s, %(TYPE)s, %(LOCATION)s, %(DESCRIPTION)s)""",data)

    cur.executemany("UPDATE Sites SET State_ID = (%s) WHERE State_ID IS NULL", list_ids)
    return


# Make sure to commit your database changes with .commit() on the database connection.

conn.commit() # Necessary to save changes in database


# Write code to be invoked here (e.g. invoking any functions you wrote above)

csv_files = ['california.csv', 'michigan.csv', 'arkansas.csv']
csv_open(csv_files)


# Write code to make queries and save data in variables here.

cur.execute('SELECT Location FROM Sites')
all_locations = cur.fetchall()
#print(all_locations)

cur.execute("""SELECT Name FROM Sites WHERE Description LIKE '%beautiful%'""")
beautiful_sites = cur.fetchall()
#print(beautiful_sites)

cur.execute("""SELECT COUNT(Type) FROM Sites WHERE Type LIKE '%National Lakeshore%'""")
natl_lakeshores = cur.fetchall()
#print(natl_lakeshores)

cur.execute("""SELECT Sites.Name FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) WHERE States.Name like 'michigan'""")
michigan_names = cur.fetchall()
#print(michigan_names)

cur.execute("""SELECT Count(Sites.Name) FROM Sites INNER JOIN States ON (Sites.State_ID = States.ID) WHERE States.Name like 'arkansas'""")
total_number_arkansas = cur.fetchall()
#print(total_number_arkansas)


print('\n   *** Done ***\n')
