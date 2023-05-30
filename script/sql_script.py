import mysql.connector as mysql
import pandas as pnd
import sys
import numpy as np

import pathlib

sys.path.append(str(pathlib.Path.cwd()))
print(pathlib.Path.cwd())

conn = None

from mysql.connector import Error

try:
    conn = mysql.connect(host='localhost', user='root', database='books',
                         password='password')
    if conn.is_connected():
        print("Database connected to")
except Error as e:
    print("Error while connecting to MySQL", e)

# initial unit tables creation

# list of columns that we are going to turn into unit tables (like author, year, ganre...)
columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]
conn.autocommit = False
cursor = conn.cursor(buffered=True)


def init_db():
    # iterate over the needed columns
    for column in columns_to_tables:

        # get the cursor and create the table with this column's name as a title with an s at the end (like author => authors)
        # it only holds id and the value of the column
        result = cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {column}s(
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            {column} MEDIUMTEXT
        )
        """)
        conn.commit()

        print(f"Table for column {column} created")

        # read the single column from the file
        data = pnd.read_csv('C:/Users/hande/OneDrive/Pulpit/ZTBD/data/library-collection-inventory.csv', usecols=[column])
        # get the qunique values for the insertion
        data = data[column].unique()
        data = data[~pnd.isnull(data)]

        print(f"Preparing to write {len(data)} rows {data[0].rsplit(';')[0]}")
        print(f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)")

        # for every unique entry - insert in into the correcponring table
        for entry in data:
            query = f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)"
            query_params = [entry.rsplit(';')[0]]

            cursor.execute(query, query_params)
            conn.commit()

        # repeat intil the tables are created and filled out

        print(f"Written {cursor.rowcount} entries to the table {column}")

    print("Done!!!")


if __name__ == '__main__':
    init_db()