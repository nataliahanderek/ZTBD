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

#initial unit tables creation

#list of columns that we are going to turn into unit tables (like author, year, ganre...)
columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]
conn.autocommit = False
cursor = conn.cursor(buffered=True)

def init_db():
    #iterate over the needed columns
    for column in columns_to_tables:

        #get the cursor and create the table with this column's name as a title with an s at the end (like author => authors)
        #it only holds id and the value of the column
        result = cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {column}s(
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            {column} MEDIUMTEXT
        )
        """)
        conn.commit()

        print(f"Table for column {column} created")

        #read the single column from the file
        data = pnd.read_csv('./data/library-collection-inventory.csv', usecols=[column])
        #get the qunique values for the insertion
        data = data[column].unique()
        data = data[~pnd.isnull(data)]

        print(f"Preparing to write {len(data)} rows {data[0].rsplit(';')[0]}")
        print(f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)")

        #for every unique entry - insert in into the correcponring table
        for entry in data:
            query = f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)"
            query_params = [entry.rsplit(';')[0]]

            cursor.execute(query, query_params)
            conn.commit()
        
        #repeat intil the tables are created and filled out

        print(f"Written {cursor.rowcount} entries to the table {column}")

    print("Done!!!")


#DO NOT RUN THE BELOW CODE!!!!


# def convert_to_relational():
#     data_iter = pnd.read_csv('./data/library-collection-inventory.csv', usecols=columns_to_tables, iterator=True, chunksize=10000, skiprows=range(1, 300000))

#     #create relational tables
#     for title_relation in columns_to_tables:
#         #avoid title relation with itself
#         if title_relation == "Title":
#             continue

#         #create if needed the relation table for title and other column
#         query = f"""CREATE TABLE IF NOT EXISTS title_{title_relation.lower()} (
#             id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, 
#             Title_id INT NOT NULL,
#             {title_relation}_id INT NOT NULL
#         )""" 
#         cursor.execute(query)
#     conn.commit()

#     #iterate over chunks of data
#     for data_chunk in data_iter:
#         #drop rows with any NA values
#         data = data_chunk.dropna()

#         #get all title indexes for this chunk
#         titles = data["Title"].apply(lambda row: row.rsplit(';')[0]).to_numpy()
#         query = "SELECT id FROM books.titles WHERE Title = %s"
#         title_id = []
#         for title in titles:
#             cursor.execute(query, [title])
#             title_id.append(cursor.fetchone()[0])

#         # print("Fetched id's")
#         # print(len(title_id))

#         #iterate over relations
#         for relation in ["Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]:
#             #get relation's indexes for the chunk
#             relation_data = data[relation].apply(lambda row: row.rsplit(';')[0]).to_numpy()
#             query = f"SELECT id FROM books.{relation.lower()}s WHERE {relation} = %s"
#             relation_id = []
#             for rel in relation_data:
#                 cursor.execute(query, [rel])
#                 relation_id.append(cursor.fetchone()[0])

#             # print(len(relation_id))

#             #insert many into corresponding table
#             query = f"INSERT INTO books.title_{relation.lower()} (Title_id, {relation}_id) VALUES (%s, %s)"
#             cursor.executemany(query, list(zip(title_id, relation_id)))

#         conn.commit()
#         print("Chunk commited")

#         # #iterate over individual rows (records)
#         # for index, row in data.iterrows():
#         #     #create a holder for all the needed id's
#         #     id_holder = {
#         #         f'{key.lower()}_id': None for key in columns_to_tables
#         #     }
#         #     #for every column in the record retrieve it's value's id from the corresponding table in db
#         #     for column in columns_to_tables:
#         #         query = f"SELECT id FROM books.{column.lower()}s WHERE {column} = (%s)"
#         #         cursor.execute(query, [row[column].rsplit(';')[0]])
#         #         id_holder[f'{column.lower()}_id'] = cursor.fetchone()[0]
#         #     #now we have all ids for every column value in the record. We can start puting them into reletional tables

#         #     for title_relation in ["Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]:
#         #         #insert there the title id and the other column's id
#         #         query = f"INSERT INTO books.title_{title_relation.lower()} (Title_id, {title_relation}_id) VALUES (%s, %s)"
#         #         cursor.execute(query, [id_holder['title_id'], id_holder[f'{title_relation.lower()}_id']])
#         #         conn.commit()

#         #         #continue with the other relations

if __name__ == '__main__':
    init_db()
    # convert_to_relational()
    # print("DONE!")





