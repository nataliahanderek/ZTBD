import mysql.connector as mysql
from mysql.connector import Error
import pandas as pnd
from sqlalchemy import create_engine, Table, select, MetaData, text, insert, func
from sqlalchemy.orm import Session
from config import *


def init_connection():
    conn = None
    try:
        conn = mysql.connect(host=HOST, user=USERNAME, database=DATABASE, port=PORT_SQL,
                             password=PASSWORD)
        if conn.is_connected():
            print("Database connected to")
    except Error as e:
        print("Error while connecting to MySQL", e)
    return conn


class MySql:
    def __init__(self):
        self.r = init_connection()
        self.columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]
        self.r.autocommit = False
        self.cursor = self.r.cursor(buffered=True)

        self.connection_string = 'mysql://' + USERNAME + ':' + PASSWORD + '@' + HOST + '/' + DATABASE
        self.engine = create_engine(self.connection_string, echo=True)

        self.meta = MetaData()

    def create_single_tables(self):
        # iterate over the needed columns
        for column in self.columns_to_tables:
            # get the cursor and create the table with this column's name as a title with an s at the end (like author => authors)
            # it only holds id and the value of the column
            result = self.cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {column}s(
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    {column} MEDIUMTEXT
                )
                """)

            self.r.commit()

            print(f"Table for column {column} created")

            # read the single column from the file
            data = pnd.read_csv(CSV_URL, usecols=[column])

            # get the qunique values for the insertion
            data = data[column].unique()
            data = data[~pnd.isnull(data)]

            print(f"Preparing to write {len(data)} rows {data[0].rsplit(';')[0]}")
            print(f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)")

            # for every unique entry - insert in into the correcponring table
            for entry in data:
                query = f"INSERT INTO books.{column.lower()}s ({column}) VALUES (%s)"
                query_params = [entry.rsplit(';')[0]]

                self.cursor.execute(query, query_params)
                self.r.commit()

            # repeat intil the tables are created and filled out
            print(f"Written {self.cursor.rowcount} entries to the table {column}")

        print("Done!!!")

    def create_connection_table(self):
        data_counter = 0
        insert_counter = 0

        # start session
        with Session(bind=self.engine) as session:
            # result = conection.execute(text(
            #     """CREATE TABLE IF NOT EXISTS dump_table (
            #         id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            #         title_val VARCHAR(400) NOT NULL,
            #         author_val VARCHAR(400) NOT NULL,
            #         itemcollection_val VARCHAR(400) NOT NULL,
            #         itemtype_val VARCHAR(400) NOT NULL,
            #         publisher_val VARCHAR(400) NOT NULL,
            #         publicationyear_val VARCHAR(400) NOT NULL
            #     )"""))

            # create reflections of the tables in the database
            info_table = Table('books_info', self.meta, autoload_with=self.engine)
            dump_table = Table('dump_table', self.meta, autoload_with=self.engine)

            title_table = Table('titles', self.meta, autoload_with=self.engine)
            author_table = Table('authors', self.meta, autoload_with=self.engine)
            publisher_table = Table('publishers', self.meta, autoload_with=self.engine)
            year_table = Table('publicationyears', self.meta, autoload_with=self.engine)
            type_table = Table('itemtypes', self.meta, autoload_with=self.engine)
            collection_table = Table('itemcollections', self.meta, autoload_with=self.engine)

            # load data file, chunk by chunk

            data_iter = pnd.read_csv(CSV_URL,
                                     usecols=self.columns_to_tables,
                                     iterator=True,
                                     chunksize=10000)

            session.execute(dump_table.delete())

            session.commit()

            for data_chunk in data_iter:
                # drop NA rows and rename the columns for convenience
                data = data_chunk.dropna()

                data_counter += data.shape[0]

                data["Title"] = data["Title"].apply(lambda row: row.rsplit(';')[0]).to_numpy()

                data.rename(columns={
                    "Title": "title_val",
                    "Author": "author_val",
                    "PublicationYear": "publicationyear_val",
                    "Publisher": "publisher_val",
                    "ItemType": "itemtype_val",
                    "ItemCollection": "itemcollection_val"
                }, inplace=True)

                # write the full rows to the dunp table
                session.execute(
                    insert(dump_table),
                    data.to_dict("records")
                )

                session.commit()

                # prepare the statement to convert the data in dump_table into indexes with joins and jada jada
                statement = (
                    select(title_table.c.id, author_table.c.id, collection_table.c.id, type_table.c.id,
                           publisher_table.c.id, year_table.c.id)
                    .select_from(dump_table).join(title_table, dump_table.c.title_val == title_table.c.Title)
                    .join(author_table, dump_table.c.author_val == author_table.c.Author)
                    .join(collection_table, dump_table.c.itemcollection_val == collection_table.c.ItemCollection)
                    .join(type_table, dump_table.c.itemtype_val == type_table.c.ItemType)
                    .join(publisher_table, dump_table.c.publisher_val == publisher_table.c.Publisher)
                    .join(year_table, dump_table.c.publicationyear_val == year_table.c.PublicationYear)
                )

                insert_statement = insert(info_table).from_select(
                    ["title_id", "author_id", "itemcollection_id", "itemtype_id", "publisher_id", "publicationyear_id"],
                    statement
                )

                print(insert_statement)

                # insert into out final info_table the indexes. This is now a many-to-many table with full information
                session.execute(insert_statement)
                session.commit()

                # empty the dump_table and repeat for the next data chunk
                session.execute(dump_table.delete())
                session.commit()

        print(f"Sesscion concluded {data_counter}")

    def insert(self):
        # TODO
        pass

    def update(self):
        # TODO
        pass

    def select(self):
        # TODO
        pass

    def select_all(self):
        # TODO
        pass

    def delete(self):
        # TODO
        pass