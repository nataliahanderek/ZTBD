import pandas as pnd
from sqlalchemy import create_engine, Table, select, MetaData, text, insert, func
from sqlalchemy.orm import Session

import sys
from pathlib import Path

sys.path.append(str(Path.cwd()))

connection_string = 'mysql://root:password@localhost/books'
engine = create_engine(connection_string, echo = True)

meta = MetaData()

columns = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

data_counter = 0
insert_counter = 0

#start session
with Session(bind=engine) as session:

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

    #create reflections of the tables in the database

    info_table = Table('books_info', meta, autoload_with = engine)
    dump_table = Table('dump_table', meta, autoload_with = engine)

    title_table = Table('titles', meta, autoload_with = engine)
    author_table = Table('authors', meta, autoload_with = engine)
    publisher_table = Table('publishers', meta, autoload_with = engine)
    year_table = Table('publicationyears', meta, autoload_with = engine)
    type_table = Table('itemtypes', meta, autoload_with = engine)
    collection_table = Table('itemcollections', meta, autoload_with = engine)

    #load data file, chunk by chunk

    data_iter = pnd.read_csv('./data/library-collection-inventory.csv', usecols=columns, iterator=True, chunksize=10000)

    session.execute(dump_table.delete())
    session.commit()

    for data_chunk in data_iter:

        #drop NA rows and rename the columns for convenience

        data = data_chunk.dropna()

        data_counter += data.shape[0]

        data["Title"] = data["Title"].apply(lambda row: row.rsplit(';')[0]).to_numpy()

        data.rename(columns = {
            "Title": "title_val", 
            "Author": "author_val", 
            "PublicationYear": "publicationyear_val", 
            "Publisher": "publisher_val", 
            "ItemType": "itemtype_val", 
            "ItemCollection": "itemcollection_val"
        }, inplace = True)

        #write the full rows to the dunp table

        session.execute(
            insert(dump_table),
            data.to_dict("records")
        )

        session.commit()

        #prepare the statement to convert the data in dump_table into indexes with joins and jada jada

        statement = (
            select(title_table.c.id, author_table.c.id, collection_table.c.id, type_table.c.id, publisher_table.c.id, year_table.c.id)
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

        #insert into out final info_table the indexes. This is now a many-to-many table with full information

        session.execute(insert_statement)
        session.commit()

        #empty the dump_table and repeat for the next data chunk

        session.execute(dump_table.delete())
        session.commit()

print(f"Sesscion concluded {data_counter}")





