


import os
import sqlite3

from pandas import DataFrame

DB_FILEPATH = os.path.join(os.path.dirname(__file__), "tweet_collection_development.db") # a path to wherever your database exists

TABLE_NAMES = [
    # "domains", "entities",
    "media", "tweets",
    "status_annotations", "status_entities", "status_media", "status_mentions", "status_tags",
]

class CollectionDatabase:
    def __init__(self, destructive=True, filepath=DB_FILEPATH):
        self.destructive = bool(destructive)

        self.filepath = filepath
        print("DB FILEPATH:", self.filepath)

        self.connection = sqlite3.connect(self.filepath)
        self.connection.row_factory = sqlite3.Row
        #print("CONNECTION:", self.connection)

        self.cursor = self.connection.cursor()
        #print("CURSOR", self.cursor)

        if self.destructive:
            self.drop_tables()

        #self.migrate_tables()


    def drop_tables(self):
        print("DROPPING TABLES:")
        for table_name in TABLE_NAMES:
            print("...", table_name)
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    #def migrate_tables(self):
    #    self.migrate_tweets()
    #    #self.migrate_tags()
    #    #self.migrate_mentions()
    #    #self.migrate_annotations()
    #    #self.migrate_media()

    #def migrate_tweets(self):
    #    print("-------------------")
    #    table_name = "tweets"
    #    sql = f"""
    #        DROP TABLE IF EXISTS {table_name};
    #        CREATE TABLE IF NOT EXISTS {table_name} (
    #            -- row_id SERIAL PRIMARY KEY,
#
    #            -- do we need a FK to the search term record?
#
    #            status_id INT64 NOT NULL,
    #            status_text VARCHAR(500),
    #            -- status_created_at TIMESTAMP,
#
    #            user_id INT64 NOT NULL,
    #            user_screen_name VARCHAR(250),
    #            user_name VARCHAR(250),
    #            -- user_created_at TIMESTAMP,
    #            user_verified BOOL,
#
    #            retweet_status_id INT64,
    #            retweet_user_id INT64,
    #            reply_status_id INT64,
    #            reply_user_id INT64,
    #            quote_status_id INT64,
    #            quote_user_id INT64,
    #        );
    #    """
    #    print("SQL:", sql)
    #    self.cursor.execute(sql)

    #
    # INSERT DATA
    #

    def insert_data(self, table_name, records):
        if not records or not any(records):
            return None

        df = DataFrame(records)
        #df.index.rename("row_id", inplace=True) # assigns a column label "id" for the index column
        #df.index += 1 # starts ids at 1 instead of 0
        #print(df.head())
        # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
        df.to_sql(table_name, con=self.connection,
            if_exists="append", # append to existing tables (don't throw error)
            #index_label="row_id", # store unique ids for the rows, so we could count them (JK this restarts numbering at 1 for each df)
            index=False
        )


    def save_media(self, media):
        self.insert_data("media", media)

    def save_tweets(self, tweets):
        self.insert_data("tweets", tweets)

    def save_status_annotations(self, annotations):
        self.insert_data("status_annotations", annotations)

    def save_status_entities(self, status_entities):
        self.insert_data("status_entities", status_entities)

    def save_status_media(self, status_media):
        self.insert_data("status_media", status_media)

    def save_status_mentions(self, mentions):
        self.insert_data("status_mentions", mentions)

    def save_status_tags(self, tags):
        self.insert_data("status_tags", tags)


if __name__ == "__main__":

    db = CollectionDatabase()
    cursor = db.cursor

    #sql = ""
    #result = cursor.execute(sql).fetchall()
