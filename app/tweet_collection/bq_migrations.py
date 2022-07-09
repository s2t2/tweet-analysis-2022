

from app import seek_confirmation
from app.tweet_collection.bq import BigQueryDatabase

if __name__ == "__main__":

    db = BigQueryDatabase()
    print("DB:", db.dataset_address.upper())

    print("THIS SCRIPT WILL DESTRUCTIVELY MIGRATE TABLES")
    seek_confirmation()

    db.migrate_media_table()
    db.migrate_tweets_table()
    db.migrate_status_annotations_table()
    db.migrate_status_entities_table()
    db.migrate_status_media_table()
    db.migrate_status_mentions_table()
    db.migrate_status_tags_table()
