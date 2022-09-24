
from pprint import pprint

from app import seek_confirmation
from app.tweet_streaming.bq import BigQueryDatabase


if __name__ == "__main__":

    bq = BigQueryDatabase()

    print("THIS SCRIPT WILL DESTRUCTIVELY MIGRATE STREAMING TABLES:")
    seek_confirmation()

    #bq.migrate_entities_table(destructive=True)
    bq.migrate_rules_table(destructive=True)
    #bq.migrate_media_table(destructive=True)
    #bq.migrate_tweets_table(destructive=True)
    #bq.migrate_status_hashtags_table(destructive=True)
    #bq.migrate_status_mentions_table(destructive=True)
    #bq.migrate_status_annotations_table(destructive=True)
    #bq.migrate_status_entities_table(destructive=True)
    #bq.migrate_status_urls_table(destructive=True)
    #bq.migrate_status_media_table(destructive=True)
    #bq.migrate_users_table(destructive=True)
    #bq.migrate_user_hashtags_table(destructive=True)
    #bq.migrate_user_mentions_table(destructive=True)
