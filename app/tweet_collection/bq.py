
import os
from functools import cached_property

from app.bq_service import BigQueryService

DATASET_ADDRESS = os.getenv("DATASET_ADDRESS", default="tweet-collector-py.jan6_committee_development") # "MY_PROJECT.MY_DATASET"


class BigQueryDatabase(BigQueryService):

    def __init__(self, dataset_address=DATASET_ADDRESS, client=None):
        super().__init__(client=client)
        self.dataset_address = dataset_address.replace(";","") # be safe about sql injection, since we'll be using this address in queries

    @cached_property
    def tweets_table(self):
        return self.client.get_table(f"{self.dataset_address}.tweets")

    def save_tweets(self, records):
        self.insert_records_in_batches(self.tweets_table, records)

    def migrate_tweets_table(self):
        """WARNING! USE WITH EXTREME CAUTION!"""
        sql = f"""
            DROP TABLE IF EXISTS `{self.dataset_address}.tweets`;
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.tweets` (
                status_id INT64,
                status_text STRING,
                created_at TIMESTAMP,

                user_id INT64,
                user_screen_name STRING,
                user_name STRING,
                user_created_at TIMESTAMP,
                user_verified BOOLEAN,

                retweet_status_id INT64,
                retweet_user_id INT64,
                reply_status_id INT64,
                reply_user_id INT64,
                quote_status_id INT64,
                quote_user_id INT64,
            );
        """
        self.execute_query(sql)
