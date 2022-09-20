#
# ADAPTED FROM: https://github.com/s2t2/tweet-analysis-2020/blob/3ab4abf156c48b5cabbf807c0fc30d63f81444f8/app/bq_service.py#L138-L227
#

import os
from functools import cached_property
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DATASET_ADDRESS = os.getenv("DATASET_ADDRESS", default="tweet-collector-py.jan6_committee_development") # "MY_PROJECT.MY_DATASET"


class BigQueryStorage(BigQueryService):

    def __init__(self, dataset_address=DATASET_ADDRESS, client=None):
        super().__init__(client=client)
        self.dataset_address = dataset_address.replace(";","") # be safe about sql injection, since we'll be using this address in queries

    def fetch_topics(self):
        """Returns a list of topic strings"""
        sql = f"""
            SELECT topic, created_at
            FROM `{self.dataset_address}.topics`
            ORDER BY created_at;
        """
        return self.execute_query(sql)

    def fetch_topic_names(self):
        return [row.topic for row in self.fetch_topics()]

    def append_topics(self, topics):
        """
        Inserts topics unless they already exist.
        Param: topics (list of dict)
        """
        rows = self.fetch_topics()
        existing_topics = [row.topic for row in rows]
        new_topics = [topic for topic in topics if topic not in existing_topics]
        if new_topics:
            rows_to_insert = [[new_topic, self.generate_timestamp()] for new_topic in new_topics]
            errors = self.client.insert_rows(self.topics_table, rows_to_insert)
            return errors
        else:
            print("NO NEW TOPICS...")
            return []

    def append_tweets(self, tweets):
        """Param: tweets (list of dict)"""
        rows_to_insert = [list(d.values()) for d in tweets]
        errors = self.client.insert_rows(self.tweets_table, rows_to_insert)
        return errors
