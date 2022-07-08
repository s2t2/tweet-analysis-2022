
import os
from functools import cached_property

from app.bq_service import BigQueryService

DATASET_ADDRESS = os.getenv("DATASET_ADDRESS") # "MY_PROJECT.MY_DATASET"


class BigQueryDatabase(BigQueryService):

    def __init__(self, dataset_address=DATASET_ADDRESS):
        self.dataset_address = dataset_address

    @cached_property
    def tweets_table(self):
        return self.client.get_table(f"{self.dataset_address}.tweets")

    def save_tweets(self, records):
        self.insert_records_in_batches(self.tweets_table, records)
