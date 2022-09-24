#
# ADAPTED FROM: https://github.com/s2t2/tweet-analysis-2020/blob/3ab4abf156c48b5cabbf807c0fc30d63f81444f8/app/bq_service.py#L138-L227
#

from functools import cached_property
from pprint import pprint

from app.bq_service import BigQueryService, DATASET_ADDRESS


class BigQueryDatabase(BigQueryService):
    """All streaming data table names should start with 'streaming_'"""

    def __init__(self, dataset_address=DATASET_ADDRESS, client=None):
        super().__init__(client=client)
        self.dataset_address = dataset_address.replace(";","") # be safe about sql injection, since we'll be using this address in queries

        print("BQ STREAMING DATABASE:", self.dataset_address.upper())

    #
    # MIGRATIONS
    #

    def migrate_rules_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_rules`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_rules` (
                rule STRING,
                created_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    def migrate_media_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_media`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_media` (
                media_key STRING,
                type STRING,
                url STRING,
                preview_image_url STRING,

                alt_text STRING,
                duration_ms INT64,
                height INT64,
                width INT64,
            );
        """
        self.execute_query(sql)

    def migrate_tweets_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_tweets`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_tweets` (
                status_id INT64,
                status_text STRING,
                created_at TIMESTAMP,

                user_id INT64,

                retweet_status_id INT64,
                reply_status_id INT64,
                quote_status_id INT64,
                conversation_id INT64,
            );
        """
        self.execute_query(sql)

    def migrate_status_annotations_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_annotations`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_annotations` (
                status_id INT64,
                type STRING,
                text STRING,
                probability FLOAT64,
            );
        """
        self.execute_query(sql)

    def migrate_status_entities_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_entities`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_entities` (
                status_id INT64,
                domain_id INT64,
                entity_id INT64,
            );
        """
        self.execute_query(sql)

    def migrate_status_media_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_media`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_media` (
                status_id INT64,
                media_key STRING
            );
        """
        self.execute_query(sql)

    def migrate_status_mentions_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_mentions`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_mentions` (
                status_id INT64,
                user_id INT64,
                user_screen_name STRING,
            );
        """
        self.execute_query(sql)

    def migrate_status_hashtags_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_tags`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_tags` (
                status_id INT64,
                tag STRING,
            );
        """
        self.execute_query(sql)

    def migrate_status_urls_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_status_urls`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_status_urls` (
                status_id INT64,
                url STRING,
            );
        """
        self.execute_query(sql)

    def migrate_users_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_users`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_users` (
                user_id INT64,
                screen_name STRING,
                name STRING,
                description STRING,
                url STRING,
                profile_image_url STRING,
                verified BOOLEAN,
                created_at TIMESTAMP,
                pinned_tweet_id INT64,
                followers_count INT64,
                following_count INT64,
                tweet_count INT64,
                listed_count INT64,
                accessed_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    def migrate_user_hashtags_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_user_hashtags`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_user_hashtags` (
                user_id INT64,
                tag STRING,
                accessed_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    def migrate_user_mentions_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.streaming_user_mentions`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.streaming_user_mentions` (
                user_id INT64,
                mention_screen_name STRING,
                accessed_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    #
    # FETCHING RECORDS
    #

    def fetch_rules(self):
        """Returns a list of rule / rule strings"""
        sql = f"""
            SELECT rule, created_at
            FROM `{self.dataset_address}.streaming_rules`
            ORDER BY created_at;
        """
        return self.execute_query(sql)

    def fetch_rule_names(self):
        return [row.rule for row in self.fetch_rules()]

    #
    # SAVING RECORDS
    #

    @cached_property
    def rules_table(self):
        return self.client.get_table(f"{self.dataset_address}.streaming_rules")

    def seed_rules(self, records):
        """
        Inserts rules unless they already exist.
        Param: rules (list of dict)
        """
        existing_rules = self.fetch_rule_names()
        new_rules = [row["rule"] for row in records if row["rule"] not in existing_rules]
        if any(new_rules):
            rows_to_insert = [[new_rule, self.generate_timestamp()] for new_rule in new_rules]
            print("NEW RULES:")
            pprint(rows_to_insert)
            errors = self.client.insert_rows(self.rules_table, rows_to_insert)
            print(errors)
            return errors
        else:
            print("NO NEW RULES...")
            return []

    def append_tweets(self, tweets):
        """Param: tweets (list of dict)"""
        rows_to_insert = [list(d.values()) for d in tweets]
        errors = self.client.insert_rows(self.tweets_table, rows_to_insert)
        return errors
