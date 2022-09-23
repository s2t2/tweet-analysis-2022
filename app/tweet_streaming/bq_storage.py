#
# ADAPTED FROM: https://github.com/s2t2/tweet-analysis-2020/blob/3ab4abf156c48b5cabbf807c0fc30d63f81444f8/app/bq_service.py#L138-L227
#


from app.bq_service import BigQueryService, DATASET_ADDRESS


class BigQueryStorage(BigQueryService):
    """All streaming data table names should end in "_stream"."""

    def __init__(self, dataset_address=DATASET_ADDRESS, client=None):
        super().__init__(client=client)
        self.dataset_address = dataset_address.replace(";","") # be safe about sql injection, since we'll be using this address in queries

    #
    # MIGRATIONS
    #

    #def migrate_domains_table(self, destructive=False):
    #    sql = ""
    #    if destructive:
    #        sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.domains_stream`; "
    #    sql += f"""
    #        CREATE TABLE IF NOT EXISTS `{self.dataset_address}.domains_stream` (
    #            domain_id INT64,
    #            domain_name STRING,
    #        );
    #    """
    #    self.execute_query(sql)

    #def migrate_entities_table(self, destructive=False):
    #    sql = ""
    #    if destructive:
    #        sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.entities_stream`; "
    #    sql += f"""
    #        CREATE TABLE IF NOT EXISTS `{self.dataset_address}.entities_stream` (
    #            entity_id INT64,
    #            entity_name STRING,
    #            domain_ids ARRAY<INT64>,
    #        );
    #    """
    #    self.execute_query(sql)

    def migrate_tweets_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.tweets_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.tweets_stream` (
                status_id INT64,
                status_text STRING,
                created_at TIMESTAMP,

                user_id INT64,
                retweet_status_id INT64,
                retweet_user_id INT64,
                reply_status_id INT64,
                reply_user_id INT64,
                quote_status_id INT64,
                quote_user_id INT64,

                accessed_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    def migrate_media_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.media_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.media_stream` (
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

    def migrate_status_annotations_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_annotations_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_annotations_stream` (
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
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_entities_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_entities_stream` (
                status_id INT64,
                domain_id INT64,
                entity_id INT64,
            );
        """
        self.execute_query(sql)

    def migrate_status_media_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_media_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_media_stream` (
                status_id INT64,
                media_key STRING
            );
        """
        self.execute_query(sql)

    def migrate_status_mentions_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_mentions_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_mentions_stream` (
                status_id INT64,
                user_id INT64,
                user_screen_name STRING,
            );
        """
        self.execute_query(sql)

    def migrate_status_tags_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_tags_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_tags_stream` (
                status_id INT64,
                tag STRING,
            );
        """
        self.execute_query(sql)

    def migrate_status_urls_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.status_urls_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.status_urls_stream` (
                status_id INT64,
                url STRING,
            );
        """
        self.execute_query(sql)


    def migrate_users_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.users_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.users_stream` (
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
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.user_hashtags_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.user_hashtags_stream` (
                user_id INT64,
                tag STRING,
                accessed_at TIMESTAMP,
            );
        """
        self.execute_query(sql)

    def migrate_user_mentions_table(self, destructive=False):
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS `{self.dataset_address}.user_mentions_stream`; "
        sql += f"""
            CREATE TABLE IF NOT EXISTS `{self.dataset_address}.user_mentions_stream` (
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
            FROM `{self.dataset_address}.rules`
            ORDER BY created_at;
        """
        return self.execute_query(sql)

    def fetch_rule_names(self):
        return [row.rule for row in self.fetch_rules()]

    #
    # SAVING RECORDS
    #

    def append_rules(self, rules):
        """
        Inserts rules unless they already exist.
        Param: rules (list of dict)
        """
        rows = self.fetch_rules()
        existing_rules = [row.rule for row in rows]
        new_rules = [rule for rule in rules if rule not in existing_rules]
        if new_rules:
            rows_to_insert = [[new_rule, self.generate_timestamp()] for new_rule in new_rules]
            errors = self.client.insert_rows(self.rules_table, rows_to_insert)
            return errors
        else:
            print("NO NEW RULES...")
            return []

    def append_tweets(self, tweets):
        """Param: tweets (list of dict)"""
        rows_to_insert = [list(d.values()) for d in tweets]
        errors = self.client.insert_rows(self.tweets_table, rows_to_insert)
        return errors
