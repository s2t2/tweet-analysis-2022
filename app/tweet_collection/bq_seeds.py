

from app.tweet_collection.bq import BigQueryDatabase
from app.tweet_collection.twitterdev import fetch_context_entities

if __name__ == "__main__":

    db = BigQueryDatabase()

    print("---------------------")
    print("FETCHING CONTEXT ENTITIES AND DOMAINS...")
    df = fetch_context_entities()
    df.drop(columns=["domains_csv"], inplace=True) # use domain_ids list instead of domains_csv
    print(len(df))
    print(df.head())

    entity_records = df.to_dict("records")
    # todo: consider splitting entities table from entities_domains table

    db.migrate_entities_table(destructive=True)
    db.save_entities(entity_records)
