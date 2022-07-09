

from app.tweet_collection.db import CollectionDatabase
from app.tweet_collection.twitterdev import fetch_context_entities

#def list_to_csv(domain_ids):
#    # SQLite doesn't support nested datatypes like lists, so let's do CSV string for now
#    # in future we could make a separate entities_domains table (but let's do what bq does for now)
#    return ",".join(str(domain_ids))

if __name__ == "__main__":

    db = CollectionDatabase()
    print(db.filepath.upper())

    print("---------------------")
    print("FETCHING CONTEXT ENTITIES AND DOMAINS...")
    df = fetch_context_entities()
    df.drop(columns=["domain_ids"], inplace=True) # use domains_csv string instead
    print(len(df))
    print(df.head())

    entity_records = df.to_dict("records")
    # todo: consider splitting entities table from entities_domains table

    #db.migrate_entities_table(destructive=True)
    db.drop_entities_table()
    db.save_entities(entity_records)
