
# Tweet Collection (Stream Listener)

Adapted from [previous approach](https://github.com/s2t2/tweet-analysis-2020/tree/main/app/tweet_collection_v2), updated for Twitter API v2 (Tweepy package v4).

## Setup

Setup your Twitter credentials, and demonstrate your ability to connect, as described in the [README](/README.md).

Choose a database (SQLite or BigQuery), and if BigQuery, test your ability to connect, as described in the [README](/README.md).

### Database Migrations

BigQuery Migration:

```sh
# WARNING!!! USE WITH CAUTION!!!
DATASET_ADDRESS="YOUR_PROJECT.YOUR_DATASET" python -m app.tweet_streaming.bq_migrations
```

There is no need to migrate the SQLite database.

### Database Seeds (Adding Rules)

Make a directory in the "data/tweet_streaming" directory with a name representing your own `EVENT_NAME` (e.g. "jan6_committee"). In it create a "rules.csv" file with contents like:

    rule
    #Jan6Committee lang:en
    #January6Committe lang:en
    etc...


> FYI: the first row "rule" is a required column header

Seed your chosen database with rules from the CSV file:

```sh
# for BigQuery:
STORAGE_MODE="remote" DATASET_ADDRESS="YOUR_PROJECT.YOUR_DATASET" python -m app.tweet_streaming.seed_rules
#EVENT_NAME="YOUR_EVENT" DATASET_ADDRESS="YOUR_PROJECT.YOUR_DATASET" python -m app.tweet_streaming.seed_rules

# for SQLite:
STORAGE_MODE="local" python -m app.tweet_streaming.seed_rules
# EVENT_NAME="YOUR_EVENT" python -m app.tweet_streaming.seed_rules
```

## Usage

```sh
python app.tweet_streaming.job

# with batch size:
BATCH_SIZE=5 python app.tweet_streaming.job

# storing to SQLite:
STORAGE_MODE="local" python app.tweet_streaming.job

# storing to BigQuery:
STORAGE_MODE="remote" DATASET_ADDRESS="YOUR_PROJECT.YOUR_DATASET" python app.tweet_streaming.job
```
