# Tweet Collection (2022)

## Setup

You can use a SQLite database, or a BigQuery database. If you want to start with an SQLite database, feel free to skip the BigQuery setup steps below.

### BigQuery Setup

Create two new datasets for each new collection effort. Named `DATASET_ID_development` and `DATASET_ID_production`.


If this is your first time setting up the database, also run the migrations to create the tables:

```sh
python -m app.tweet_collection.bq_migrations
```

> NOTE: be careful when running this script, it is destructive!


### Twitter API Setup


## Usage

Collect tweets:

```sh
python -m app.tweet_collection.job

# pass custom params:
START_DATE="2022-07-01" END_DATE="2022-07-01" QUERY="lang:en #january6thcommittee" MAX_RESULTS=10 python -m app.tweet_collection.job
```
