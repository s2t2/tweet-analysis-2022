
# Tweet Collection (Stream Listener)

Adapted from [previous approach](https://github.com/s2t2/tweet-analysis-2020/tree/main/app/tweet_collection_v2), updated for Twitter API v2 (Tweepy package v4).


## Setup

Setup your twitter credentials, sendgrid credentials, and demonstrate your ability to connect, as described in the [README](/README.md).

Also choose a database (CSV or bigquery), and if bigquery, run migrations and test your ability to connect, as described in the [README](/README.md).


### CSV File Storage

Make a directory in the "data/tweet_streaming" dir called something like "jan6_committee", for your own event name. In it create a "topics.csv" file with contents like:

    topic
    #Jan6Committee
    #January6Committe
    January 6th
    etc...


> FYI: the first row "topic" is a required column header. Twitter will match these topics case-insensitively and inclusively, so a topic of "rain" would include tweets about "#Rainbows".

> GUIDANCE: a narrow set of specific hashtags (i.e. "TrumpImpeachment") may be less likely to encounter crushing rate limits than a broader set of keywords (i.e. "impeach")


Collecting tweets locally (where `EVENT_NAME` is the directory where the local "topics.csv" file is stored):

```sh
STORAGE_ENV="local" EVENT_NAME="jan6_committee" python -m app.tweet_streaming.job
```

> NOTE: run this for a while and make sure you aren't getting rate limited too bad, otherwise try removing some topics / splitting topics across more collection servers. it is harder to remove a topic once it has hit the remote databases...
>
## Usage

```sh
python app.tweet_streaming.job

EVENT_NAME="jan6_committee" python app.tweet_streaming.job
```
