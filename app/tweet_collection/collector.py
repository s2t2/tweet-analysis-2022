
# adapted from code by Lucienne Julian (see "notebooks/Tweet_Collector.ipynb")


from datetime import datetime
import os
from pprint import pprint

from dotenv import load_dotenv
from tweepy import Paginator
from pandas import DataFrame, concat

from app.twitter_service import twitter_api_client

load_dotenv()

# https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#availability
QUERY = os.getenv("QUERY", default="#COP26 lang:en")

START_DATE = os.getenv("START_DATE", default="2022-01-01")
END_DATE = os.getenv("END_DATE", default="2022-01-03")

MAX_RESULTS = int(os.getenv("MAX_RESULTS", default="100")) # per page
PAGE_LIMIT = os.getenv("PAGE_LIMIT") # num of pages max (use only in development)

def fetch_tweets(query=QUERY, start_date=START_DATE, end_date=END_DATE, max_results=MAX_RESULTS, limit=PAGE_LIMIT):
    """
    Params
        query (str) : '#COP26 lang:en'
            see https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#availability

        start_date (str) : '2022-01-01'

        end_date (str) : '2022-01-03'

        max_results (int) : 100
            (must be less than or equal to 100)
            (maybe for pagination)
            (sometimes we get 99 / less??)

    """
    client = twitter_api_client()

    # start at beginning of day, end at end of day
    formatting_template = '%Y-%m-%d %H:%M:%S'
    start_time = datetime.strptime(f"{start_date} 00:00:00", formatting_template)
    end_time = datetime.strptime(f"{end_date} 23:59:59", formatting_template)

    # https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
    #return client.search_all_tweets(query=query,
    #    # can't have spaces between the commas ??
    #    expansions=['author_id','attachments.media_keys','referenced_tweets.id','geo.place_id'],
    #    tweet_fields=['created_at','entities','context_annotations'],
    #    media_fields=['url','preview_image_url'],
    #    user_fields=['verified'],
    #    max_results=max_results,
    #    start_time=start_time,
    #    end_time=end_time
    #)

    request_params = dict(
        query=query,
        expansions=[
            'author_id',
            'attachments.media_keys',
            'referenced_tweets.id',
            'referenced_tweets.id.author_id',
            #'in_reply_to_user_id',
            'geo.place_id',
            'entities.mentions.username'
        ],
        tweet_fields=['created_at', 'entities', 'context_annotations'],
        media_fields=['url', 'preview_image_url'],
        user_fields=['verified', 'created_at'],
        max_results=max_results,
        start_time=start_time,
        end_time=end_time
    )
    args = {}
    if limit:
        args["limit"] = int(limit)
    #return Paginator(client.search_all_tweets, limit=limit, **request_params)
    return Paginator(client.search_all_tweets, **args, **request_params)


def process_response(response):
    """
    Param response : tweepy.client.Response
    """
    tweet_records = []
    users = response.includes["users"]
    media = response.includes["media"]
    tweets = response.includes["tweets"]

    for tweet in response.data:
        user_id = tweet.author_id
        user = [user for user in users if user.id == user_id][0]

        full_text = tweet.text

        retweet_status_id, retweet_user_id = None, None
        reply_status_id, reply_user_id = None, None
        quote_status_id, quote_user_id = None, None

        # sometimes value can be None even if attr is present (WEIRD)
        if hasattr(tweet, "referenced_tweets") and tweet["referenced_tweets"]:
            referenced_tweets = tweet["referenced_tweets"]
            #print("REFS:", len(referenced_tweets))
            for ref in referenced_tweets:
                ref_id = ref.id
                ref_type = ref.type #> "replied_to", "retweeted", "quoted"
                original = [tweet for tweet in tweets if tweet.id == ref_id][0]
                if ref_type == "retweeted":
                    #print("... RT")
                    retweet_status_id = ref_id
                    retweet_user_id = original.author_id
                    full_text = original.text
                elif ref_type == "replied_to":
                    #print("... REPLY")
                    reply_status_id = ref_id
                    reply_user_id = original.author_id
                elif ref_type == "quoted":
                    #print("... QUOTE")
                    quote_status_id = ref_id
                    quote_user_id = original.author_id


        tweet_records.append({
            "status_id": tweet.id,
            "status_text": full_text,
            "created_at": tweet.created_at,
            "user_id": user_id,
            "user_screen_name":user.username,
            "user_name": user.name,
            "user_created_at": user.created_at,
            "user_verified": user.verified,
            # metadata for now
            #"ref_types": ref_types
            "retweet_status_id": retweet_status_id,
            "retweet_user_id": retweet_user_id,
            "reply_status_id": reply_status_id,
            "reply_user_id": reply_user_id,
            "quote_status_id": quote_status_id,
            "quote_user_id": quote_user_id,
        })
    return DataFrame(tweet_records)




if __name__ == "__main__":

    #response = fetch_tweets()
    #tweets = response.data
    #print("TWEETS:", len(tweets))
    #pprint(dict(tweets[0]))

    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    csv_filepath = os.path.join(DATA_DIR, "tweets.csv")

    dfs = []
    page_counter = 0
    for response in fetch_tweets():
        page_counter+=1
        print("PAGE:", page_counter, "TWEETS:", len(response.data))
        #pprint(dict(tweets[0]))

        tweets_df = process_response(response)
        print(tweets_df.head())
        #tweets_df.to_csv(csv_filepath)
        dfs.append(tweets_df)

    print("WRITING TO CSV...")
    final_df = concat(dfs)
    final_df.to_csv(csv_filepath, index=False)
