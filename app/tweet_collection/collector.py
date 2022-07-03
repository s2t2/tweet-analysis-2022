
# adapted from code by Lucienne Julian (see "notebooks/Tweet_Collector.ipynb")


from datetime import datetime
import os
from pprint import pprint

from dotenv import load_dotenv
from tweepy import Paginator

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
        # can't have spaces between the commas ??
        expansions=['author_id','attachments.media_keys','referenced_tweets.id','geo.place_id'],
        tweet_fields=['created_at','entities','context_annotations'],
        media_fields=['url','preview_image_url'],
        user_fields=['verified'],
        max_results=max_results,
        start_time=start_time,
        end_time=end_time
    )
    #return Paginator(client.search_all_tweets, limit=limit, **request_params)
    args = {}
    if limit:
        args["limit"] = int(limit)
    return Paginator(client.search_all_tweets, **args, **request_params)



if __name__ == "__main__":

    #response = fetch_tweets()
    #tweets = response.data
    #print("TWEETS:", len(tweets))
    #pprint(dict(tweets[0]))

    page_counter = 0
    for response in fetch_tweets():
        page_counter+=1
        tweets = response.data
        print("PAGE:", page_counter, "TWEETS:", len(tweets))
        #pprint(dict(tweets[0]))
