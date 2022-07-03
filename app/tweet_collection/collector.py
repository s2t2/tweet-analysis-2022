
# adapted from code by Lucienne Julian (see "notebooks/Tweet_Collector.ipynb")

from app.twitter_service import twitter_api_client


from datetime import datetime

client = twitter_api_client()


# https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query#availability
query='#COP26 lang:en'

start_date="2022-01-01"
end_date="2022-01-03"

# `max_results` must be less than or equal to `100`
# (maybe for pagination)
# sometimes we get 99 / less??
max_results = 100




# start at beginning of day, end at end of day
formatting_template = '%Y-%m-%d %H:%M:%S'
start_time = datetime.strptime(f"{start_date} 00:00:00", formatting_template)
end_time = datetime.strptime(f"{end_date} 23:59:59", formatting_template)

# https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
response = client.search_all_tweets(query=query,
    # can't have spaces between the commas ??
    expansions=['author_id','attachments.media_keys','referenced_tweets.id','geo.place_id'],
    tweet_fields=['created_at','entities','context_annotations'],
    media_fields=['url','preview_image_url'],
    user_fields=['verified'],
    max_results=max_results,
    start_time=start_time,
    end_time=end_time
)
tweets = response.data
print("TWEETS:", len(tweets))
