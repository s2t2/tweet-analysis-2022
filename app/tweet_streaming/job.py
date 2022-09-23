
import os
from pprint import pprint
#from time import sleep
from datetime import datetime

from tweepy.streaming import StreamingClient
from tweepy import StreamRule
#from urllib3.exceptions import ProtocolError

#from app import seek_confirmation
from app.twitter_service import TWITTER_BEARER_TOKEN

BATCH_SIZE = int(os.getenv("BATCH_SIZE", default="50"))


class MyClient(StreamingClient):
    """
    The filtered stream deliver Tweets to you in real-time that match on a set of rules.
    Rules are made up of operators that are used to match on a variety of Tweet attributes.
    Multiple rules can be applied to a stream using the POST /tweets/search/stream/rules endpoint.
    Once you've added rules and connect to your stream using the GET /tweets/search/stream endpoint,
        only those Tweets that match your rules will be delivered in real-time through a persistent streaming connection.
        You do not need to disconnect from your stream to add or remove rules.

    See:
        https://docs.tweepy.org/en/stable/streaming.html#using-streamingclient
        https://docs.tweepy.org/en/stable/streamingclient.html#tweepy.StreamingClient

    Data received from the stream is passed to on_data().
    This method handles sending the data to other methods.
        Tweets recieved are sent to on_tweet(),
        includes data are sent to on_includes(),
        errors are sent to on_errors(), and
        matching rules are sent to on_matching_rules().

    A StreamResponse instance containing all four fields is sent to on_response().
    By default, only on_response() logs the data received, at the DEBUG logging level.

    Errors:
        on_closed() is called when the stream is closed by Twitter.
        on_connection_error() is called when the stream encounters a connection error.
        on_request_error() is called when an error is encountered while trying to connect to the stream.
        on_request_error() is also passed the HTTP status code that was encountered.
        The HTTP status codes reference for the Twitter API can be found at https://developer.twitter.com/en/support/twitter-api/error-troubleshooting.
    """

    def __init__(self, bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True, batch_size_limit=BATCH_SIZE):
        # todo: also consider passing max_retries
        super().__init__(bearer_token=bearer_token, wait_on_rate_limit=wait_on_rate_limit)

        print("-----------")
        print("STREAMING CLIENT!")
        print("  RUNNING:", self.running)
        print("  SESSION:", self.session)
        print("  THREAD:", self.thread)
        print("  USER AGENT:", self.user_agent)

        #seek_confirmation()

        # self.add_rules()

        self.counter = 0 # refers to the number of tweets processed
        self.batch_size_limit = batch_size_limit # refers to the max number of tweets in the batch before saving
        self.batch = self.default_batch

    @property
    def default_batch(self):
        return {
            "tweets": [],
            "annotations": [],
            "context_entities": [],
            "hashtags": [],
            "mentions": [],
            "users": [],
        }

    @property
    def stream_params(self):
        """
        Expansions: https://developer.twitter.com/en/docs/twitter-api/expansions

        Media Fields: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/media

        Tweet Fields: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet

        User Fields: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user
        """
        return dict(
            backfill_minutes=5,
            #threaded=False
            expansions=[
                'author_id',
                'attachments.media_keys',
                'referenced_tweets.id', # Returns a Tweet object that this Tweet is referencing (either as a Retweet, Quoted Tweet, or reply)
                'referenced_tweets.id.author_id', # Returns a user object for the author of the referenced Tweet
                'in_reply_to_user_id',
                #'geo.place_id',
                'entities.mentions.username'
            ],
            media_fields=[
                'duration_ms',
                'preview_image_url',
                'public_metrics',
                'width',
                'alt_text',
                'url',
                ],
            #place_fields=[],
            #poll_fields=[],
            tweet_fields=[
                'author_id',
                'context_annotations',
                'conversation_id',
                'created_at',
                'entities',
                'in_reply_to_user_id',
                'lang',
                'public_metrics', #> retweet_count, reply_count, like_count, quote_count
                'referenced_tweets',
                'source'
            ],
            user_fields=[
                'created_at',
                'description',
                #'location',
                'pinned_tweet_id',
                'profile_image_url',
                'public_metrics', #> followers_count, following_count, tweet_count, listed_count
                'entities',
                'url',
                'verified',
            ],
        )

    def parse_tweets(self, tweets):

        tweet_records = []

        for tweet in tweets:

            retweet_status_id, reply_status_id, quote_status_id = None, None, None
            if hasattr(tweet, "referenced_tweets") and tweet["referenced_tweets"]:
                ref_tweets = tweet["referenced_tweets"]
                for ref_tweet in ref_tweets:
                    ref_type = ref_tweet.type #> "replied_to", "retweeted", "quoted"
                    if ref_type == "retweeted":
                        retweet_status_id = ref_tweet.id
                    elif ref_type == "replied_to":
                        reply_status_id = ref_tweet.id
                    elif ref_type == "quoted":
                        quote_status_id = ref_tweet.id

            tweet_records.append({
                "status_id": tweet.id,
                "status_text": tweet.text,
                "created_at": tweet.created_at,
                # user info:
                "user_id": tweet.author_id,
                # referenced tweet info:
                "retweet_status_id": retweet_status_id,
                "reply_status_id": reply_status_id,
                "quote_status_id": quote_status_id,
                # this is new
                "conversation_id": tweet.conversation_id,
            })

        return tweet_records

    def parse_users(self, users):
        user_records = []
        user_hashtag_records = []
        user_mention_records = []

        for user in users:
            user_records.append({
                "user_id": user.id,
                "screen_name": user.username,
                "name": user.name,
                "description": user.description,
                "url": user.url,
                "profile_image_url": user.profile_image_url,
                "verified": user.verified,
                "created_at": user.created_at,
                "pinned_tweet_id": user.pinned_tweet_id, #> todo: request this
                "followers_count": user.public_metrics["followers_count"],
                "following_count": user.public_metrics["following_count"],
                "tweet_count": user.public_metrics["tweet_count"],
                "listed_count": user.public_metrics["listed_count"],
                "accessed_at": datetime.now(),
            })

            profile_entities = user.entities.get("description") or {}
            hashtags = profile_entities.get("hashtags") or []
            mentions = profile_entities.get("mentions") or []

            user_hashtag_records += [{
                "user_id": user.id,
                "tag": tag["tag"],
                "accessed_at": datetime.now()
            } for tag in hashtags]

            user_mention_records += [{
                "user_id": user.id,
                "mention_screen_name": mention["username"],
                "accessed_at": datetime.now()
            } for mention in mentions]

        return user_records, user_hashtag_records, user_mention_records




    def parse_media(self, includes):
        pass





    #
    # DATA PROCESSING
    #

    #def on_data(self, raw_data):
    #    """This is called when raw data is received from the stream.
    #        This method handles sending the data to other methods.
    #    """
    #    print("-----------")
    #    print("ON DATA!")
    #    print(type(raw_data)) #> bytes

    #def on_tweet(self, tweet):
    #    """This is called when a Tweet is received."""
    #    print("----------------")
    #    self.counter +=1
    #    print(f"DETECTED AN INCOMING TWEET! ({self.counter} -- {tweet.id})")
    #
    #    tweet_record, annotation_records, mention_records = parse_tweet(tweet)
    #
    #    #self.update_batch(tweet)
    #    #if len(self.batch["tweets"]) >= self.batch_size:
    #        #self.save_and_clear_batch()

    #def on_includes(self, includes):
    #    """This is called when includes are received."""
    #    print("-----------")
    #    print("ON INCLUDES!")
    #    print(type(includes)) #> dict
    #    print(includes)
    #    breakpoint()
    #    #> {
    #    #>     'users': [
    #    #>         <User id=734791975 name=Deb2 username=deb2_debra>,
    #    #>         <User id=165185845 name=Claudia Maheux 🇨🇦 username=claudiacm1146>],
    #    #>     'tweets': [
    #    #>         <Tweet id=1573037573425053703 text='Oh my! Heads are going to roll! #Jan6 #Jan6th #Jan6thInsurrection #GOP\nhttps://t.co/oLEJpp6qgq'>
    #    #>     ]
    #    #> }

    #def on_errors(self, errors):
    #    """This is called when errors are received."""
    #    print("-----------")
    #    print("ON ERRORS!")
    #    print(type(errors)) #> dict
    #    print(errors)

    #def on_matching_rules(self, matching_rules):
    #    print("-----------")
    #    print("ON MATCHING RULES!")
    #    print(matching_rules)
    #    #breakpoint()
    #    #> [StreamRule(value=None, tag='', id='1573058221656375301'), StreamRule(value=None, tag='', id='1573064191111577601')]
    #    # WEIRD THAT THE VALUES ARE NULL?

    def on_response(self, response):
        print("-----------------")
        print("ON RESPONSE...")
        self.counter += 1
        print(self.counter, "---", response)

        self.update_batch(response)

        if len(self.batch["tweets"]) > self.batch_size_limit:
            # TODO: self.storage.save_batch(self.batch)
            self.batch = self.default_batch

    def update_batch(self, response):
        # wrapper for named tuple ("data", "includes", "errors", "matching_rules")
        tweet = response.data
        includes = response.includes
        if any(response.errors):
            print("-----------")
            print("ERRORS...")
            breakpoint()

        #tweet_record, annotation_records, mention_records = parse_tweet(tweet)
        #self.batch["tweets"].append(tweet_record)
        #self.batch["annotations"] += annotation_records
        #self.batch["mentions"] += mention_records

        users = includes.get("users") or []
        user_records, user_hashtag_records, user_mention_records = self.parse_users(users)
        self.batch["users"] += user_records
        self.batch["user_hashtags"] += user_hashtag_records
        self.batch["user_mentions"] += user_mention_records

        ref_tweets = includes.get("tweets") or []
        tweets = [tweet] + ref_tweets
        results = self.parse_tweets(tweets)






    #
    # ERROR HANDLING
    #

    def on_close(self):
        print("-----------")
        print("STREAM ON CLOSE!")

    def on_connection_error(self):
        print("-----------")
        print("STREAM ON CONNECTION ERROR!")
        #self.disconnect()
        pass






if __name__ == "__main__":

    #client = MyClient(TWITTER_BEARER_TOKEN, wait_on_rate_limit=True)
    client = MyClient()

    #
    # ADD RULES (todo: get these from the database)
    #
    #   https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule
    #   https://docs.tweepy.org/en/stable/streamrule.html#tweepy.StreamRule
    #   https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/api-reference/post-tweets-search-stream-rules
    #
    # All operators are evaluated in a case-insensitive manner. For example, the rule cat will match all of the following: cat, CAT, Cat.
    #
    # EXAMPLES:
    #   `#MyTag`
    #   `"twitter data" has:mentions (has:media OR has:links)` ...
    #   `snow day #NoSchool` ... will match Tweets containing the terms snow and day and the hashtag #NoSchool.
    #   `grumpy OR cat OR #meme` will match any Tweets containing at least the terms grumpy or cat, or the hashtag #meme.
    #   `cat #meme -grumpy` ... will match Tweets containing the hashtag #meme and the term cat, but only if they do not contain the term grumpy.
    #   `(grumpy cat) OR (#meme has:images)` ... will return either Tweets containing the terms grumpy and cat, or Tweets with images containing the hashtag #meme. Note that ANDs are applied first, then ORs are applied.
    print("RULES:")
    rules = [
        "@January6thCmte lang:en",
        "#January6Committe lang:en",
        "#January6Hearing lang:en",
        "#Jan6Committee lang:en",
        "#Jan6 lang:en",
    ] # TODO: self.storage.fetch_rules()
    stream_rules = [StreamRule(rule) for rule in rules]
    client.add_rules(stream_rules)
    print(client.get_rules())

    # go listen for tweets matching the specified rules
    # https://github.com/tweepy/tweepy/blob/9b636bc529687dbd993bb1aef0177ee78afdabec/tweepy/streaming.py#L553



    # Streams about 1% of all Tweets in real-time.
    #client.sample()
    #client.sample(**stream_params)

    # Streams Tweets in real-time based on a specific set of filter rules.
    #client.filter()
    client.filter(**client.stream_params)
