
import os
#from pprint import pprint
#from time import sleep

from tweepy.streaming import StreamingClient
from tweepy import StreamRule
#from urllib3.exceptions import ProtocolError

#from app import seek_confirmation
from app.twitter_service import TWITTER_BEARER_TOKEN
from app.tweet_streaming.parser import parse_tweet

BATCH_SIZE = int(os.getenv("BATCH_SIZE", default="20"))


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

    def __init__(self, bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True, batch_size=BATCH_SIZE):
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

        self.counter = 0
        self.batch_size = batch_size
        self.tweets_batch = []
        self.mentions_batch = []
        self.annotations_batch = []

    #
    # DATA PROCESSING
    #

    # DON'T OVERRIDE THIS ORCHESTRATION FUNCTION!
    #
    #def on_data(self, raw_data):
    #    """This is called when raw data is received from the stream.
    #        This method handles sending the data to other methods.
    #    """
    #    print("-----------")
    #    print("ON DATA!")
    #    print(type(raw_data)) #> bytes
    #    #breakpoint()

    def on_tweet(self, tweet):
        """This is called when a Tweet is received."""
        self.counter +=1
        print("----------------")
        print(f"DETECTED AN INCOMING TWEET! ({self.counter} -- {tweet.id})")

        #self.store_in_batches(tweet)
        tweet_record, annotation_records, mention_records = parse_tweet(tweet)
        self.tweets_batch.append(tweet_record)
        self.annotations_batch += annotation_records
        self.mentions_batch += mention_records
        #self.hashtags_batch += hashtag_records

        if len(self.tweets_batch) >= self.batch_size:
            self.store_and_reset_batches()




    def on_includes(self, includes):
        """This is called when includes are received."""
        print("-----------")
        print("ON INCLUDES!")
        print(type(includes)) #> dict
        print(includes)
        #breakpoint()
        #> {
        #>     'users': [
        #>         <User id=734791975 name=Deb2 username=deb2_debra>,
        #>         <User id=165185845 name=Claudia Maheux 🇨🇦 username=claudiacm1146>],
        #>     'tweets': [
        #>         <Tweet id=1573037573425053703 text='Oh my! Heads are going to roll! #Jan6 #Jan6th #Jan6thInsurrection #GOP\nhttps://t.co/oLEJpp6qgq'>
        #>     ]
        #> }

    def on_errors(self, errors):
        """This is called when errors are received."""
        print("-----------")
        print("ON ERRORS!")
        print(type(errors)) #> dict
        print(errors)
        #breakpoint()

    #def on_matching_rules(self, matching_rules):
    #    print("-----------")
    #    print("ON MATCHING RULES!")
    #    print(matching_rules)
    #    #breakpoint()
    #    #> [StreamRule(value=None, tag='', id='1573058221656375301'), StreamRule(value=None, tag='', id='1573064191111577601')]
    #    # WEIRD THAT THE VALUES ARE NULL?

    #
    # ERROR HANDLING
    #

    def on_close(self):
        print("-----------")
        print("ON CLOSE!")
        #breakpoint()

    def on_connection_error(self):
        print("-----------")
        print("ON CONNECTION ERROR!")
        #breakpoint()
        #self.disconnect()

    #
    # SAVING DATA
    #

    #def store_in_batches(self, tweet):
    #    tweet_record, annotation_records, mention_records = parse_tweet(tweet)
    #
    #    self.tweets_batch.append(tweet_record)
    #    self.annotations_batch += annotation_records
    #    self.mentions_batch += mention_records
    #    #self.hashtags_batch += hashtag_records
    #
    #    #if len(self.tweets_batch) >= self.batch_size:
    #    #    self.store_and_clear_tweets_batch()
    #    #if len(self.annotations_batch) >= self.batch_size:
    #    #    self.store_and_clear_annotations_batch()
    #    #if len(self.mentions_batch) >= self.batch_size:
    #    #    self.store_and_clear_mentions_batch()
    #    if len(self.tweets_batch) >= self.batch_size:
    #        self.store_and_clear_batches()


    def store_and_reset_batches(self):
        print("STORING BATCHES OF...", len(self.tweets_batch), "TWEETS",
            "|", len(self.annotations_batch), "ANNOTATIONS",
            "|", len(self.mentions_batch), "MENTIONS",
        )
        #TODO: self.storage.save_tweets(self.tweets_batch)
        #TODO: self.storage.save_annotations(self.annotations_batch)
        #TODO: self.storage.save_mentions(self.tweets_batch)
        print("CLEARING BATCHES...")
        self.counter = 0
        self.tweets_batch = []
        self.annotations_batch = []
        self.mentions_batch = []







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
        StreamRule("@January6thCmte lang:en"),
        StreamRule("#January6Committe lang:en"),
        StreamRule("#January6Hearing lang:en"),
        StreamRule("#Jan6Committee lang:en"),
        StreamRule("#Jan6 lang:en"),
    ]
    client.add_rules(rules)
    print(client.get_rules())

    # go listen for tweets matching the specified rules
    # https://github.com/tweepy/tweepy/blob/9b636bc529687dbd993bb1aef0177ee78afdabec/tweepy/streaming.py#L553

    #stream_params = dict(backfill_minutes=5,
    #    expansions=[],
    #    media_fields=[],
    #    place_fields=[],
    #    poll_fields=[],
    #    tweet_filelds=[],
    #    user_fields=[],
    #    threaded=False
    #)

    # using similar params as our search collection script:
    stream_params = dict(
        expansions=[
            'author_id',
            'attachments.media_keys',
            'referenced_tweets.id',
            'referenced_tweets.id.author_id', # didn't come through so...
            #'referenced_tweets.author_id', # making this up? > JK, LEADS TO 400 ERRORS!
            #'in_reply_to_user_id',
            #'geo.place_id',
            'entities.mentions.username'
        ],
        tweet_fields=['created_at', 'entities', 'context_annotations'],
        media_fields=['url', 'preview_image_url'],
        user_fields=['verified', 'created_at'],
    )

    # Streams about 1% of all Tweets in real-time.
    #client.sample()
    #client.sample(**stream_params)

    # Streams Tweets in real-time based on a specific set of filter rules.
    #client.filter()
    client.filter(**stream_params)
