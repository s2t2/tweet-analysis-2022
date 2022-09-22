

#from pprint import pprint
#from time import sleep

from tweepy.streaming import StreamingClient
from tweepy import StreamRule
#from urllib3.exceptions import ProtocolError

#from app import seek_confirmation
from app.twitter_service import TWITTER_BEARER_TOKEN


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

    def __init__(self, bearer_token=TWITTER_BEARER_TOKEN, wait_on_rate_limit=True):
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
        print("-----------")
        print("ON TWEET!")
        print(type(tweet)) #> Tweet model instance
        #print(tweet.id)
        print(tweet.data) #> dict representation, by default contains "id" and "text"
        # 'items', 'keys', 'values',
        # 'id', 'author_id', 'conversation_id', 'created_at', 'in_reply_to_user_id'
        # 'text', 'geo', 'lang', 'possibly_sensitive',
        # 'referenced_tweets', 'reply_settings', 'source', 'withheld'
        # 'attachments',  'context_annotations', 'entities',
        # 'non_public_metrics', 'organic_metrics', 'promoted_metrics', 'public_metrics',

        # WE ARE WORKING WITH SOMETHING LIKE THIS:
        #
        #>{
        #>    'attachments': {},
        #>    'author_id': '1188503073175560192',
        #>    'context_annotations': [
        #>        {
        #>            'domain': {'id': '10', 'name': 'Person', 'description': 'Named people in the world like Nelson Mandela'},
        #>            'entity': {'id': '981179589291515904', 'name': 'Jamie Raskin', 'description': 'US Representative Jamie Raskin (MD-08)'}
        #>        },
        #>        {
        #>            'domain': {'id': '35', 'name': 'Politician', 'description': 'Politicians in the world, like Joe Biden'},
        #>            'entity': {'id': '981179589291515904', 'name': 'Jamie Raskin', 'description': 'US Representative Jamie Raskin (MD-08)'}
        #>        },
        #>        {
        #>            'domain': {'id': '131', 'name': 'Unified Twitter Taxonomy', 'description': 'A taxonomy view into the Semantic Core knowledge graph'},
        #>            'entity': {'id': '847878884917886977', 'name': 'Politics', 'description': 'Politics'}
        #>        },
        #>        {
        #>            'domain': {'id': '131', 'name': 'Unified Twitter Taxonomy', 'description': 'A taxonomy view into the Semantic Core knowledge graph'},
        #>            'entity': {'id': '981179589291515904', 'name': 'Jamie Raskin', 'description': 'US Representative Jamie Raskin (MD-08)'}
        #>        },
        #>        {
        #>            'domain': {'id': '131', 'name': 'Unified Twitter Taxonomy', 'description': 'A taxonomy view into the Semantic Core knowledge graph'},
        #>            'entity': {'id': '1070032753834438656', 'name': 'Political figures', 'description': 'Politician'}
        #>        }
        #>    ],
        #>    'created_at': '2022-09-22T21:38:30.000Z',
        #>    'entities': {
        #>        'annotations': [
        #>            {'start': 27, 'end': 40, 'probability': 0.9801, 'type': 'Person', 'normalized_text': 'Anika Navaroli'}
        #>        ],
        #>        'mentions': [
        #>            {'start': 0, 'end': 10, 'username': 'RepRaskin', 'id': '806906355214852096'},
        #>            {'start': 11, 'end': 26, 'username': 'January6thCmte', 'id': '1415384176593883137'}
        #>        ]
        #>    },
        #>    'geo': {},
        #>    'id': '1573064219926339584',
        #>    'referenced_tweets': [
        #>        {'type': 'replied_to', 'id': '1573033231976464387'}
        #>    ],
        #>    'text': '@RepRaskin @January6thCmte Anika Navaroli is a very brave and patriotic person! I thank her for trying!'
        #>}
        # WITH ATTACHMENTS:  'attachments': {'media_keys': ['13_963799572161101824']}

        print(tweet.id, tweet.text)


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

    def on_matching_rules(self, matching_rules):
        print("-----------")
        print("ON MATCHING RULES!")
        print(matching_rules)
        #breakpoint()
        #> [StreamRule(value=None, tag='', id='1573058221656375301'), StreamRule(value=None, tag='', id='1573064191111577601')]
        # WEIRD THAT THE VALUES ARE NULL?

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
        StreamRule("@January6thCmte lang:en"), # lang:en
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
            'referenced_tweets.id.author_id',
            #'in_reply_to_user_id',
            'geo.place_id',
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
