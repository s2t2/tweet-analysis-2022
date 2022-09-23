

#def parse_full_text(tweet):
#    return tweet.text


def parse_tweet(tweet):
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

    #
    # TWEET
    #

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

            #original = [tweet for tweet in tweets if tweet.id == ref_id][0]
            #try:
            #    original = [tweet for tweet in tweets if tweet.id == ref_id][0]
            #except Exception as err:
            #    #print(err, "original tweet not found. will need to look it up later.") #> list index out of range
            #    original = None

            if ref_type == "retweeted":
                #print("... RT")
                retweet_status_id = ref_id
                #if original:
                #    retweet_user_id = original.author_id
                #    full_text = original.text

            elif ref_type == "replied_to":
                #print("... REPLY")
                reply_status_id = ref_id
                #if original:
                #    reply_user_id = original.author_id
            elif ref_type == "quoted":
                #print("... QUOTE")
                quote_status_id = ref_id
                #if original:
                #    quote_user_id = original.author_id

    # USER (TODO)
    user_screen_name, user_name, user_created_at, user_verified = None, None, None, None

    tweet_record = {
        "status_id": tweet.id,
        "status_text": tweet.text, # parse_full_text(tweet),
        "created_at": tweet.created_at,
        # user info
        "user_id": tweet.author_id,
        "user_screen_name": user_screen_name,
        "user_name": user_name,
        "user_created_at": user_created_at,
        "user_verified": user_verified,

        "retweet_status_id": retweet_status_id,
        "retweet_user_id": retweet_user_id,
        "reply_status_id": reply_status_id,
        "reply_user_id": reply_user_id,
        "quote_status_id": quote_status_id,
        "quote_user_id": quote_user_id,
    }

    #
    # ENTITIES
    #

    annotation_records = []
    mention_records = []

    entities = tweet.get("entities")
    if entities:
        annotations = entities.get("annotations") or []
        annotation_records = [{
            "probability": a["probability"],
            "type": a["type"],
            "normalized_text": a["normalized_text"]
        } for a in annotations]

        mentions = entities.get("mentions") or []
        mention_records = [{
            "screen_name": m["username"],
            "user_id": m["id"]
        } for m in mentions]

    return tweet_record, annotation_records, mention_records
