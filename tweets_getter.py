## Importing all necessary library

import tweepy
import boto3
import json
from datetime import datetime, timedelta

##Tokens from twitter API
AUTH_TOKENS = {
                "API_KEY" : "",
                "API_KEY_SECRET"  : "", 
                "BEARER_TOKEN" : "",
                "ACCESS_TOKEN" : "",
                "ACCESS_TOKEN_SECRET" : ""
                }

## Initizalizing connection
client = tweepy.Client(AUTH_TOKENS["BEARER_TOKEN"],AUTH_TOKENS["API_KEY"],AUTH_TOKENS["API_KEY_SECRET"],AUTH_TOKENS["ACCESS_TOKEN"],AUTH_TOKENS["ACCESS_TOKEN_SECRET"])

auth = tweepy.OAuth1UserHandler(AUTH_TOKENS["BEARER_TOKEN"],AUTH_TOKENS["API_KEY"],AUTH_TOKENS["API_KEY_SECRET"],AUTH_TOKENS["ACCESS_TOKEN"],AUTH_TOKENS["ACCESS_TOKEN_SECRET"])

api = tweepy.API(auth)


# Setting s3 config to boto3
s3 = boto3.resource(
    's3',
    region_name='sa-east-1'
)

# Filter list to collect twitter tweets
termos_de_busca = ["presidente do brasil" , "Jair Bolsonaro" , "Lula", "Biroliro", "Bozo", "Luladrão", "9 dedos", "Bolsonaro", "Luiz Inácio Lula da Silva", "@LulaOficial", "@jairbolsonaro"]

# How many tweets a json will have
SIZE_OF_JSON = 100

# Primary Classy from tweepy to collect 
class TTStream (tweepy.StreamingClient):
    tweets = [] # Save dict format tweets
    datas = []  # Collect all timestamp from tweets
    GMT = -3    # Set timezone to get correct time
    def on_connect(self):
        return super().on_connect()

    def on_tweet(self, tweet):
        if tweet.referenced_tweets == None:
            # Save a string based on tweet time, and setting a PATH as recommended by AWS by year=/month=/...
            self.datas.append((tweet.created_at + timedelta(hours=self.GMT)).strftime("year=%Y/month=%m/day=%d/%Y-%m-%d %H:%M:%S"))
            
            # Save on tweets list an dict based on fields of interest
            self.tweets.append({'id' : tweet.id,
                                'tweet_text' : tweet.text,
                                'tweet_date' : (tweet.created_at + timedelta(hours=self.GMT)).strftime("%Y-%m-%d %H:%M:%S")})
                              
            if len(self.tweets) == SIZE_OF_JSON:
                tt = self.tweets
                s3.Bucket('YOUR-BUCKET-NAME').put_object(Key='YOUR/PATH/TO/SAVE/'+str(min(self.datas))+'.json', Body=json.dumps(tt))
                self.datas = []
                self.tweets = []
                
stream = TTStream(bearer_token=AUTH_TOKENS['BEARER_TOKEN'])
stream.delete_rules([rule.id for rule in stream.get_rules()[0]])

# Add terms to stream rules
for termo in termos_de_busca:
    stream.add_rules(tweepy.StreamRule(termo))

#Start getting tweets
stream.filter(tweet_fields=['referenced_tweets','id', 'text','created_at'])
