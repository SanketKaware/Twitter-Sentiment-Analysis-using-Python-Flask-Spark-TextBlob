import socket
import sys
import json
import requests
import requests_oauthlib
import re

class TwitterFetch():
    # Replace the values below with your relevant TWITTER Credentials
    ACCESS_TOKEN = '%Your Access Token%'
    ACCESS_SECRET = '%Your Access Secret%'
    CONSUMER_KEY = '%Your Consumer Key%'
    CONSUMER_SECRET = '%Your Consumer Secret%'
    TCP_IP = "localhost"
    TCP_PORT = 9009

    user_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
    conn = None
    sock = None
    topic_name = None
    flag_fetch = True
    
    test_var = "Init"

    spsc = None

    def send_tweets_spark(self,http_resp, tcp_connection):
        '''
        The function receives TCP connection and reponse as parameters
        and loads current tweets through TWITTER API and send to Spark.
        Args:
            http_resp (object) : It is a http respnose which is returning tweets.
            tcp_connection (object) : TCP socket connection object
        Returns:
            None
        '''
        for line in http_resp.iter_lines():
            try:
                if(self.flag_fetch == True):
                    full_tweet = json.loads(line)
                    tweet_text = full_tweet['text']
                    tweet_text = full_tweet['text'] + "||"
                    tweet_text_clean = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet_text).split()) 
                    print("Tweet Text: " + tweet_text)
                    print("Clean Tweet: " + tweet_text_clean)
                    print("~" * 40) # to print a line as seperator
                    tweet_text_clean = tweet_text_clean + "||"
                    tcp_connection.send(tweet_text.encode('utf-8'))
                else:
                    print("Connection closed")
                    tcp_connection.close()
                    break
            except:
                e = sys.exc_info()[0]
                print("Error: %s" % e)
                tcp_connection.close()
                break

    def get_twitter_tweets(self):
        '''
        The function queries for data from currently running tweets from Twitter API and
        print the respective url
        Returns:
            object : http response object after getting tweets from twitter API
        '''
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        query_data = [
            ('language', 'en'),
            #('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'),
            ('track', self.topic_name)
        ]
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        response = requests.get(query_url, auth=self.user_auth, stream=True)
        print(query_url, response)
        return response

    def fetch(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.TCP_IP, self.TCP_PORT))
        self.sock.listen(1)
        print("Waiting for TCP connection to open...")
        self.conn, addr = self.sock.accept()
        print("Connected... Tweets are getting started.")
        resp = self.get_twitter_tweets()
        self.send_tweets_spark(resp, self.conn)

    def abort(self):
        self.conn.close()
        self.sock.close()
        print("Connection Aborted")

    
    
    



