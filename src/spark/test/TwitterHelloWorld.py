
# coding: utf-8

# In[5]:


import os
import time
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
 
consumer_key = 'euUrwE9cTbLYda2WRNfnIO9ql'
consumer_secret = 'CJDsMstUENkHwAlOuGhjOuKlMUCzMtw0NCpSpJNW7VLXHgz5bl'
access_token = '32018698-Znhj0YwI1zOnd6J5ZA1FjtqBKtXOZaqQYaUcv4zFZ'
access_secret = '2GEBbFn98RYKAptPrszyLudIpmyINb6m7pSUBW6ycS7gp'
 
class TweetsListener(StreamListener):
    
    def __init__(self):
        self.siesta = 0
    
    def on_data(self, data):
        try:
            tweet = json.loads(data)
            print(tweet['text'])
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status_code):
        print('Errouuuu:' + str(status_code))

        if status_code == 420:
            sleepy = 60 * math.pow(2, self.siesta)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print ("A reconnection attempt will occur in " +             str(sleepy/60) + " minutes.")
            print ('''
            *******************************************************************
            From Twitter Streaming API Documentation
            420: Rate Limited
            The client has connected too frequently. For example, an 
            endpoint returns this status if:
            - A client makes too many login attempts in a short period 
              of time.
            - Too many copies of an application attempt to authenticate 
              with the same credentials.
            *******************************************************************
            ''')
            time.sleep(sleepy)
            self.siesta += 1

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
myListener = TweetsListener()
twitter_stream = Stream(auth, myListener)
twitter_stream.filter(track=['flamengo'])


# In[3]:





# In[4]:




