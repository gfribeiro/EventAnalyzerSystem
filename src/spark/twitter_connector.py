# twitter_connector.py
import time
import socket
import json
import datetime
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from database import DataManager

class TweetsListener(StreamListener):
    
    def __init__(self,socketConnection):
        self.timeout = 0
        self.clientSocket = socketConnection
    
    def on_data(self, data):
        try:
            self.clientSocket.send(data.encode())
            tweet = json.loads(data)
            print(str(datetime.datetime.now()), file=open("connector.log","a"))
            return True
        except BaseException as e:
            print("[ERROR] on_data: %s" % str(e))
        return True

    def on_error(self, statusCode):
        print('[ERROR] on_error:' + str(statusCode))

        if statusCode == 420:
            print("[WARNING] TWITTER STREAMMING API - Rate limited! CODE: 420")
            sleepy = 60 * math.pow(2, self.timeout)
            print(time.strftime("%Y%m%d_%H%M%S"))
            print ("A reconnection attempt will occur in " +             str(sleepy/60) + " minutes.")
            time.sleep(sleepy)
            self.timeout += 1

def sendTwitterData(appConfig,monitoringKeywords,socketConnection):
    auth = OAuthHandler(appConfig['TWITTER_API_KEY'], appConfig['TWITTER_API_SECRET'])
    auth.set_access_token(appConfig['TWITTER_ACCESS_TOKEN'], appConfig['TWITTER_ACCESS_TOKEN_SECRET'])
    myListener = TweetsListener(socketConnection)
    twitter_stream = Stream(auth, myListener)
    twitter_stream.filter(track=[monitoringKeywords])

if __name__ == "__main__":
    dataManager = DataManager()
    appConfig = dataManager.getAppConfig()
    monitoringKeywords = dataManager.getMonitoringKeywords()
    s = socket.socket()     # Create a socket object
    host = appConfig['TWITTER_CONNECTOR_HOST']      # Get local machine name
    port = int(appConfig['TWITTER_CONNECTOR_PORT'])             # Reserve a port for your service.
    s.bind((host, port))    # Bind to the port

    print("Listening on port: " + str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print("Received request from: " + str(addr))
    try:
        sendTwitterData(appConfig, monitoringKeywords, c)
    except Exception as e:
        print("[ERROR] sendTwitterData: %s" % str(e))
        sendTwitterData(appConfig, monitoringKeywords, c)