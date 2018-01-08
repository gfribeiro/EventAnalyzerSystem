# event_processor.py
import json
import nltk
import re
import time
import requests
from database import DataManager
from pprint import pprint

class TwitterProcessor():
    
    def createEvent(self,tweet):
        """Creates an event from a Twitter post"""
        event = {}
        event['timestamp_ms'] = tweet['timestamp_ms']
        if 'retweeted_status' in tweet.keys():
            tweet = tweet['retweeted_status']
        event['source_type'] = 'twitter'
        event['source_id'] = tweet['id']
        event['sender_id'] = tweet['user']['id']
        event['sender_name'] = tweet['user']['name']
        event['sender_influence'] = tweet['user']['followers_count']
        event['event_desc'] = tweet['text']
        event['lang'] = tweet['lang']
        event['reference_count'] = tweet['retweet_count']
        event['favorite_count'] = tweet['favorite_count']
        return event

class EventEnhancer():

    def __init__(self):
        self.LOG_FILE = 'enhance.log'

    def categorizeEvent(self,event):
        """Detects the event category based on a dictionary of keywords"""
        categories = DataManager().getEventCategories()
        tokens = nltk.tokenize.word_tokenize(event['event_desc'])
        for category in categories:
            keywords = category.keywords.split(',')
            for keyword in keywords:
                if keyword in tokens:
                    event['category_name'] = category.category_name
                    break
        return event

    def processGeolocation(self,event):
        """Processes the geolocations of an event:
           1. Applies a regex matching location prepositions to refine event descriptions with possible location information
           2. If matches regex, extract entities from result
           3. Calls the Google Geocoding API
           4. Parses the result and defines locations attributes for the event
        """
        START_TIME = time.monotonic()
        appConfig = DataManager().getAppConfig()
        nlp = NlpToolkit()
        google = GoogleApiToolkit()
        prepositions = self.getLocationPrepositions(event['lang'])

        match = re.search('.*' + prepositions + '\s([A-Z][a-z]+.*)',event['event_desc'])
        if match:
            result = match.group(1)
            entities = nlp.extractEntities(result)
            if len(entities) > 0:
                geocode_result = google.processGeocoding(' '.join(entities), appConfig["GOOGLE_GEOCODE_API_KEY"])
                if geocode_result['status'] == 'OK':
                    event = self.parseGoogleGeocodeData(event,geocode_result)
                elif geocode_result['status'] == 'ZERO_RESULTS':
                    event['location_level'] = -1
                else:
                    print("ERROR: Geocoding Api: %s >> source_id: %s" % (geocode_result['status'],event['source_id']), file=open(self.LOG_FILE,"a"))
            else:
                event['location_level'] = -1
        else:
            event['location_level'] = -1

        ELAPSED_TIME = time.monotonic() - START_TIME
        print('SUCCESS: ELAPSED_TIME: %.3f >> SOURCE_ID: %s' % (ELAPSED_TIME,event['source_id']), file=open(self.LOG_FILE,"a"))
        return event

    def getLocationPrepositions(self,lang):
        """ Receives a language code and returns the prepositions of that language."""
        prepositions = {
            'pt':'[no|em|na]'
        }
        return prepositions.get(lang,'')

    def parseGoogleGeocodeData(self,event,geocodeData):
        event['location_level'] = -1
        event['latitude'] = geocodeData["results"][0]["geometry"]["location"]["lat"]
        event['longitude'] = geocodeData["results"][0]["geometry"]["location"]["lng"]
        for component in geocodeData["results"][0]["address_components"]:
            if 'route' in component["types"]:
                event['street'] = component["long_name"]
                event = self.setEventLocationLevel(event,4)
            
            if 'sublocality' in component["types"]:
                event['neighborhood'] = component["long_name"]
                event = self.setEventLocationLevel(event,3)
            
            if 'locality' in component["types"]:
                event['city'] = component["long_name"]
                event = self.setEventLocationLevel(event,2)
            
            if 'administrative_area_level_1' in component["types"]:
                event['state'] = component["long_name"]
                event = self.setEventLocationLevel(event,1)
            
            if 'country' in component["types"]:
                event['country'] = component["long_name"]
                event = self.setEventLocationLevel(event,0)

        return event

    def setEventLocationLevel(self,event,location_level):
        if event['location_level'] < location_level:
            event['location_level'] = location_level
        return event
        
class GoogleApiToolkit():

    def processGeocoding(self,searchText,appKey):
        response = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address="+searchText+"&key="+appKey)
        if response.ok:
            return json.loads(response.content)
        else:
            response.raise_for_status()
            return None

class NlpToolkit():

    def processEntityTree(self,t):
        entity_names = []

        if hasattr(t, 'label') and t.label:
            if t.label() == 'NE':
                entity_names.append(' '.join([child[0] for child in t]))
            else:
                for child in t:
                    entity_names.extend(self.processEntityTree(child))

        return entity_names

    def extractEntities(self,text):
        sentences = nltk.sent_tokenize(text)
        tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
        tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
        chunked_sentences = nltk.ne_chunk_sents(tagged_sentences, binary=True)
        entities = []
        for tree in chunked_sentences:
            entities.extend(self.processEntityTree(tree))
        return entities

def testGoogleGeocoding():
    google = GoogleApiToolkit()
    geocode_result = google.processGeocoding("rua doutor luiz palmier","AIzaSyARSfX6sppFwIp7QWJATLYqpTVOdxqnJ6s")
    print(geocode_result)

def testEventProcessing():
    event = {}
    event['source_id'] = 123
    event['event_desc'] = 'tiros na Niteroixx'
    event['lang'] = 'pt'

    eventEnhancer = EventEnhancer()
    event = eventEnhancer.categorizeEvent(event)
    event = eventEnhancer.processGeolocation(event)
    pprint(event)

if __name__ == "__main__":
    #testGoogleGeocoding()
    testEventProcessing()
    #nltk.download('all')