# event_processor.py
import json
from database import DataManager
class TwitterProcessor():
    
    def createEvent(self,tweet):
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

if __name__ == "__main__":
    processor = TwitterProcessor()
    tweet = json.loads(r'''{"created_at":"Fri Dec 01 01:40:30 +0000 2017","id":936409642007252991,"id_str":"936409642007252992","text":"RT @DouglasQuerubim: Flamengo come\u00e7a a jogar quando ?????? Vamos querer em crlh!!","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":848028590012866560,"id_str":"848028590012866560","name":"Romenia","screen_name":"MeniaMathias","location":"Queimados, Brasil","url":null,"description":"PDA \u2764 \nEu n\u00e3o estou b\u00eabada, s\u00f3 perdi o controle!!","translator_type":"none","protected":false,"verified":false,"followers_count":723,"friends_count":414,"listed_count":1,"favourites_count":8417,"statuses_count":16448,"created_at":"Sat Apr 01 04:25:46 +0000 2017","utc_offset":null,"time_zone":null,"geo_enabled":true,"lang":"pt","contributors_enabled":false,"is_translator":false,"profile_background_color":"F5F8FA","profile_background_image_url":"","profile_background_image_url_https":"","profile_background_tile":false,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/936195546288738306\/9bClg56a_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/936195546288738306\/9bClg56a_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/848028590012866560\/1512035695","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"retweeted_status":{"created_at":"Fri Dec 01 01:27:37 +0000 2017","id":936406398061219841,"id_str":"936406398061219840","text":"Flamengo come\u00e7a a jogar quando ?????? Vamos querer em crlh!!","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":836653481842835456,"id_str":"836653481842835456","name":"Querubim","screen_name":"DouglasQuerubim","location":"Magalh\u00e3es Bastos","url":"https:\/\/curiouscat.me\/Querubiim","description":"Concurseiro Militar","translator_type":"none","protected":false,"verified":false,"followers_count":798,"friends_count":702,"listed_count":2,"favourites_count":5831,"statuses_count":1610,"created_at":"Tue Feb 28 19:05:09 +0000 2017","utc_offset":null,"time_zone":null,"geo_enabled":true,"lang":"pt","contributors_enabled":false,"is_translator":false,"profile_background_color":"F5F8FA","profile_background_image_url":"","profile_background_image_url_https":"","profile_background_tile":false,"profile_link_color":"1DA1F2","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/924453575434096640\/RG48YY6e_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/924453575434096640\/RG48YY6e_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/836653481842835456\/1505449118","default_profile":true,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":5,"favorite_count":1,"entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"pt"},"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[{"screen_name":"DouglasQuerubim","name":"Querubim","id":836653481842835456,"id_str":"836653481842835456","indices":[3,19]}],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"pt","timestamp_ms":"1512092430961"}''')
    event = processor.createEvent(tweet)
    dataManager = DataManager()
    dataManager.insertJson('events',json.dumps(event))    
    print(str(event))