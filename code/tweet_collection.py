from os import path

import pandas as pd
from twarc import Twarc
from util.util import DataCollector
from util.util import create_dir, Config

keys = pd.read_csv('resources/tweet_keys_file.txt').iloc[0]
t = Twarc(keys['app_key'], keys['app_secret'], keys['oauth_token'], keys['oauth_token_secret'])


features = ['tweet_id', 'retweeted_id', 'created_at', 'favorite_count', 'retweet_count',
                         'user_id', 'location', 'verified', 'followers_count',
                         'source',
                         'text',
                         'fake']

def collect_tweets(news_list, news_source, label, config: Config):
    create_dir(config.dump_location)
    create_dir("{}/{}".format(config.dump_location, news_source))
    create_dir("{}/{}/tweets".format(config.dump_location, news_source))


    for news in news_list:
        print('Downloading ' + news_source + ' ' + label + ' ' + news.news_id + ' tweets')
        create_dir("{}/{}/{}/{}".format(config.dump_location, news_source, label, news.news_id))
        data = pd.DataFrame(columns= features)
        news_dir = "{}/{}/tweets/{}.csv".format(config.dump_location, news_source,
                                                             news.news_id)
        if path.exists(news_dir):
            continue
        else:
            for tweet in t.hydrate(news.tweet_ids):
                data = data.append(extract_tweet_features(tweet, label),
                                                     ignore_index=True)
            data.to_csv(news_dir, index=False)


def extract_tweet_features(tweet, label):
    if label == 'fake':
        fake = 1
    else:
        fake = 0
    return{'tweet_id': tweet['id'],
     'retweeted_id': 0,
     'created_at': tweet['created_at'],
     'favorite_count': tweet['favorite_count'],
     'retweet_count': tweet['retweet_count'],
     'user_id': tweet['user']['id'],
     'location': tweet['user']['location'],
     'verified': tweet['user']['verified'],
     'followers_count': tweet['user']['followers_count'],
     'source': tweet['source'],
     'text': repr(tweet["full_text"]),
     'fake': fake}


class TweetCollector(DataCollector):

    def __init__(self, config):
        super(TweetCollector, self).__init__(config)

    def collect_data(self, choices):
        for choice in choices:
            news_list = self.load_news_file(choice)
            collect_tweets(news_list, choice["news_source"], choice["label"], self.config)
