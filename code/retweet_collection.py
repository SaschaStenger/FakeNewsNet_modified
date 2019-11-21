import json
import logging
from twython import TwythonError, TwythonRateLimitError
from util.TwythonConnector import TwythonConnector
from util.util import create_dir, Config, multiprocess_data_collection

from util.util import DataCollector
from os import path

import pandas as pd


class NewsItem:

    def __init__(self, tweet_data, dir):
        self.tweet_data = tweet_data
        self.dir = dir


def dump_retweets_job(news: NewsItem, config: Config, twython_connector: TwythonConnector):
    data = news.tweet_data
    dir = news.dir
    for tweet, count in zip(data.tweet_id, data.retweet_count):
        if count != 0:
            try:
                connection = twython_connector.get_twython_connection("get_retweet")
                retweets = connection.get_retweets(id=tweet, count=100, cursor=-1)
            except TwythonRateLimitError:
                logging.exception("Twython API rate limit exception - tweet id : {}".format(tweet))
            except Exception:
                logging.exception(
                    "Exception in getting retweets for tweet id %d using connection %s" % (tweet, connection))
            for retweet in retweets:
                data = data.append(extract_retweet_features(retweet, tweet, data['fake'][0]),
                                   ignore_index=True)
    print('Saving ' + dir)
    data.to_csv(dir, index=False)




def collect_retweets(news_list, news_source, label, config: Config):
    create_dir("{}/{}/raw".format(config.dump_location, news_source))
    news_list_to_process = []
    empty_data_objects = 0
    for news in news_list:
        news_dir = "{}/{}/{}/tweets/{}.csv".format(config.dump_location, news_source, label,
                                                             news.news_id)
        data = pd.read_csv(news_dir)
        raw_dir = "{}/{}/complete/{}.csv".format(config.dump_location,news_source,
                                                       news.news_id)
        if data.empty:
            empty_data_objects += 1
            continue
        if path.exists(raw_dir):
            continue
        else:
            news_list_to_process.append(NewsItem(data, raw_dir))
    print('Collecting for ' + str(len(news_list_to_process)) + ' news storie retweets.')
    print(str(empty_data_objects) + '/' + str(len(news_list)) + ' datasets were skipped, as they were empty. ')
    multiprocess_data_collection(dump_retweets_job, news_list_to_process,(config, config.twython_connector), config)


def extract_retweet_features(tweet, retweeted_id, label):
    return {'tweet_id': tweet['id'],
            'retweeted_id': retweeted_id,
            'created_at': tweet['created_at'],
            'favorite_count': tweet['favorite_count'],
            'retweet_count': tweet['retweet_count'],
            'user_id': tweet['user']['id'],
            'location': tweet['user']['location'],
            'verified': tweet['user']['verified'],
            'followers_count': tweet['user']['followers_count'],
            'source': tweet['source'],
            'text': repr(tweet["text"]),
            'fake': label}




class RetweetCollector(DataCollector):

    def __init__(self, config):
        super(RetweetCollector, self).__init__(config)

    def collect_data(self, choices):
        for choice in choices:
            news_list = self.load_news_file(choice)
            collect_retweets(news_list, choice["news_source"], choice["label"], self.config)
