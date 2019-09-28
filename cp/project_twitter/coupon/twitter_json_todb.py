import MySQLdb as mdb
import re
import os
import json
from datetime import datetime as dt
import datetime
import config
from dateutil import parser

def format_time(time_input, format):
    time = ""
    try:
        time = parser.parse(time_input).strftime(format)
    except Exception, e:
        print "Error time convert: ", e
    return time

def rename_file(file_name):
    os.rename(file_name, "%s" % (file_name[:-10], ))


class Tweet():
    def __init__(self):
        self.DB_HOST = config.DB_HOST
        self.DB_NAME = config.DB_NAME
        self.DB_USER = config.DB_USER
        self.DB_PSWD = config.DB_PSWD
        self.CHARSET = config.CHARSET
        self.twitter_user_table = config.twitter_user_table
        self.couponinfo_table = config.couponinfo_table
        self.likely_couponinfo_table = config.likely_couponinfo_table

    def connect_to_db(self):
        self.tweet_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
        self.tweet_cur = self.tweet_con.cursor()
        self.tweet_cur.execute("SET NAMES utf8")

    def close_db(self):
        self.tweet_cur.close()
        self.tweet_con.close()

    def get_user(self):
        self.tweet_cur.execute("select distinct user_id from %s" % (self.twitter_user_table))
        return self.tweet_cur.fetchall()

    def insert_user( self, likely=False ):
        self.connect_to_db()
        exist_user_dict = {userid[0]:True for userid in self.get_user()}
        src_dir = config.coupon_tweet_filter_filestore_path
#               src_dir = "/home/moma/Documents/data_mining/twitter/"
        tweet_sql_base = """INSERT INTO %s (`text`, full_text, lang, user_id, tweet_id, expand_url,url,display_url,\
            user_mention,media_url, media_id, media_type, possibly_sensitive,hashtag,retweet_count, extend_tweet_url, \
            extend_tweet_expand_url, extend_tweet_display_url, user_url, user_name, user_screen_name, favorite_count, \
            create_time, create_time_format, update_time, update_time_format) \
            values (%s)"""
        # insert_user_sql = "INSERT into %s (user_id, name, screen_name, tweet_count, location, description, follower_count, friend_count, list_count, create_time, verified, lang, spider_time) values (%s)" % (self.twitter_user_table, "%s, "*12 + "%s")
        insert_user_sql = """INSERT into %s (user_id, name, screen_name, tweet_count, location, description, url, \
            protected, profile_background_image_url, profile_image_url, profile_banner_url, follower_count, \
            friend_count, list_count, create_time, verified, lang, spider_time) values (%s)""" % (self.twitter_user_table, "%s, "*17 + "%s")
        parents = os.listdir(src_dir)
      # yestoday = (dt.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
      # parent_dir_files = [aa for aa in parents if aa.startswith(yestoday) and 0 == aa.count("likely")]
        if likely:
            insert_tweet_sql = tweet_sql_base % (self.likely_couponinfo_table, "%s, "*25 + "%s")
            parent_dir_files = [aa for aa in parents if aa.endswith("_freshdata") and 0 < aa.count("likely")]
        else:
            insert_tweet_sql = tweet_sql_base % (self.couponinfo_table, "%s, "*25 + "%s")
            parent_dir_files = [aa for aa in parents if aa.endswith("_freshdata") and 0 == aa.count("likely")]

        for parent in parent_dir_files:
            fname = os.path.join(src_dir,parent)
            # if 0 >= parent.count("likely") and 0 >= parent.count("longer140"):
            # if 0 < parent.count("goon_rule"):
            # if 0 < parent.count("likely"):
            # # if 0 < parent.count("likely") or 0 < parent.count("longer140"):
            #     rename_file( fname )
            #     continue
            print fname
            list_tweet_data = []
            list_user_data = []
            with open(fname, "r") as f:
                for line in f:
                    try:
                        tweet = json.loads(line)
                        # if "healthcare" in tweet['text'].lower():
                        if "en" == tweet["lang"]:
                            update_time = tweet["created_at"]
                            update_time_format = format_time(tweet["created_at"], "%Y-%m-%d %H:%M:%S")
                            if tweet.has_key("retweeted_status"):
                                tweet = tweet["retweeted_status"]
                            ########## extend tweet ##################################
                            extended_tweet = self.get_item_value(tweet, "extended_tweet")
                            extend_tweet_url, extend_tweet_expanded_url, extend_tweet_display_url = ["", "", "", ]
                            if extended_tweet:
                                extend_tweet_url = "****".join([a["url"] for a in extended_tweet["entities"]["urls"]])
                                extend_tweet_expanded_url = "****".join([a["expanded_url"] for a in extended_tweet["entities"]["urls"]])
                                extend_tweet_display_url = "****".join([a["display_url"] for a in extended_tweet["entities"]["urls"]])
                            ###################################################
                            media_entities = self.get_item_value(tweet,"entities","media")
                            media_entity = media_entities[0] if media_entities else {}
                            list_tweet_data.append([tweet["text"], self.get_item_value(tweet,"extended_tweet", "full_text"), 
                                tweet["lang"], tweet["user"]["id_str"], tweet["id_str"],\
                                "****".join([a["expanded_url"] for a in tweet["entities"]["urls"]]),\
                                "****".join([a["url"] for a in tweet["entities"]["urls"]]),\
                                "****".join([a["display_url"] for a in tweet["entities"]["urls"]]),\
                                "****".join([a["screen_name"] for a in tweet["entities"]["user_mentions"]]),\
                                self.get_item_value(media_entity, "media_url"),self.get_item_value(media_entity, "id_str"),\
                                self.get_item_value(media_entity, "type"),self.get_item_value(tweet, "possibly_sensitive"),\
                                "****".join([a["text"] for a in tweet["entities"]["hashtags"]]), tweet["retweet_count"],\
                                extend_tweet_url, extend_tweet_expanded_url, extend_tweet_display_url, self.get_item_value(tweet,"user","url"), \
                                self.get_item_value(tweet,"user","name"), self.get_item_value(tweet,"user","screen_name"), \
                                tweet["favorite_count"], tweet["created_at"], format_time(tweet["created_at"], "%Y-%m-%d %H:%M:%S"), \
                                update_time, update_time_format])

                            if not exist_user_dict.has_key(tweet["user"]["id_str"]):
                                # list_user_data.append([tweet["user"]["id_str"], tweet["user"]["name"], tweet["user"]["screen_name"], tweet["user"]["statuses_count"], tweet["user"]["location"], tweet["user"]["description"], tweet["user"]["followers_count"], tweet["user"]["friends_count"], tweet["user"]["listed_count"], tweet["user"]["created_at"], tweet["user"]["verified"], tweet["user"]["lang"], dt.now().strftime("%Y-%m-%d %H:%M:%S") ])
                                list_user_data.append([self.get_item_value(tweet,"user","id_str"), self.get_item_value(tweet,"user","name"), \
                                    self.get_item_value(tweet,"user","screen_name"), self.get_item_value(tweet,"user","statuses_count"), \
                                    self.get_item_value(tweet,"user","location"), self.get_item_value(tweet,"user","description"), \
                                    self.get_item_value(tweet,"user","url"), self.get_item_value(tweet,"user","protected"), \
                                    self.get_item_value(tweet,"user","profile_background_image_url"), self.get_item_value(tweet,"user","profile_image_url"), \
                                    self.get_item_value(tweet,"user","profile_banner_url"), self.get_item_value(tweet,"user","followers_count"), \
                                    self.get_item_value(tweet,"user","friends_count"), self.get_item_value(tweet,"user","listed_count"), \
                                    self.get_item_value(tweet,"user","created_at"), self.get_item_value(tweet,"user","verified"), \
                                    self.get_item_value(tweet,"user","lang"), dt.now().strftime("%Y-%m-%d %H:%M:%S") ])
                                exist_user_dict[tweet["user"]["id_str"]] = True
                            #list_tweet_data.append([tweet["text"], tweet["id_str"], tweet["entities"]["urls"][0],tweet["entities"]["user_mentions"][0],tweet["entities"]["hashtags"][0] ])
                            # if len(list_tweet_data) > 10:
                            #       break
                    except Exception, e:
                        pass
                        # print e
            rename_file( fname )
            print len(list_tweet_data)
            print "begin insert"
            self.tweet_cur.executemany(insert_tweet_sql, list_tweet_data)
            self.tweet_con.commit()

            self.tweet_cur.executemany(insert_user_sql, list_user_data)
            self.tweet_con.commit()
        self.close_db()

    def get_item_value(self, item_dict, item, item2="", item3=""):
        if item_dict.has_key(item):
            if not item2:
                return item_dict[item]
            else:
                if item_dict[item].has_key(item2):
                    if not item3:
                        return item_dict[item][item2]
                    else:
                        if item_dict[item][item2].has_key(item3):
                            return item_dict[item][item2][item3]
                        else:
                            return ""
                else:
                    return ""
        else:
            return ""


if "__main__" == __name__:
    t = Tweet()
    t.insert_user()
    t.insert_user( likely=True )
