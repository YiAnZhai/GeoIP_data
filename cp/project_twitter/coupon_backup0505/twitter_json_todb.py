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

	def insert_user(self):
		self.connect_to_db()
		exist_user_dict = {userid[0]:True for userid in self.get_user()}
		src_dir = config.coupon_tweet_filter_filestore_path
#		src_dir = "/home/moma/Documents/data_mining/twitter/"
		insert_tweet_sql = "INSERT INTO %s (`text`, user_id, tweet_id, expand_url,url,display_url,user_mention,hashtag,retweet_count, favorite_count, create_time, create_time_format, update_time, update_time_format) values (%s)" % (self.couponinfo_table, "%s, "*13 + "%s")
		insert_user_sql = "INSERT into %s (user_id, name, screen_name, tweet_count, location, description, follower_count, friend_count, list_count, create_time, verified, lang, spider_time) values (%s)" % (self.twitter_user_table, "%s, "*12 + "%s")
		parents = os.listdir(src_dir)
#		yestoday = (dt.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
#		parent_dir_files = [aa for aa in parents if aa.startswith(yestoday) and 0 == aa.count("likely")]
#		parent_dir_files = [aa for aa in parents if aa=="20160724_coupon.txt"]
                parent_dir_files = [aa for aa in parents if aa.endswith("_freshdata")]

		for parent in parent_dir_files:
			fname = os.path.join(src_dir,parent)
                        if 0 < parent.count("likely"):
                            rename_file( fname )
                            continue
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
							list_tweet_data.append([tweet["text"], tweet["user"]["id_str"], tweet["id_str"],"****".join([a["expanded_url"] for a in tweet["entities"]["urls"]]),"****".join([a["url"] for a in tweet["entities"]["urls"]]),"****".join([a["display_url"] for a in tweet["entities"]["urls"]]),"****".join([a["screen_name"] for a in tweet["entities"]["user_mentions"]]),"****".join([a["text"] for a in tweet["entities"]["hashtags"]]), tweet["retweet_count"],tweet["favorite_count"], tweet["created_at"], format_time(tweet["created_at"], "%Y-%m-%d %H:%M:%S"), update_time, update_time_format])

							if not exist_user_dict.has_key(tweet["user"]["id_str"]):
								list_user_data.append([tweet["user"]["id_str"], tweet["user"]["name"], tweet["user"]["screen_name"], tweet["user"]["statuses_count"], tweet["user"]["location"], tweet["user"]["description"], tweet["user"]["followers_count"], tweet["user"]["friends_count"], tweet["user"]["listed_count"], tweet["user"]["created_at"], tweet["user"]["verified"], tweet["user"]["lang"], dt.now().strftime("%Y-%m-%d %H:%M:%S") ])
								
								exist_user_dict[tweet["user"]["id_str"]] = True
							#list_tweet_data.append([tweet["text"], tweet["id_str"], tweet["entities"]["urls"][0],tweet["entities"]["user_mentions"][0],tweet["entities"]["hashtags"][0] ])
							# if len(list_tweet_data) > 10:
							# 	break
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

if "__main__" == __name__:
	t = Tweet()
	t.insert_user()
