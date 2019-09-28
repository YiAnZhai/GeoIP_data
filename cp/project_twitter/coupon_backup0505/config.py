#datebase
#DB_HOST = "192.168.8.222"
DB_HOST = "localhost"
DB_NAME = "coupon_datacenter"
DB_USER = "root"
DB_PSWD = "moma1601cool"
CHARSET = "utf8"

#table
negative_domain_table = "negative_domain"
couponinfo_table = "twitter_coupon_info"
couponinfohandle_table = "twitter_coupon_info_handle"
twitter_user_table = "tweet_user"
dup_base_table = "coupon_twitter_today_copy"


#field
couponinfo_tohandled_common_fields = "text, user_id, tweet_id, expand_url, url, display_url, user_mention, hashtag, retweet_count, favorite_count, create_time, create_time_format, update_time, update_time_format"


#file path to store
tweet_common_coupon_filestore_path = "/disk1/data/twitter/data_twitter_coupon_commonkeywords/"
coupon_tweet_filter_filestore_path = "/disk1/data/twitter/data_twitter_couponselect/"
