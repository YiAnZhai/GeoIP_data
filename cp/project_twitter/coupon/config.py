#datebase
#DB_HOST = "192.168.8.222"
DB_HOST = "localhost"
DB_NAME = "mycoupon_datacenter"
DB_USER = "root"
DB_PSWD = "moma"
CHARSET = "utf8mb4"

#table
negative_domain_table = "domain_negative"
couponinfo_table = "twitter_coupon_info"
couponinfohandle_table = "twitter_coupon_info_handle"
likely_couponinfo_table = "twitter_coupon_info_likely"
likely_couponinfohandle_table = "twitter_coupon_info_likely_handle"
twitter_user_table = "tweet_user"
dup_base_table = "coupon_twitter_fresh"


# couponinfo_table = "twitter_coupon_info_test"
# couponinfohandle_table = "twitter_coupon_info_test_handle"
# couponinfo_table = "twitter_coupon_info_test_checked"
# couponinfohandle_table = "twitter_coupon_info_test_check_handle"

# couponinfo_table = "twitter_coupon_info_han"
# couponinfohandle_table = "twitter_coupon_info_handle"
# couponinfo_table = "twitter_coupon_info"
# couponinfohandle_table = "twitter_coupon_info_handle_0101"
# couponinfo_table = "twitter_coupon_info_handle_0101"
# couponinfohandle_table = "twitter_coupon_info_handle_0101_22"


#field
couponinfo_tohandled_common_fields = """lang, text, full_text, user_id, tweet_id, expand_url, url, display_url, \
    extend_tweet_expand_url, extend_tweet_url, extend_tweet_display_url, user_url, user_name, user_screen_name, \
    user_mention, hashtag, retweet_count, media_url, media_id, media_type, possibly_sensitive, \
    favorite_count, create_time, create_time_format, update_time, update_time_format, datetime_handle"""


#file path to store
tweet_common_coupon_filestore_path = "/disk1/data/twitter/data_twitter_coupon_commonkeywords/"
coupon_tweet_filter_filestore_path = "/disk1/data/twitter/data_twitter_couponselect/"


config_process_num = 4
config_num_each = 1000




import signal
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
