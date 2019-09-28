#########db info
DB_HOST = "localhost"
DB_NAME = "mycoupon_datacenter"
DB_USER = "root"
# DB_PSWD = "moma1601cool"
DB_PSWD = "moma"
CHARSET = "utf8"

#########tables
##domains
negative_domain_table = "domain_negative"
negative_domain_reviewed_table = "domain_negative_reviewed"
negative_domain_reviewing_table = "domain_negative_reviewing"
negative_domain_revieweveryday_table = "domain_negative_to_revieweveryday"
negative_domain_competitor_revieweveryday_table = "domain_negative_competitor_to_revieweveryday"

needhandled_domain_table = "domain_needhandled"
domain_whitebrand_list = "domain_white_brand"
domain_whitebrand_reviewed_list = "domain_white_brand_reviewed"
domain_whitebrand_needreviewed_list = "domain_white_brand_to_revieweveryday"


red_url_table = "get_red_url"
domain_red_url_table = "get_domain_titledescription"


twitter_user_table = "tweet_user"
couponinfo_table = "twitter_coupon_info"
couponinfohandle_table = "twitter_coupon_info_handle"
couponinfohandle_all_table = "twitter_coupon_info_handle_all"
couponblackpending_table = "twitter_coupon_info_handle_black_pending"
couponblacklist_table = "twitter_coupon_info_handle_blackdestination"
couponneedhandled_list_table = "twitter_coupon_info_handle_handleneeded_domain"
couponneedhandled_list_all_table = "twitter_coupon_info_handle_handleneeded_domain_all"
couponinfohandle_todo_later_table = "twitter_coupon_info_handle_todo_later"


dup_base_table = "coupon_twitter_today_fresh"
dup_baseall_table = "coupon_twitter_today_fresh_all"
dup_basenegwordall_table = "coupon_twitter_today_fresh_allnegword"

dup_after_all_table = "coupon_twitter_chijiao"
dup_remove_table = "coupon_twitter_today_remdup"
dup_remove_backup_table = "coupon_twitter_today_remdup_all"

dup_innertwitter_table = "coupon_twitter_innertwitter"
dup_innertwitterall_table = "coupon_twitter_innertwitter_all"


twitter_etsy_table = "coupon_twitter_z_etsy"
twitter_instagram_table = "coupon_twitter_z_instagram"
twitter_linkis_table = "coupon_twitter_z_linkis"
twitter_constantcontact_table = "coupon_twitter_z_constantcontact"
twitter_campaign_table = "coupon_twitter_z_campaign"

###
dup_innertwitter_test_history_table = "coupon_twitter_innertwitter_test_historydup"


coupon_twitter_table = "coupon_twitter"
coupon_retail_api_table = "coupon_api"

twitter_expand_domain_table = "twitter_domains_expand"
twitter_destination_domain_table = "twitter_domains_destination"


#########fields
to_dup_remove_back_fields = """source, advertiserid, brand, title, description, code, fresh, dup_flag, common_words, useful_words, end_date, start_date, redirect_url, url, landing_page, url_status, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, retweet_count, favorite_count, text, full_text, price, coupon_requirement, media_url, media_type, first_create_date"""

handle_to_black_commonfileds = """lang, guid, user_id, text, full_text, title_info, show_title, show_description, location_intwitter, code, discount, price, freeshipping, coupon_requirement, tweet_id, choosed_url, expand_url, expand_domain, url, display_url, extend_tweet_expand_url, extend_tweet_expand_domain, extend_tweet_url, extend_tweet_display_url, user_url, user_name, user_screen_name, user_mention, hashtag, hashtag_handled, media_url, media_id, media_type, possibly_sensitive, retweet_count, favorite_count, coupon_start_date, coupon_end_date, create_time, create_time_format, update_time, update_time_format, datetime_handle"""
handle_to_all_fields =         """lang, guid, user_id, text, full_text, title_info, show_title, show_description, location_intwitter, code, discount, price, freeshipping, coupon_requirement, tweet_id, choosed_url, expand_url, expand_domain, url, display_url, extend_tweet_expand_url, extend_tweet_expand_domain, extend_tweet_url, extend_tweet_display_url, user_url, user_name, user_screen_name, user_mention, hashtag, hashtag_handled, media_url, media_id, media_type, possibly_sensitive, retweet_count, favorite_count, coupon_start_date, coupon_end_date, create_time, create_time_format, update_time, update_time_format, datetime_handle"""

dupremove_fields = """brand, `code`, title, description, source, url, restriction, linkid, start_date, end_date, adddate, advertiserid, redirect_url, landing_page, url_status, category, keywords, htmlofdeal, promotiontypes, freeshipping, discount, guid, advertisername, retweet_count, favorite_count, text, full_text, price, coupon_requirement, media_url, media_type, first_create_date"""
# dupwithapi_fields = "brand, `code`, title, description, source, url, restriction, linkid, start_date, end_date, adddate, advertiserid, redirect_url, landing_page, url_status, category, keywords, htmlofdeal, promotiontypes, freeshipping, discount, guid, advertisername"

twitter_today_copy_toall =  """source, guid, advertiserid, advertisername, linkid, brand, text, full_text, title, description, code, discount, price, freeshipping, coupon_requirement, start_date, end_date, url, url_status, landing_page, redirect_url, media_url, media_type, retweet_count, favorite_count, category, restriction, keywords, htmlofdeal, promotiontypes, first_create_date, adddate"""
twitter_innercoupon_toall = """source, guid, advertiserid, advertisername, linkid, brand, text, full_text, title, description, code, discount, price, freeshipping, coupon_requirement, start_date, end_date, url, url_status, landing_page, redirect_url, media_url, media_type, retweet_count, favorite_count, category, restriction, keywords, htmlofdeal, promotiontypes, first_create_date, adddate"""


##########suffix that seldom seen
exception_suffix = set(["uk.com"])


##########negative word in tweet text
negative_words = set(["fuck","fucking","shit","ass","bastard","bitch","cock","cunt","dick","damn","nigger",])
########		"associate","association","dickies","assistant","assortment","assembly","assembled",
########		"assure","assurance","assist","Cockpit","dick's","Assorted","asse%","cocktail","assy"])

#########porn word in domain title and description
porn_words = ["porn","anal","pussy","fuck","xxx",
	"ass","tits","porno","creampie", "hentai", "orgasm",
	"fucking", "blowjob",  "cock", "boobs", "gangbang", "handjob",
	"interracial","milf","pornstar","camgirl","deepthroat",
	"creampies","gangbangs","pornstars","handjobs","blowjobs","camgirls","pornos","cocks",
	"sexy,video","sex,video","adult,video","japanese,video","gay,video",
	"sexy,videos","sex,videos","adult,videos","japanese,videos","gay,videos",
	"sexy,movie","sex,movie","adult,movie","japanese,movie","gay,movie",
	"sexy,movies","sex,movies","adult,movies","japanese,movies","gay,movies",
	"sexy,vids","sex,vids","adult,vids","japanese,vids","gay,vids",
	#"naked","lesbian",
	]


###########review black and white domain or not, *****The older data keeped when turn off******
review_whitedomain = False
review_blackdomain = False
review_competitordomain = False
###########black domain everyday
count_reviewblack_domains=10
count_reviewblack_nurls=10
###########white subdomain everyday
count_review_tld=30
count_review_url=10
###########competitor domain everyday
count_review_domain = 1000000
