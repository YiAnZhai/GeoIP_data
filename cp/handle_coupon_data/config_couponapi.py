#####db info
DB_HOST = "localhost"
DB_NAME = "coupon_datacenter"
DB_USER = "root"
DB_PSWD = "moma1601cool"
CHARSET = "utf8"


#####tables
coupon_apifresh_table = "coupon_api_today"
coupon_apifresh_all_table = "coupon_api_today_all"

api_base_origin_table = "coupon_api"
api_fresh_copy_table = "coupon_api_today_copy"
api_fresh_copy_all_table = "coupon_api_today_copy_all"
api_dup_remove_table = "coupon_api_chijiao"
api_fresh_dup_remove_table = "coupon_api_today_remdup"

coupon_domain_brand_table = "coupon_brand"
coupon_ad_domain_brand_table = "cj_sas_linkshare_brand"

api_redirect_url_table = "get_red_url"


#####fields
api_dup_remove_fields = "brand, `code`, title, description, source, url, restriction, linkid, start_date, end_date, adddate, advertiserid, redirect_url, landing_page, url_status, category, keywords, htmlofdeal, promotiontypes, freeshipping, discount, guid, advertisername"
api_fresh_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, url"
api_fresh_copy_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, url, redirect_url, landing_page, url_status"
api_fresh_to_copy_common_fileds = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, id, advertisername, linkid"
