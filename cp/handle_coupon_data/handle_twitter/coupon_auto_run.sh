export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
### scrapy crawl twitter_search_coupon
### scrapy crawl twitter_search_api
scrapy crawl getredirecturl -a api="twitter" -L WARNING


cd /home/moma/Documents/handle_coupon_data/handle_twitter
python update_brandingetredurl.py

#######handle etsy and instagram
python instagram_crawler.py
python etsy_crawler.py
python crawl_ins_landingpage.py
#######extract etsy and instagram
python update_handleneeded_brand.py


cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
#####the order twitter and etsy_instagram is important
scrapy crawl getredirecturl_fordomain -a src="twitter" -L WARNING
scrapy crawl getredirecturl_fordomain -a src="etsy_instagram" -L WARNING


cd /home/moma/Documents/handle_coupon_data/handle_twitter
# 下面会清理 dup_base
python update_domain_porn.py
### python join_red_url.py
### python get_clean_url.py
python judge_destinationurl_black.py
# 添加新流程 Yifan
python handler_linkis.py
python handler_constantcontact.py
python handler_campagin.py
# 下面会重置 handled
python judge_destinationurl_needhandled.py
python delete_negativeword.py

python dup_remove_innertwitter.py
### python dup_remove_withapi.py
####### python init_today_table.py    ####do it in dup_remove_innertwitter.py
