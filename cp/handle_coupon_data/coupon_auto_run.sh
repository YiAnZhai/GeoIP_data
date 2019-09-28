export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games

cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
scrapy crawl cj_coupon
scrapy crawl rakutenmarketing
scrapy crawl shareasale_coupon

cd /home/moma/Documents/handle_coupon_data
python update_guidinfresh.py

cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
scrapy crawl getredirecturl -a api="coupon_api"


cd /home/moma/Documents/handle_coupon_data
python update_brand_in_coupon.py

cd /home/moma/Documents/spider/coupon_new/coupon/coupon/spiders
scrapy crawl add_new_advertiser
scrapy crawl add_new_advertiser_ls

cd /home/moma/Documents/handle_coupon_data
python update_brand_in_coupon.py
########## url_status!=0 use landing_page extract domain
python update_brand_from_redurl.py

python join_red_url.py
python get_clean_url.py
python dup_remove.py
python init_today_table.py

python insert_newbrand_to_coupon_brand.py