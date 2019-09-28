# -*- coding: utf-8 -*-
import scrapy
import MySQLdb as mdb
from coupon.items import URLItem

class LinksynergySpider(scrapy.Spider):
    name = "getdirecturl"
#    allowed_domains = ["linksynergy.com"]
    start_urls = (
#        'http://www.linksynergy.com/',
    )
    handle_httpstatus_list = [400, 401, 402, 403, 404, 500, 501, 502, 503, 504]
    custom_settings = {
#        'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline':100,},
                        #'coupon.pipelines.DataProcessingPipeline':50,},
        'MYSQL_HOST':'192.168.8.147',
        'MYSQL_DBNAME':'scrapy',
        'MYSQL_USER':'root',
        'MYSQL_PASSWD':'123',
	'RETRY_ENABLED':False,
#	'REDIRECT_ENABLED':False,
    }

    def start_requests(self):
	url = "http://go.v1tracking.com/aff_c?offer_id=23&aff_id=1022"	
	request = scrapy.Request(url, callback = self.parse, dont_filter = True)
#	request.meta['couponid'] = item[1]
	yield request
#	con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'scrapy')	
#	cur = con.cursor()
#	sql = 'select url, id from coupon_api_today_copy where final_url is null'
#	cur.execute(sql)
#	res = cur.fetchall()
#	for item in res[:10]:
#		print item
#		request = scrapy.Request(url = item[0], callback = self.parse, dont_filter = True)
#		request.meta['couponid'] = item[1]
#		yield request
#	cur.close()
#	con.close()

    def parse(self, response):
	item = URLItem()
#	item['couponid'] = response.meta['couponid']
	url = response.url
	item['urlstatus'] = response.status
	first_question = url.find('?')
        if first_question > -1:
		tmp_str = url[first_question+1:]
		if 'utm_' in tmp_str or 'shareasale' in tmp_str:
			url = url[:first_question]
	item['destination'] = url
	yield item
