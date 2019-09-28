# -*- coding: utf-8 -*-
import scrapy
import MySQLdb as mdb
from coupon.items import LinkshareItem

class LinksynergySpider(scrapy.Spider):
    name = "linksynergy"
    allowed_domains = ["linksynergy.com"]
    start_urls = (
        'http://www.linksynergy.com/',
    )
    handle_httpstatus_list = [301, 302]
    custom_settings = {
        'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline':100,},
                        #'coupon.pipelines.DataProcessingPipeline':50,},
        'MYSQL_HOST':'192.168.8.240',
        'MYSQL_DBNAME':'haipeng',
        'MYSQL_USER':'root',
        'MYSQL_PASSWD':'moma',
	'REDIRECT_ENABLED':False,
    }

    def start_requests(self):
	con = mdb.connect(host = '192.168.8.240', user = 'root', passwd = 'moma', db = 'haipeng')	
	cur = con.cursor()
	sql = 'select url, id from coupon_api_copy where source = "linkshare" and url like "%click.linksyn%"'
	cur.execute(sql)
	res = cur.fetchall()
	for item in res[:]:
		print item
		request = scrapy.Request(url = item[0], callback = self.parse)
		request.meta['couponid'] = item[1]
		yield request
	cur.close()
	con.close()

    def parse(self, response):
	item = LinkshareItem()
	item['couponid'] = response.meta['couponid']
	item['destination'] = response.headers['Location']
	yield item
