# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from coupon.items import SasCouponItem2
import xmltodict
import json
import MySQLdb as mdb
import datetime

class ShareasaleSpider(scrapy.Spider):
    name = "shareasale_coupon2"
    allowed_domains = ["shareasale.com"]
    start_urls = (
#        'http://www.shareasale.com/',
    )
    custom_settings = {
	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline':100,},
	'MYSQL_HOST':'192.168.8.147',
	'MYSQL_DBNAME':'coupon',
	'MYSQL_USER':'root',
	'MYSQL_PASSWD':'123',
#	'COOKIES_ENABLED':True,
    }

    couponurl = 'http://shareasale.com/dealdatabase2.xml'
    con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'coupon')
    cur = con.cursor()
    sql = 'select id, link from shareasale_coupon'
    cur.execute(sql)
    existed_coupons = cur.fetchall()
#    existed_coupons = [item[0][item[0].rfind('=')+1:] for item in existed_coupons]
#    print len(existed_coupons), existed_coupons[:5], '----------------------------'
    cur.close()
    con.close()    
    today = datetime.datetime.now().strftime('%Y-%m-%d')


    def start_requests(self):
	url = "https://account.shareasale.com/a-login.cfm"
        formdata = {'step2':'True','redirect':'a-main.cfm','username':'cathyca','nuisance':'thanks','gadgetry':'azimuth',\
                    'drafter':'millionth','jq1':'overawe','password':'MOMAcnbeta#1102','glad-hand':'dispirit',\
                    'glad-hand':'dispirit'}
        request = FormRequest(url, callback = self.parse, formdata = formdata)
	yield request

    def parse(self, response):
	for coupon in self.existed_coupons[:]:
		url = coupon[1]
		request = scrapy.Request(url, callback = self.get_landingpage)
		request.meta['id'] = coupon[0]
		yield request	
    
    def get_landingpage(self, response):
	item = SasCouponItem2()
	item['couponid'] = response.meta['id']
	landingpage = response.xpath("//tr[contains(td[1]/text(), 'Landing Page')]/td[2]/text()").extract_first(default = "").strip()
	item['landingpage'] = landingpage
	yield item
#	item_list = response.xpath("//item")
#	for sascoupon in item_list[:1]:
#		item = SasCouponItem()
#		item['merchantid'] = sascoupon.xpath("merchantID/text()").extract_first(default = '').strip()
#		item['merchantname'] = sascoupon.xpath("merchantname/text()").extract_first(default = '').strip()
#		item['title'] = sascoupon.xpath("title/text()").extract_first(default = '').strip()
#		item['description'] = sascoupon.xpath("description/text()").extract_first(default = '').strip()
#		item['dealstartdate'] = sascoupon.xpath("dealstartdate/text()").extract_first(default = '').strip()
#		item['dealenddate'] = sascoupon.xpath("dealenddate/text()").extract_first(default = '').strip()
#		item['dealpublishdate'] = sascoupon.xpath("dealpublishdate/text()").extract_first(default = '').strip()
#		item['couponcode'] = sascoupon.xpath("couponcode/text()").extract_first(default = '').strip()
#		item['keywords'] = sascoupon.xpath("keywords/text()").extract_first(default = '').strip()
#		item['category'] = sascoupon.xpath("category/text()").extract_first(default = '').strip()
#		item['restrictions'] = sascoupon.xpath("restrictions/text()").extract_first(default = '').strip()
#		item['commissionpercentage'] = sascoupon.xpath("commissionpercentage/text()").extract_first(default = '').strip()
#		item['dealtitle'] = sascoupon.xpath("dealtitle/text()").extract_first(default = '').strip()
#		item['imagebig'] = sascoupon.xpath("imagebig/text()").extract_first(default = '').strip()
#		item['imagesmall'] = sascoupon.xpath("imagesmall/text()").extract_first(default = '').strip()
#		item['htmlofdeal'] = sascoupon.xpath("htmlofdeal/text()").extract_first(default = '').strip()
#		item['link'] = sascoupon.xpath("link/text()").extract_first(default = '').strip()
		##yield item

    def close(reason):
	con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'coupon')
        cur = con.cursor()
        sql = 'select count(*) from shareasale_coupon where add_date like %s'
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        cur.execute(sql, (current_date+"%", ))
        res = cur.fetchone()
        if res:
                sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
                cur.execute(sql_insert, ('shareasale_coupon', str(res[0]), datetime.datetime.now()))
                con.commit()
        cur.close()
        con.close()
