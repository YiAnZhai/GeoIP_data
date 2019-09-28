# -*- coding: utf-8 -*-
import scrapy
import json
import time
from coupon.items import CouponItem
import MySQLdb as mdb
import datetime
import re
from coupon import settings

class RakutenmarketingSpider(scrapy.Spider):
# 	handle_httpstatus_list = [403, 404, 301, 302]
# 	name = "rakutenmarketing"
# #	allowed_domains = ["rakutenmarketing.com"]
# 	start_urls = (
# 		'http://www.rakutenmarketing.com/',
# 	)

# 	custom_settings = {
# 		'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,
# 				'coupon.pipelines.DataProcessingPipeline':50},
# #		"MYSQL_HOST" : '192.168.8.222',
# #		"MYSQL_DBNAME" : 'coupon_datacenter',
# #		"MYSQL_USER" : 'root',
# #		"MYSQL_PASSWD" : 'moma',
# 		# "DOWNLOAD_DELAY" : 0.5,
# 		"CONCURRENT_REQUESTS": 1,
# 		"REDIRECT_ENABLED":False,
# 	}

# 	con = mdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, passwd=settings.MYSQL_PASSWD, db=settings.MYSQL_DBNAME, charset='utf8')
# 	cur = con.cursor()
# 	sql = 'select linkid from linkid where source = "linkshare"'
# 	cur.execute(sql)
# 	existed_coupons = cur.fetchall()
# 	existed_coupons = [item[0] for item in existed_coupons]
# #	print len(existed_coupons), existed_coupons[:5], '----------------------------'
# 	cur.close()
# 	con.close()
# 	today = datetime.datetime.now().strftime('%Y-%m-%d')	

	def start_requests(self):
		url = "https://api.rakutenmarketing.com/token"
		header = {"Authorization":"Basic X3BVNlJXRXJXdTE5VHByaVhiOXRGR3haWEgwYTpoWmZ5MTV3SGZienZvNkp0TFJhUldOeUZkR1lh"}
		para_data = {
			"grant_type":"password",
			"username":"OliviaM",
			"password":"aff@RLS414",
			"scope": "3290723"
		}
		request = scrapy.http.FormRequest(url=url, formdata=para_data, callback=self.get_api_token, headers=header, dont_filter=True, method="POST")
		yield request


	def get_api_token(self,response):
		data = json.loads(response.body)
#		print data
		api_token = "%s %s" % ("Bearer", data["access_token"])
		refresh_token = data["refresh_token"]
		url = "https://api.rakutenmarketing.com/token"
		header = {"Authorization":"Basic X3BVNlJXRXJXdTE5VHByaVhiOXRGR3haWEgwYTpoWmZ5MTV3SGZienZvNkp0TFJhUldOeUZkR1lh"}
		para_data = {
			"grant_type":"refresh_token",
			"refresh_token":refresh_token,
			"scope": "PRODUCTION"
		}
		request = scrapy.http.FormRequest(url=url, formdata=para_data, callback=self.refresh_token, headers=header, dont_filter=True, method="POST")
		request.meta["total_pages"] = -1
		yield request

	def refresh_token(self,response):
		data = json.loads(response.body)
#		print data
		api_token = "%s %s" % ("Bearer", data["access_token"])
		refresh_token = data["refresh_token"]
		header = {
			"Authorization": api_token,
			"User-Agent":  "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:42.0) Gecko/20100101 Firefox/42.0",
			# "Host": "api.rakutenmarketing.com",
			# "Accept": "application/json, text/javascript, */*; q=0.01",
			# "Accept-Language": "en-US,en;q=0.5",
			# "Accept-Encoding": "gzip, deflate",
			# "Referer": "https://developers.rakutenmarketing.com/subscribe/apis/info?name=Coupon&version=1.0&provider=LinkShare&",
			# "Origin": "https://developers.rakutenmarketing.com",
			# "Connection": "keep-alive"
		}
#		print header
		total_pages = response.meta["total_pages"]
		if -1 == total_pages:
			url = "https://api.rakutenmarketing.com/coupon/1.0?resultsperpage=0"
			request = scrapy.Request(url=url, headers=header,callback=self.get_all_coupons_num)
		else:
			page = int(response.meta["the_page"])
			url = "https://api.rakutenmarketing.com/coupon/1.0?resultsperpage=500&pagenumber=%s" % (page + 1)
			request = scrapy.Request(url=url, headers=header,callback=self.parse_api_data)
			request.meta["the_page"] = page + 1

		request.meta["refresh_token"] = refresh_token
		request.meta["total_pages"] = total_pages
		yield request

	def get_all_coupons_num(self, response):
		fault = response.css("fault")
		if fault:
			errorcode = response.css("fault errorcode::text").extract_first(default="")
			errorstring = response.css("fault errorstring::text").extract_first(default="")
			print
			print
			print "errorrrrrrrrrrrrrrrr"
			print errorcode, ":  ", errorstring
			print
			print
		# print response.body
		total_num = 0
		try:
			total_num = int(response.css("couponfeed TotalMatches::text").extract_first(default=0))
		except Exception, e:
			print "tatol_num error: ", e
		print "get_all_coupons_num: ", total_num
		pages = total_num / 500 + 1

		refresh_token = response.meta["refresh_token"]
		url = "https://api.rakutenmarketing.com/token"
		header = {"Authorization":"Basic X3BVNlJXRXJXdTE5VHByaVhiOXRGR3haWEgwYTpoWmZ5MTV3SGZienZvNkp0TFJhUldOeUZkR1lh"}
		para_data = {
			"grant_type":"refresh_token",
			"refresh_token":refresh_token,
			"scope": "PRODUCTION"
		}
		request = scrapy.http.FormRequest(url=url, formdata=para_data, callback=self.refresh_token, headers=header, dont_filter=True, method="POST")
		request.meta["total_pages"] = pages
		request.meta["the_page"] = 0
		yield request

		# for page in range(pages):
			
		# 	url = "https://api.rakutenmarketing.com/coupon/1.0?resultsperpage=500&pagenumber=%s" % (page + 1)
		# 	# time.sleep(120)
		# 	request = scrapy.Request(url=url, headers=header,callback=self.parse_api_data)
		# 	yield request

	def parse_api_data(self, response):
		fault = response.css("fault")
		if fault:
			errorcode = response.css("fault errorcode::text").extract_first(default="")
			errorstring = response.css("fault errorstring::text").extract_first(default="")
			print
			print
			print "errorrrrrrrrrrrrrrrr"
			print errorcode, ":  ", errorstring
			print
			print
		total_num = response.css("couponfeed TotalMatches::text").extract_first(default="")
		total_pages = response.css("couponfeed TotalPages::text").extract_first(default="")
		the_page = response.css("couponfeed PageNumberRequested::text").extract_first(default="")
		print "total coupons: ", total_num
		print "total pages: ", total_pages
		if str(response.meta["total_pages"]) != str(total_pages):
			print "total num mismatch"
		print "the page: ", the_page
		if str(response.meta["the_page"]) != str(the_page):
			print "the_page mismatch"
		print 
		print

		coupons = response.css("link")
		for coupon in coupons:
			clickurl = coupon.css("clickurl::text").extract_first(default="")
			if clickurl:
				linkid = re.findall("offerid=(.*?)&type", clickurl)[0]
				print clickurl, 'yyyyyyyyyyyyy'
				if str(linkid) in self.existed_coupons:
			#		print 'existed in db ----------'
					continue
				else:
					print 'not existed xxx', 'aaaaaaaaaaa'
			else:
				linkid = ''
			end_date = coupon.css("offerenddate::text").extract_first(default="")
			if end_date < self.today:
				print 'end before today'
				continue
			else:
				print "end after today"
			start_date = coupon.css("offerstartdate::text").extract_first(default="")
			if start_date and end_date:
				if end_date < start_date:
					continue

			item = CouponItem()
			item['source'] = 'linkshare'
			item["advertiserid"] = coupon.css("advertiserid::text").extract_first(default="")
			item["advertisername"] =  coupon.css("advertisername::text").extract_first(default="")
			item['title'] = "" 
			item["description"] = coupon.css("offerdescription::text").extract_first(default="")
			item["code"] = coupon.css("couponcode::text").extract_first(default="")
			item["end_date"] = end_date 
			item["start_date"] = start_date 

			item["url"] = clickurl 
	
			item["category"] = "; ".join(coupon.css("categories category::text").extract())
			item["restriction"] = coupon.css("couponrestriction::text").extract_first(default="")
			item["keywords"] = "" 
			item["htmlofdeal"] = "" 
			item["promotiontypes"] = "; ".join(coupon.css("promotiontypes promotiontype::text").extract())
			item['linkid'] = str(linkid)
			item['freeshipping'] = '0'
			item['affiliate_status'] = ''
			print item
			yield item
#			request = scrapy.Request(url = clickurl, callback = self.get_finalpage)
#			request.meta['linkid'] = linkid
#			request.meta["advertiserid"] = coupon.css("advertiserid::text").extract_first(default="")
#			request.meta["advertisername"] = coupon.css("advertisername::text").extract_first(default="")
#			request.meta['title'] = ""
#			request.meta["description"] = coupon.css("offerdescription::text").extract_first(default="")
#			request.meta["code"] = coupon.css("couponcode::text").extract_first(default="")
#			request.meta["end_date"] = end_date 
#			request.meta["start_date"] = coupon.css("offerstartdate::text").extract_first(default="")
#			request.meta["category"] = "; ".join(coupon.css("categories category::text").extract())
#			request.meta["restriction"] = coupon.css("couponrestriction::text").extract_first(default="")
#			request.meta["keywords"] = ""
#			request.meta["htmlofdeal"] = ""
#			request.meta["promotiontypes"] = "; ".join(coupon.css("promotiontypes promotiontype::text").extract())
#			request.meta["url"] = clickurl
#			yield request
#			print request, '------------------222222'
	
#			item = CouponItem()
#			item['source'] = 'linkshare'
#			item["advertiserid"] = coupon.css("advertiserid::text").extract_first(default="")
#			item["advertisername"] = coupon.css("advertisername::text").extract_first(default="")
#			item['title'] = ''	
#			item["description"] = coupon.css("offerdescription::text").extract_first(default="")
#			item["code"] = coupon.css("couponcode::text").extract_first(default="")
#			item["end_date"] = coupon.css("offerenddate::text").extract_first(default="")
#			item["start_date"] = coupon.css("offerstartdate::text").extract_first(default="")
#			item["url"] = coupon.css("clickurl::text").extract_first(default="")
#			item["category"] = "; ".join(coupon.css("categories category::text").extract()) #			item["restriction"] = coupon.css("couponrestriction::text").extract_first(default="")
#			item['keywords'] = ''
#			item['htmlofdeal'] = ''
#			item["promotiontypes"] = "; ".join(coupon.css("promotiontypes promotiontype::text").extract())
#			yield item

		if int(total_pages) > int(the_page):
			refresh_token = response.meta["refresh_token"]
			url = "https://api.rakutenmarketing.com/token"
			header = {"Authorization":"Basic X3BVNlJXRXJXdTE5VHByaVhiOXRGR3haWEgwYTpoWmZ5MTV3SGZienZvNkp0TFJhUldOeUZkR1lh"}
			para_data = {
				"grant_type":"refresh_token",
				"refresh_token":refresh_token,
				"scope": "PRODUCTION"
			}
			request = scrapy.http.FormRequest(url=url, formdata=para_data, callback=self.refresh_token, headers=header, dont_filter=True, method="POST")
			request.meta["total_pages"] = total_pages
			request.meta["the_page"] = the_page
			yield request
	
	def get_finalpage(self, response):
		print 'yesssssssssssssssssssss'
		item = CouponItem()
		item['source'] = 'linkshare'
		item["advertiserid"] = response.meta['advertiserid']
		item["advertisername"] = response.meta['advertisername']
		item['title'] = response.meta['title']
		item["description"] = response.meta['description']
		item["code"] = response.meta['code']
		item["end_date"] = response.meta['end_date']
		item["start_date"] = response.meta['start_date']

		item["url"] = response.headers['Location']
	
		item["category"] = response.meta['category']
		item["restriction"] = response.meta['restriction']
		item["keywords"] = response.meta['keywords']
		item["htmlofdeal"] = response.meta['htmlofdeal']
		item["promotiontypes"] = response.meta['promotiontypes']
		item['linkid'] = str(response.meta['linkid'])
		print item
		yield item
