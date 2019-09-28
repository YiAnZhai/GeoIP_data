# -*- coding: utf-8 -*-
import scrapy
import urllib2
import hmac
import hashlib
import base64
import time
import MySQLdb as mdb
import json
from coupon.items import CouponTTItem
import re
from coupon import settings


class TwitterSearchCouponSpider(scrapy.Spider):
	name = "twitter_search_coupon"
	allowed_domains = ["twitter.com"]
	start_urls = (
		'http://www.api.twitter.com/',
	)
	custom_settings = {
		'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,
				'coupon.pipelines.DataProcessingPipeline':50},
#		"MYSQL_HOST" : '192.168.8.233',
#		"MYSQL_DBNAME" : 'coupon_datacenter',
#		"MYSQL_USER" : 'root',
#		"MYSQL_PASSWD" : 'moma',
		# "DOWNLOAD_DELAY" : 0.5,
		"CONCURRENT_REQUESTS": 1,
		"DOWNLOAD_DELAY": 60
	}


	def __init__(self):

		con = mdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, passwd=settings.MYSQL_PASSWD, db=settings.MYSQL_DBNAME, charset='utf8')
		cur = con.cursor()
		sql = 'select linkid from linkid where source = "atwitter"'
		cur.execute(sql)
		self.existed_coupons = cur.fetchall()
		self.existed_coupons = [item[0] for item in self.existed_coupons]
		print len(self.existed_coupons), self.existed_coupons[:5], '----------------------------'
		cur.close()
		con.close()

		self.url_search_base = 'https://api.twitter.com/1.1/search/tweets.json'
		self.header_auth = {
			"Authorization": """OAuth oauth_consumer_key="iOhpOCAOXraYQQMm3IfKnekIE",oauth_signature_method="HMAC-SHA1",oauth_timestamp="1452911297",oauth_nonce="1239090925",oauth_version="1.0",oauth_token="4765861099-f9kOly9fNSZgEjyNb7zHSj7r7QR7l0xTaPl9QHB",oauth_signature="huiEc98yLapa3JhcMZ82P6gHEqU%3D" """	
		}
		self.para_dic = {
			"oauth_consumer_key":"iOhpOCAOXraYQQMm3IfKnekIE",
			"oauth_nonce":"1239090925",
			"oauth_signature_method":"HMAC-SHA1",
			"oauth_timestamp":"1452911297",
			"oauth_token":"4765861099-f9kOly9fNSZgEjyNb7zHSj7r7QR7l0xTaPl9QHB",
			"oauth_version":"1.0",
			"count":"100",
			"lang":"en",
			"f":"tweets",
			"q":"addidas code",
			"result_type":"recent",
		}

	def start_requests(self):
		print
		print
		print
		print

		# brands = ["addidas", "target",  "1800"]
		# while True:
		for i in range(500):
			query_code = "#coupon"
			para_dic = self.para_dic
			para_dic["oauth_timestamp"] = str(time.time())[:10]
			para_dic["q"] = query_code

			query_code = urllib2.quote(query_code)
			url_code = "%s?count=100&lang=en&f=tweets&q=%s&result_type=recent" % (self.url_search_base, query_code)

			signature_base_string = self.get_signature_base_string("GET", self.url_search_base, para_dic)
			oauth_signature = self.get_hmac_sha1(signature_base_string)
			del(para_dic["q"])
			request_code = scrapy.Request(url_code, callback=self.parse_code, headers=self.get_header(para_dic, oauth_signature), dont_filter=True)
			# request_code.meta["brand"] = brand
			request_code.meta["coupon_type"] = "coupon"
			yield request_code
			print url_code

			query_discount = "#couponcode"
			para_dic["oauth_timestamp"] = str(time.time())[:10]
			para_dic["q"] = query_discount
			query_discount = urllib2.quote(query_discount)
			url_discount = "%s?count=100&lang=en&f=tweets&q=%s&result_type=recent" % (self.url_search_base, query_discount)
			signature_base_string = self.get_signature_base_string("GET", self.url_search_base, para_dic)
			oauth_signature = self.get_hmac_sha1(signature_base_string)
			del(para_dic["q"])
			request_discount = scrapy.Request(url_discount, callback=self.parse_code, headers=self.get_header(para_dic, oauth_signature), dont_filter=True)
			request_discount.meta["coupon_type"] = "couponcode"
			# request_discount.meta["brand"] = brand
			yield request_discount
			print url_discount

	def parse_code(self, response):
		print response.url
		data = json.loads(response.body)
		statuses = self.get_coupon_value(data, "statuses")
		print len(statuses)
		for statuse in statuses:
			# for a in statuse:
			# 	print a, ": ", statuse[a], type(statuse[a])
			print
			print
			print
			if self.get_coupon_value(statuse, "id_str").strip() in self.existed_coupons:
			#	print 'existed in db ----------'
				continue
			else:
				print 'not existed xxx', 'aaaaaaaaaaa'
			if 1 == len(self.get_coupon_value(statuse, "entities", "urls")):
				text = self.get_coupon_value(statuse, "text").strip()
				if "en" == self.get_coupon_value(statuse, "lang"):
					item = CouponTTItem()
					item["source"]  = "atwitter"
					item["linkid"] = self.get_coupon_value(statuse, "id_str").strip()
					item["url"] = ";".join([self.get_coupon_value(url, "expanded_url").strip() for url in self.get_coupon_value(statuse, "entities", "urls")])
					item["title"] = text

					item["advertiserid"] = "" 
					item["advertisername"] = ""
					item["description"] = ""
					item["code"] = ""
					item["end_date"] = ""
					item["start_date"] = ""
				
					item["category"] = ""
					item["restriction"] = ""
					item["keywords"] = ""
					item["htmlofdeal"] = ""
					item["promotiontypes"] = ""	

					yield item



	def is_english(self, text):
		try:
			for i in text:#.decode("utf8"):
				flag = i < u"\u0080" or i >= u"\u2000" and i < u"\u27C0"
				if not flag:
					return 0
			return 1
		except Exception, e:
			print "decode errorrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
			print text
			print e
			return 11


	def get_signature_base_string(self, method, base_url, para_dict, sep="&"):
		para_list = ["%s=%s" % (urllib2.quote(para), urllib2.quote(para_dict[para])) for para in sorted(para_dict.keys())]
		para_str = "&".join(para_list)
		result = "%s%s%s%s%s" % (method, sep, urllib2.quote(base_url), sep, urllib2.quote(para_str))
		result = result.replace("/", "%2F")
		return result

	def get_hmac_sha1(self, data):
		Token = "jMQtDIg3ez1DiYWwaBOzw0Y6emsMcy0sADYYqR1GhsswzlntJs&9jXwkwfo5PNKa2K5pOihOiA4pQdFwnIFvNaGYulhozwxh"
		return urllib2.quote(hmac.new(Token,data,hashlib.sha1).digest().encode('base64').rstrip())

	def get_header(self, header_para, oauth_signature):
		Authorization = ",".join([r'%s="%s"'  % (para, header_para[para]) for para in header_para])
		Authorization = 'OAuth %s,oauth_signature="%s"' % (Authorization, oauth_signature)
		headers = {}
		headers["Authorization"] = Authorization
		return headers

	def get_coupon_value(self, coupon, item, item2="", item3=""):
		if coupon.has_key(item):
			if not item2:
				return coupon[item]
			else:
				if coupon[item].has_key(item2):
					if not item3:
						return coupon[item][item2]
					else:
						if coupon[item][item2].has_key(item3):
							return coupon[item][item2][item3]
						else:
							return ""
				else:
					return ""
		else:
			return ""
