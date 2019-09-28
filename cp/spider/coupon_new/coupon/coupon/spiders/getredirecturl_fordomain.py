#-*- coding: utf-8 -*-
import scrapy
import MySQLdb as mdb
import os
from coupon.items import RedurlItem_domain
import random
#from simhash import Simhash
from datetime import datetime
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from twisted.internet.error import TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError

class GetredirecturlSpider(scrapy.Spider):
	name = "getredirecturl_fordomain"
#	allowed_domains = ["example.com"]
	start_urls = (
		'http://www.example.com/',
	)
	custom_settings = {
		# "MYSQL_HOST" : '192.168.8.222',
#		"MYSQL_DBNAME" : 'coupon_datacenter',
#		"MYSQL_USER" : 'root',
#		"MYSQL_PASSWD" : 'moma',
		# "DOWNLOAD_DELAY" : 0.5,
		"DOWNLOAD_TIMEOUT" : 60,
		"CONCURRENT_REQUESTS": 80,
		'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,},
	}

	def __init__(self, src="twitter"):
		settings = get_project_settings()
		self.host=settings.get('MYSQL_HOST')
		self.db=settings.get('MYSQL_DBNAME')
		self.user=settings.get('MYSQL_USER')
		self.passwd=settings.get('MYSQL_PASSWD')
		self.charset='utf8'
		self.domain_table = settings.get('RED_URL_TABLE')
		self.freshtwitter_table = settings.get("TWITTER_API_FRESH_TABLE")
		self.domain_store_table = settings.get('RED_DOMAIN_TABLE')
		self.etsy_inst_table = settings.get("ETSY_INSTAGRAM_DOMAINTABLE")

		src_sql_dict = {
			"twitter": """SELECT a.red_url, a.domain_tld, a.suffix from %s as a inner join %s as b on a.guid=b.guid
				LEFT join %s as c on concat(a.domain_tld, ".", a.suffix)=c.domain where c.domain is null and 
				a.domain_tld is not null and a.domain <> '' group by concat(a.domain_tld, ".", a.suffix)""" \
				% ( self.domain_table, self.freshtwitter_table, self.domain_store_table ), 
			# "twitter": """SELECT a.red_url, a.domain_tld, a.suffix from %s as a 
			# 	LEFT join %s as c on concat(a.domain_tld, ".", a.suffix)=c.domain where c.domain is null and 
			# 	a.domain_tld is not null and a.domain <> '' group by concat(a.domain_tld, ".", a.suffix)""" \
			# 	% ( self.domain_table, self.domain_store_table ), 
			"etsy_instagram": """SELECT a.landing_page, a.brand from %s as a left join %s as b on a.brand=b.domain 
				where b.domain is null and a.brand <> '' and a.brand is not null group by a.brand"""
				% (self.etsy_inst_table, self.domain_store_table)
		}
		self.sql = src_sql_dict[src]
		self.src = src

	def close_db(self):
		self.originurl_cur.close()
		self.originurl_con.close()

	def uniform_url(self, domain_main, url):
		protocal, url_af = url.split("//", 1)
		if url_af.startswith("www."):
			domain_main = "www.%s" % (domain_main)
		format_url = "%s//%s" % (protocal, domain_main)
		return format_url

	def start_requests(self):
#		today = datetime.now().strftime('%Y-%m-%d')
#		print today
		self.originurl_con = mdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, charset=self.charset)
		self.originurl_cur = self.originurl_con.cursor()
		self.originurl_cur.execute( "SET NAMES utf8" )

#		print sql
		a = self.originurl_cur.execute( self.sql )
		origin_urls = self.originurl_cur.fetchall()
		self.close_db()
		print
		print
		print "............We still have %d brands to get title_description from originurl.............." % (len(origin_urls))
		print
		print
		for origin_url in origin_urls:
			try:
				if 2 == len(origin_url):
					domain = origin_url[1]
					src = "etsy_instagram"
				else:
					domain = ".".join( [origin_url[1], origin_url[2], ] )
					src = "twitter"
				url = self.uniform_url( domain, origin_url[0].lower() )
				request = scrapy.Request(url, callback=self.parse, errback=self.error_handle, method="GET", dont_filter =True)
				request.meta["origin_url"] = url
				request.meta["src"] = src
				request.meta["guid"] = domain
				request.meta["retry_time"] = 0
				yield request
			except Exception, e:
				print e
				# f = open('log/red_request.txt', 'a')
				# f.write(url+'\n')
				# f.write(str(e)+'\n')
				# f.close()

	def parse(self, response):
		content = scrapy.Selector(text=response.body)
		item = RedurlItem_domain()
		item["guid"] = response.meta["guid"]
		item["origin_url"] = response.meta["origin_url"]
		item["red_url"] = response.url
		item["url_status"] = response.status
		item['title'] = content.css('title::text').extract_first(default='')
		item['description'] = content.xpath('//meta[@name="description"]/@content').extract_first(default='')
		item["error_info"] = ""
		item["src"] = response.meta["src"]
		item["middle_urls"] = ""
		if response.meta.has_key("redirect_urls"):
			item["middle_urls"] = "****".join( response.meta["redirect_urls"][:])
		yield item



	def error_handle(self, failure):
		print 'error -------------------------------------'
		# print failure.value
		# print repr(failure.value)
#		response = failure.value.response
		request = failure.request
		# self.request_time += 1
		if request.meta["retry_time"] < 2:
			request_retry = scrapy.Request(request.url, callback = self.parse, errback = self.error_handle, priority = 100, dont_filter = True)
			for a in request.meta:
				request_retry.meta[a] = request.meta[a]
			# request_retry.meta["proxy"] = "http://%s" % (self.ip_pool[(self.request_time%self.ip_num)])
			request_retry.meta["retry_time"] = request.meta["retry_time"] + 1
			yield request_retry
		else:
			item = RedurlItem_domain()
			item["src"] = request.meta["src"]
			item['origin_url'] = request.meta["origin_url"]
			item['title'] = ""
			item['description'] = ""
			item["guid"] = request.meta["guid"]
			item["middle_urls"] = ""
			if request.meta.has_key("redirect_urls"):
				item["middle_urls"] = "****".join( request.meta["redirect_urls"][:])
			print request.url
			item['url_status'] = 0
			item["red_url"] = ''
			# item['ip'] = request.meta['proxy']
			if failure.check(HttpError):
				response = failure.value.response
				self.logger.error('HttpError on %s, status code %s', response.url, response.status)
				item["red_url"] = response.url
				item['error_info'] = 'HttpError'
				item['url_status'] = response.status
				if response.status == 403:
					self.crawler.stats.inc_value('403error')
					if self.crawler.stats.get_value('403error') > 100:
						# self.break_proxy()
						self.crawler.stats.set_value('403error', 0)
	#			item['url']

			elif failure.check(DNSLookupError):
				self.logger.error('DNSLookupError on %s, dnslookuperror num: %s', request.url, str(self.crawler.stats.get_value('dnslookuperror')))
				item['error_info'] = 'DNSLookupError'
				self.crawler.stats.inc_value('dnslookuperror')
				print 'dns look up error', self.crawler.stats.get_value('dnslookuperror')
				if self.crawler.stats.get_value('dnslookuperror') > 3:
					# self.break_proxy()
					self.crawler.stats.set_value('dnslookuperror', 0)

			elif failure.check(TimeoutError):
				self.logger.error('TimeoutError on %s', request.url)
				item['error_info'] = 'TimeoutError'

			elif failure.check(TCPTimedOutError):
				self.logger.error('TCPTimedOutError on %s', request.url)
				self.crawler.stats.inc_value('tcptimeouterror')
				item['error_info'] = 'TCPTimedOutError'
				if self.crawler.stats.get_value('tcptimeouterror') > 10:
					# self.break_proxy()
					self.crawler.stats.set_value('tcptimeouterror', 0)

			elif failure.check(ConnectionRefusedError):
				self.logger.error('ConnectionRefusedError on %s', request.url)
				self.crawler.stats.inc_value('connectionrefusederror')
				item['error_info'] = 'ConnectionRefusedError'
				if self.crawler.stats.get_value('connectionrefusederror') == 10:
					# self.break_proxy()
					self.crawler.stats.set_value('connectionrefusederror', 0)

			else:
				item['error_info'] = repr(failure.value)
			print failure
			yield item

			# file_log = open("log/red_url.txt", "a")
			# file_log.write("%s\n" % (failure))
			# file_log.close()

	def break_proxy(self):
		vpns = ["51vpn-1", "51vpn-2", "51vpn-3", "51vpn-6", "51vpn-7", "123-s12", "ec2-1"]
		con_time = 10
		con_timed = 0
		while True:
			cmd_break = "nmcli con up id %s" % (random.choice(vpns))
			result = os.system(cmd_break)
			con_timed += 1
			if 0 == result or con_timed >= con_time:
				if 0 == result:
					print time.strftime('%Y-%m-%d %H:%M:%S'),  " Connect to ", rand_vpn
					print
				break
