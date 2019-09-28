#-*- coding: utf-8 -*-
import scrapy
import MySQLdb as mdb
import os
from coupon.items import RedurlItem
import random
import re
import urlparse
#from simhash import Simhash
from datetime import datetime
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError
from twisted.internet.error import TCPTimedOutError
from twisted.internet.error import ConnectionRefusedError

class GetredirecturlSpider(scrapy.Spider):
	name = "getredirecturl"
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

	def __init__(self, api="coupon_api"):
		settings = get_project_settings()
		self.host=settings.get('MYSQL_HOST')
		self.db=settings.get('MYSQL_DBNAME')
		self.user=settings.get('MYSQL_USER')
		self.passwd=settings.get('MYSQL_PASSWD')
		self.charset='utf8'
		self.freshcoupon_table = settings.get("COUPON_API_FRESH_TABLE")
		self.freshtwitter_table = settings.get("TWITTER_API_FRESH_TABLE")
		self.red_url_table = settings.get("RED_URL_TABLE")

		print self.host, self.freshcoupon_table, self.freshtwitter_table
		source = api
		sources = {
			"coupon_api": ["coupon_api", "url", self.freshcoupon_table],
			"twitter":["twitter_api", "choosed_url", self.freshtwitter_table],
		}
		self.source_table = sources[source]

	def start_requests(self):
#		today = datetime.now().strftime('%Y-%m-%d')
#		print today
		self.originurl_con = mdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, charset=self.charset)
		self.originurl_cur = self.originurl_con.cursor()
		self.originurl_cur.execute("SET NAMES utf8")
		# sql = """select a.id, a.url from (SELECT id, url from coupon_api_today where url <> '' and url is not null) as a left join get_red_url as b on b.guid = MD5(a.url) where b.guid is null group by a.url"""
		sql = """select "%s", SUBSTRING_INDEX(a.%s, "****", 1), a.guid from 
				(SELECT id, %s, guid from %s where %s <> '' and %s is not null and %s not like "%s") as a 
				left join %s as b 
			on b.guid = a.guid where b.guid is null group by a.guid""" % \
			( self.source_table[0], self.source_table[1], \
			  self.source_table[1], self.source_table[2], self.source_table[1], self.source_table[1], self.source_table[1], "%****%", \
			  self.red_url_table
			)
		print sql

		# print sql
		a = self.originurl_cur.execute(sql)
		origin_urls = self.originurl_cur.fetchall()
		print
		print
		print "............We still have %d urls to get landing url from getredurls.............." % (len(origin_urls))
		print
		print

		self.originurl_cur.close()
		self.originurl_con.close()

		for origin_url in origin_urls:
			try:
				url = self.uniform_url(origin_url[1])
				request = scrapy.Request(url, callback=self.parse, errback=self.error_handle, method="GET", dont_filter =True)
				request.meta["origin_url"] = origin_url[1]
				request.meta["src"] = origin_url[0]
				request.meta["guid"] = origin_url[2]
				request.meta["retry_time"] = 0
				yield request
			except Exception, e:
				print e
				# f = open('log/red_request.txt', 'a')
				# f.write(url+'\n')
				# f.write(str(e)+'\n')
				# f.close()

	def parse(self, response):
		item = RedurlItem()
		item["guid"] = response.meta["guid"]
		item["origin_url"] = response.meta["origin_url"]

		script_list = response.xpath('//script').extract()
		pattern = re.compile(r"window\.location\.replace\(([^)]+)\)")
		if script_list:
			 for script in script_list:
				 m = pattern.search(script)
				 if m:
					 final_url =  m.group(1)[1:-1]
					 if final_url:
						 item["origin_red_url"] = final_url.replace('\/','/')
				 else:
					 item["origin_red_url"] = response.url

		else:
			item["origin_red_url"] = response.url

		item["red_url"] = self.del_args( item["origin_red_url"] )


		item["url_status"] = response.status
		item["error_info"] = ""
		item["src"] = response.meta["src"]
		item["middle_urls"] = ""
		if response.meta.has_key("redirect_urls"):
			item["middle_urls"] = "****".join( response.meta["redirect_urls"][:])
		yield item

	def del_args(self, url):
		# print url
		url_frag = urlparse.urlsplit(url)
		scheme, netloc, path, query, fragment = url_frag
		# print query
		query_list = [q.split("=",1) for q in query.split("&") if q]
		cleaned_query_list = []
		for q in query_list:
			continue_flag = False
			if len(q) < 2:
				continue
			k,v = q
			k_lower = k.lower()
			v_lower = v.lower()
			if k_lower in ("_branch_match_id","br_t", "smile_referral_code", "ul_noapp",
				"aff","ref","rfsn","source", "hss_channel","sscid",  "raneaid", "ranmid", "ransiteid", "cm_mmc","fbclid","mbsy",#"ranEAID", "ranMID", "ranSiteID",
				"cjevent","awc",
				"nats","sid", "affid","mid","clickid","siteid","share_id","admitad_uid","campaignid","partner","affiliate_id", #"clickId","SID", 
				):
				continue
			for start_k in ["utm_", "aff_", "aff-", "extole_", "mbsy_", "affiliate", "partner", "refer", "utm-", ]:
				if k_lower.startswith(start_k):
					continue_flag = True;break
			for neg_v in ("twitter", "tweet", "track", "affili", "social" ,"refer", "aff", "ref_", "ref-","facebook",):
				if neg_v in v_lower or neg_v in k_lower:
					continue_flag = True;break
			if v.startswith("tw-"):
				continue
			if continue_flag:
				continue
			cleaned_query_list.append( "%s=%s" % (k,v) )
		cleaned_query = "&".join( cleaned_query_list )
		return urlparse.urlunsplit( (scheme, netloc, path, cleaned_query, fragment) )

	def uniform_url(self, url):
		if url.startswith("http"):
			result_url = url
		else:
			if "http" in url:
				result_url = url[url.find("http"):]
			else:
				result_url = "%s%s" % ("http://", url.strip(".:/"))
		return result_url

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
			item = RedurlItem()
			item["src"] = request.meta["src"]
			item['origin_url'] = request.meta["origin_url"]
			item["guid"] = request.meta["guid"]
			item["middle_urls"] = ""
			if request.meta.has_key("redirect_urls"):
				item["middle_urls"] = "****".join( request.meta["redirect_urls"][:])
			print request.url
			item['url_status'] = 0
			item["origin_red_url"] = ''
			# item['ip'] = request.meta['proxy']
			if failure.check(HttpError):
				response = failure.value.response
				self.logger.error('HttpError on %s, status code %s', response.url, response.status)
				item["origin_red_url"] = response.url
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
			item["red_url"] = self.del_args( item["origin_red_url"] )
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
