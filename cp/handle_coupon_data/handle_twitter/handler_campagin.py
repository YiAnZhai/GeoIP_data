#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 处理 Linkis.com
# yifan 2016年12月28日14:38:21
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import requests
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import config
import tldextract
from judge_destinationurl_black import Redurl
import common_functions
from scrapy.selector import Selector
from hashlib import md5


class Campagin_domain(Redurl):

	headers = {
	'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2638.0 Safari/537.36',
	"Accept" : "text/html",
	"Cookie":"__utmt=1; __utma=206883212.174790556.1465977299.1465977299.1465977299.1; __utmb=206883212.1.10.1465977299; __utmc=206883212; __utmz=206883212.1465977299.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __gads=ID=6563fee18e9f7b7c:T=1465977300:S=ALNI_MY_pNRXAI9Qry9b7kD6pCUVsAClVw; blogads_book_excerpt_put_ttd_tracking_tags=1",
	}

	ignore_domains_andsub = ["facebook.com", "etsy.com", "bit.ly", "youtube.com", "twitter.com", "m.facebook.com", "have2have.it", "like2have.it", "m.youtube.com", "lyft.com", "uber.com", "twitter.com", "pinterest.com", "instagram.com", "google.com", "youtu.be", "your.website.address.here", "files.ctctcdn.com", "eepurl.com", "mailchimp.com", "g.co", "google.ca", "linkedin.com","forward-to-friend.com","constantcontact.com","rs6.net","cortas.me","campaign-archive1.com", "campaign-archive2.com", "tumblr.com", "yelp.com", "site-ym.com", "t.cn", "amzn.to"]

	def __init__(self):
		super(Campagin_domain, self).__init__()
		self.twitter_campaign_table = config.twitter_campaign_table

	def fetchPage(self,url_tuple):
		guid = url_tuple[1]
		url = url_tuple[0]
		'''爬虫'''
		print "current url "+ url
		if not url.startswith("http"):
			url = "http://"+url

		try:
			response = requests.get(url,headers=Campagin_domain.headers,timeout=10)
			body = self.parse_body(response)
			resp = Selector(text=body)

			# 从页面中提取的 a 标签集合
			a_list = resp.xpath('//a/@href').extract()
			# a_list 中提取到的 根域名
			extract_list = []
			for row in a_list:
				# 过滤邮箱、list-manage类型网站
				if '@' in row or "list-manage" in row:
					continue
				subdomain, domain_full, domain, suffix = self.extract_domain( row )
				if domain and suffix:
					extract_list.append(domain+'.'+suffix);

			now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
			# 待插入数据集合
			infolist = []
			if extract_list:
				# 待插入的数据
				infos = []
				# 两个基本字段 urlguid
				infos.append(str(url));
				infos.append(str(guid));
				# 页面 domain 字典
				a_dic = {i:extract_list.count(i) for i in extract_list}

				# 过滤domain
				for neg_domain in self.ignore_domains_andsub:
					if neg_domain in a_dic:
						del a_dic[neg_domain]
				if a_dic:
					newbrand = [i for i in a_dic if a_dic[i] == max(a_dic.values())][0]
					# ext1 存新 brand
					infos.append(str(newbrand))
					# 再次过滤黑名单
					is_black = self.judge_black( '.'+newbrand )
					# ext2 存新 brand 的黑名单情况
					infos.append(str(is_black))
					# ext3 存新 是否通过了规则
					if is_black == 'False False':
						infos.append('ok')
					else:
						infos.append('black')
					print
					print infos
					print
					# 170314 - 替换 url 为出现最多的 url
					for a_row in a_list:
						if newbrand in a_row and '@' not in a_row and 'mailto' not in a_row:
							a_subdomain, a_domain, a_suffix = tldextract.extract(a_row)
							isblack = self.judge_black('.'+a_domain+'.'+a_suffix)
							if isblack == 'False False':
								infos.append(a_row)
								break
					if not infos[5]:
						infos[5] = newbrand

					infos.append(now_time)
					infolist.append(infos)

		except requests.exceptions.ConnectionError, e:
			print e
		except Exception, e:
			print e

		# 插入数据库 coupon_twitter_z_constantcontact
		self.connect_db()
		sql_insert_table = """INSERT into %s (origin_url, guid, ext1, ext2, ext3, ext4, `time`) values (%s)""" \
		% (self.twitter_campaign_table, ", ".join(["%s"]*7))
		self.coupon_cur.executemany(sql_insert_table, infolist)
		self.coupon_con.commit()
		self.close_db()

	def parse_body(self,response):
		'''对response body解码'''
		if response.encoding == 'ISO-8859-1':
			encodings = requests.utils.get_encodings_from_content(response.content)
			if encodings:
				response.encoding = encodings[0]
			else:
				response.encoding = response.apparent_encoding
		body = response.content
		if len(body)>0:
			body = response.content.decode(response.encoding, 'replace').strip().encode('utf-8', 'replace')
		else:
			body = ""
		return body

	# 从页面中抓取 a 标签 进行分析、保存
	def handle_brand(self):
		self.connect_db()
		sql_select_domains = """SELECT a.black_url, a.guid from %s as a left join %s as b \
		on a.guid=b.guid where (a.black_reason='campaign-archive1.com' or a.black_reason='campaign-archive2.com') and b.guid is null GROUP BY a.guid
		""" % (self.couponneedhandled_list_table, self.twitter_campaign_table)
		print sql_select_domains
		self.coupon_cur.execute( sql_select_domains )
		data = self.coupon_cur.fetchall()
		now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
		infolist = []
		for row in data:
			self.fetchPage(row)

	# 将数据导入 dup_base_table
	def handle_insert_dupbase(self):

		self.connect_db()
		sql_select_url = """SELECT "atwitter",a.guid, a.user_id, a.clean_text, a.text,
			a.code, a.discount, a.freeshipping, a.tweet_id, b.ext4, c.url_status, a.expand_url, a.retweet_count,
			a.favorite_count, a.create_time_format, a.update_time_format, b.ext1, b.ext1
			from %s as a INNER JOIN %s as b on a.guid = b.guid and b.ext3 = 'ok'
			LEFT JOIN %s c on a.guid = c.guid GROUP BY a.guid """  \
		% (self.couponneedhandled_list_table, self.twitter_campaign_table, self.red_url_table)
		self.coupon_cur.execute(sql_select_url)
		results = self.coupon_cur.fetchall()
		inst_coupons = []
		for tweet in results:
			inst_coupons.append( list(tweet[:17]) )

		sql_insert_to_duptable = """INSERT into %s (source, guid, advertiserid,title,
				description, code, discount, freeshipping, linkid, landing_page, url_status, url,
				retweet_count, favorite_count, adddate, start_date, brand) values (%s)""" \
		% ( self.dup_base_table, ", ".join(["%s"]*17) )
		self.coupon_cur.executemany(sql_insert_to_duptable, inst_coupons)
		self.coupon_con.commit()
		print " to dupbase"
		self.close_db()

if "__main__" == __name__:
	Campagin = Campagin_domain()
	Campagin.get_blacklist()
	Campagin.get_whitetld_list()
	Campagin.get_reviewed_blackdomain()

	# 处理 brand、判断 blacklist
	Campagin.handle_brand()
	# 数据插入 dup_base_table
	Campagin.handle_insert_dupbase()
