#!/usr/bin/python
# -*- coding: UTF-8 -*-
# 处理 Linkis.com
# yifan 2016年12月22日13:51:34 创建

import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config
import tldextract
from judge_destinationurl_black import Redurl
import common_functions

class Linkis_domain(Redurl):

	def __init__(self):
		super(Linkis_domain, self).__init__()
		self.twitter_linkis_table = config.twitter_linkis_table

	# handle linkis url,extract brand
	def handle_linkis_brand(self):
		self.connect_db()
		sql_select_domains = """SELECT a.black_url, a.guid from %s as a left join %s as b \
		on a.guid=b.guid where a.black_reason='linkis.com' and b.guid is null GROUP BY a.guid
		""" % (self.couponneedhandled_list_table, self.twitter_linkis_table)
		print sql_select_domains
		self.coupon_cur.execute( sql_select_domains )
		data = self.coupon_cur.fetchall()
		now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

		infolist = []
		for row in data:
			# 待插入的数据
			infos = []
			url_lower = row[0].lower()
			pattern = re.compile(r'http:\/\/linkis.com\/([a-z0-9.-]+)\/')
			match = pattern.match(url_lower)
			# print match
			if match:
				subdomain, domain_full, domain, suffix = self.extract_domain( match.group(1) )
				# 中间是否域名
				if domain and suffix:
					# 两个基本字段 url guid
					infos.append(str(row[0]));
					infos.append(str(row[1]));
					# ext1 存新 brand
					newbrand = domain+'.'+suffix
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
					infos.append(now_time)
					infolist.append(infos)

		sql_insert_linkis_table = """INSERT into %s (origin_url, guid, ext1, ext2, ext3, `time`) values (%s)""" \
		% (self.twitter_linkis_table, ", ".join(["%s"]*6))
		print '''  '''
		print sql_insert_linkis_table
		print
		self.coupon_cur.executemany(sql_insert_linkis_table, infolist)
		self.coupon_con.commit()
		self.close_db()

	# 将数据导入 dup_base_table
	def handle_insert_dupbase(self):
		ignore_domains_andsub = ["facebook.com", "etsy.com", "bit.ly", "youtube.com", "twitter.com", "m.facebook.com", "have2have.it", "like2have.it", "m.youtube.com", "lyft.com", "uber.com", "twitter.com", "pinterest.com", "instagram.com", "google.com", "youtu.be", "your.website.address.here", "files.ctctcdn.com", "eepurl.com", "mailchimp.com", "g.co", "google.ca", "linkedin.com","forward-to-friend.com","constantcontact.com","rs6.net","cortas.me","campaign-archive1.com","campaign-archive2.com", "tumblr.com", "yelp.com", "site-ym.com", "t.cn", "amzn.to"]
		ignore_domains_andsub_lower = [a.lower() for a in ignore_domains_andsub]

		self.connect_db()
		sql_select_url = """SELECT "atwitter",a.guid, a.user_id, a.clean_text, a.text,
			a.code, a.discount, a.freeshipping, a.tweet_id, linkis.ext1, c.url_status, a.expand_url, a.retweet_count,
			a.favorite_count, a.create_time_format, a.update_time_format, linkis.ext1, linkis.ext1
			from %s as a inner join %s as linkis on a.guid = linkis.guid and linkis.ext3 = 'ok'
			INNER JOIN %s c on a.guid = c.guid GROUP BY a.guid """ \
		% (self.couponneedhandled_list_table, self.twitter_linkis_table, self.red_url_table)
		self.coupon_cur.execute(sql_select_url)
		results = self.coupon_cur.fetchall()
		inst_coupons = []
		for tweet in results:
			inst_coupons.append( list(tweet[:17]) )

		sql_insert_to_duptable = """INSERT into %s (source, guid, advertiserid,title,
				description, code, discount, freeshipping, linkid, landing_page, url_status, url,
				retweet_count, favorite_count, adddate, start_date, brand) values (%s)""" \
		% ( self.dup_base_table, ", ".join(["%s"]*17) )
		print self.coupon_cur.executemany(sql_insert_to_duptable, inst_coupons)
		self.coupon_con.commit()
		print "linkis to dupbase"
		self.close_db()

if "__main__" == __name__:
	liskis = Linkis_domain()
	liskis.get_blacklist()
	liskis.get_whitetld_list()
	liskis.get_reviewed_blackdomain()

	# 处理 brand、判断 blacklist
	liskis.handle_linkis_brand()
	# 数据插入 dup_base_table
	liskis.handle_insert_dupbase()
