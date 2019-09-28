# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config
import tldextract
from collections import Counter
import __main__

class Redurl(object):
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET
		self.red_url_table = config.red_url_table
		self.negative_domain_table = config.negative_domain_table
		self.needhandled_domain_table = config.needhandled_domain_table
		self.couponinfohandle_table = config.couponinfohandle_table
		self.couponinfohandle_todo_later_table = config.couponinfohandle_todo_later_table
		self.couponinfohandle_all_table = config.couponinfohandle_all_table
		self.couponblacklist_table = config.couponblacklist_table
		self.couponneedhandled_list_table = config.couponneedhandled_list_table
		self.dup_base_table = config.dup_base_table
		self.handle_to_black_commonfileds = config.handle_to_black_commonfileds
		self.handle_to_all_fields = config.handle_to_all_fields
		self.twitter_expand_domain_table = config.twitter_expand_domain_table
		self.twitter_destination_domain_table = config.twitter_destination_domain_table
		self.domain_whitebrand_reviewed_list = config.domain_whitebrand_reviewed_list
		self.domain_whitebrand_list = config.domain_whitebrand_list
		self.domain_whitebrand_needreviewed_list = config.domain_whitebrand_needreviewed_list
		self.negative_domain_reviewed_table = config.negative_domain_reviewed_table
		self.negative_domain_reviewing_table = config.negative_domain_reviewing_table
		self.couponblackpending_table = config.couponblackpending_table
		self.negative_domain_revieweveryday_table = config.negative_domain_revieweveryday_table
		self.negative_domain_competitor_revieweveryday_table = config.negative_domain_competitor_revieweveryday_table
		self.domain_red_url_table = config.domain_red_url_table

		self.review_whitedomain = config.review_whitedomain
		self.review_blackdomain = config.review_blackdomain
		self.review_competitordomain = config.review_competitordomain

		self.exception_suffix = config.exception_suffix
		self.file = __main__.__file__
		print "**************************"+self.file+"****************************"
		print "************************************beginning***********************************"
		print
		"""
			self.black_domains,							#black domain in domain_negative table
			self.needhandled_domains,					#domain from needhandled_domain_table
			self.reviewed_negative_blackdomains,		#reviewed, not black the domain
			self.reviewed_blackdomains,					#all has reviewed, but don't know black or not
			self.reviewing_blackdomains					#domains is on reviewing, don't know black or not
		"""

	def __del__(self):
		print """*************************************ending*************************************"""
		# print "*************************"+self.file+"*****************************"
		print
		print
		print

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()


	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	#False,False:not black;  (1,):black;  (2,):need_handled;  (4,):sensitive
	#3: gen in logical code
	#
	def judge_black(self, url):
		url_lower = url.lower()

		subdomain, domain_full, domain, suffix = self.extract_domain( url_lower )

		if domain in ( "blogspot", ):
			pre_url = url_lower.split("blogspot.", 1)[0]
			url_lower = pre_url + "blogspot.com"
			return ( 1, domain )
		for needhandled_domain in self.needhandled_domains:
			for needhandled_pattern in self.needhandled_domains[needhandled_domain]:
				if needhandled_pattern in url_lower:
					return ( 2, needhandled_domain )

		for black_domain in self.black_domains:
			for black_pattern in self.black_domains[black_domain]:
				if black_pattern in url_lower:
					return ( 1, black_domain )

		for white_domain in self.reviewed_negative_blackdomains:
			for white_pattern in self.reviewed_negative_blackdomains[white_domain]:
				if white_pattern in url_lower:
					return "False False"

		if "@" in subdomain or "@" in domain:
			return ( 1, "@" )
		if suffix in ("edu", "gov"):
			return ( 1, "no_retail" )
		neg_domain_words = ["coupon", "promo", "news", "blog", "voucher"]
		for neg in neg_domain_words:
			if neg in domain:
				return ( 1, neg )
		if subdomain in ("app",):
			return (1,subdomain)

		sensitive_words = ["deal", "sale", "saving", "promo", "code", "save", "offer", "discount"]
		for s_word in sensitive_words:
			if s_word in domain:
				return ( 4, s_word )


		return "False False"


	def get_blacklist(self):
		self.connect_db()
		sql_select_black = "SELECT domain from %s" % (self.negative_domain_table, )
		self.coupon_cur.execute(sql_select_black)
		self.black_domains = {domain[0].lower():["."+domain[0].lower(), "/"+domain[0].lower()] \
			for domain in self.coupon_cur.fetchall()}
		####special blogspot
		for d in self.black_domains:
			if "blogspot." in d:
				self.black_domains[d] = ["."+d.split("blogspot.", 1)[0]+"blogspot.com", "/"+d.split("blogspot.", 1)[0]+"blogspot.com"]

		sql_select_needhandled = "SELECT domain from %s" % (self.needhandled_domain_table, )
		self.coupon_cur.execute(sql_select_needhandled)
		self.needhandled_domains = {domain[0].lower():["."+domain[0].lower(), "/"+domain[0].lower()] \
			for domain in self.coupon_cur.fetchall()}
		self.close_db()


	#return sub_domain, full_domain, domain, suffix; jishi: domain_full = subdomain + "." + domain
	def extract_domain(self, url):
		subdomain, domain, suffix = tldextract.extract(url)
		if "%s.%s" % (domain, suffix) in self.exception_suffix and subdomain:
			suffix = "%s.%s" % (domain, suffix)
			subdomain, domain, suffix_wrong = tldextract.extract(subdomain + ".com")

		if not subdomain:
			domain_full = domain
		else:
			while  "." in subdomain and subdomain.startswith("ww"):
				subdomain = subdomain.split(".", 1)[-1]
			domain_full = subdomain + "." + domain
			if domain_full.startswith("ww"):
				domain_full = domain_full.split(".", 1)[-1]
		return ( subdomain, domain_full, domain, suffix )


	def get_brand( self, subdomain, domain_full, domain, suffix, url ):
		if "blogspot" != domain:
			suffix_fix = suffix
		else:
			suffix_fix = "com"

		# if "photo" == subdomain or "photos" == subdomain or subdomain.startswith("photo.") or subdomain.startswith("photos.") or self.white_tld_domains.has_key(domain):
#		if "store" == subdomain or "shop" == subdomain or "photo" == subdomain or "photos" == subdomain or \
#			subdomain.startswith("store.") or subdomain.startswith("shop.") or subdomain.startswith("photo.") \
#			or subdomain.startswith("photos.") or self.white_tld_domains.has_key(domain):
		# if self.white_tld_domains.has_key(domain) or self.white_tld_domains.has_key(domain_full) or self.white_tld_domains.has_key(domain_complete):
		domain_complete = '.'.join( [domain_full, suffix] )
		if self.white_tld_domains.has_key(domain) or self.white_tld_domains.has_key(domain_complete):
			brand = "%s.%s" % ( domain_full, suffix_fix )
		else:
			if domain and suffix:
				brand = "%s.%s" % ( domain, suffix_fix )
			else:
				brand = ""

		# if "etsy" == domain and "/shop/" in url:
		# 	sub = url.split("/shop/")[-1].split("/")[0].split("?")[0]
		# 	if sub:
		# 		brand = sub + ".etsy." + suffix_fix
		# 		brand = brand.lower()

		return brand


	#### update redurl black or not
	def handle_redurl(self, counter_reviewblack=config.count_reviewblack_domains, 
		counter_domainurls=config.count_reviewblack_nurls, counter_competitor=config.count_review_domain):

		self.connect_db()
		sql_select_url = """SELECT a.id, a.red_url, a.guid from %s as a inner join %s as b 
			on a.guid=b.guid""" \
			% (self.red_url_table, self.couponinfohandle_table)

		###########review all table
		## sql_select_url = """SELECT id, red_url,guid from %s where is_black=3 or is_black=-1""" \
		## 	% (self.red_url_table, )

		self.coupon_cur.execute( sql_select_url )
		id_urls = self.coupon_cur.fetchall()

		domains_counter = Counter()
		for id_url in id_urls:
			##############tld extarct
			subdomain, domain_full, domain, suffix = self.extract_domain( id_url[1].lower() )
			if domain_full and suffix:
				domain_whole = ".".join( [domain_full, suffix] )
				if domain_whole not in self.reviewed_blackdomains and domain_whole not in self.reviewing_blackdomains:
					if "False False" == self.judge_black( id_url[1].lower() ):
						domains_counter.update( [domain_whole, ] )

		##for get_red_url table update black or not
		id_update_fields = []
		##for insert to top brand review table
		domain_urls_needreview = {}
		####{
		####domain:[
		####			[url1, guid1],
		####            [url2, guid2],
		####	   ]
		####}
		competitor_pending = Counter()
		top_domains = {a[0]:a[1] for a in domains_counter.most_common( counter_reviewblack )}
		for id_url2 in id_urls:
			id_update_fields.append([])
			subdomain, domain_full, domain, suffix = self.extract_domain(id_url2[1].lower())
			##############black or not
			is_black = self.judge_black( id_url2[1].lower() )
			if "False False" == is_black:
				if domain_full and suffix:
					domain_whole = ".".join([domain_full, suffix])
					if domain_whole in self.reviewing_blackdomains:
						id_update_fields[-1] += [3, domain_whole]
							
					#############top pending review or not###################
					elif self.review_blackdomain and domain_whole in top_domains:
						id_update_fields[-1] += [3, domain_whole]

						if domain_urls_needreview.has_key( domain_whole ):
							if counter_domainurls > len( domain_urls_needreview[domain_whole] ):
								domain_urls_needreview[domain_whole] += [ [top_domains[domain_whole], id_url2[1], id_url2[2]], ]
						else:
							domain_urls_needreview[domain_whole] = [ [top_domains[domain_whole], id_url2[1], id_url2[2]], ]

			else:
				id_update_fields[-1] += [is_black[0], is_black[1]]
				if 4 == is_black[0]:
					domain_whole = ".".join([domain_full, suffix])
					id_update_fields[-1][-1] = domain_whole
					if domain_whole not in self.reviewing_blackdomains:
						#############competitor review or not###################
						if self.review_competitordomain:
							############review, competitor not in reviewing list, add to review everyday
							competitor_pending.update( [domain_whole, ] )
						else:
							############if not review any more, competitor not in reviewing list, black it
							id_update_fields[-1][-2] = 1


			if 0 == len(id_update_fields[-1]):
				id_update_fields[-1] += [ 0, "" ]

			id_update_fields[-1] += [ id_url2[0], ]

		#############top pending review or not###################
		if self.review_blackdomain:
			time1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			#############  make list for insert to reviewtable everyday
			domain_urls_count = []
			for domain in top_domains:
				if domain_urls_needreview.has_key( domain ):
					for url_count in domain_urls_needreview[domain]:
						domain_urls_count += [[domain, url_count[0], url_count[1], url_count[2], time1], ]


			############   insert to reviewtable everyday
			sql_insert_to_revieweveryday = """INSERT INTO %s (`domain`, url_count, url, guid, add_date)
				values (%s)""" % (self.negative_domain_revieweveryday_table, ",".join(["%s"]*5))
			self.coupon_cur.executemany( sql_insert_to_revieweveryday, domain_urls_count )
			self.coupon_con.commit()
			print "domain_urls insert to reviewtable everyday for reviewing"


			time2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			############  top domains insert black_reviewing table everyday
			sql_insertto_negative_domain_reviewing = """INSERT INTO %s (domain, appear_time, `type`) values (%s)""" \
				% ( self.negative_domain_reviewing_table, ",".join(["%s"]*3) )
			self.coupon_cur.executemany( sql_insertto_negative_domain_reviewing, [[top_domain, time2, "top_perday"] for top_domain in top_domains] )
			self.coupon_con.commit()
			print "top domains insert to black_reviewing table everyday"


		#############competitor review or not###################
		if self.review_competitordomain:
			comp_domains = [[a[0], a[1], ] for a in competitor_pending.most_common( counter_competitor )]

			time3 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			############insert to negative domain competitor everyday
			sql_insertto_neg_comp_everyday = """INSERT INTO %s (`domain`, `url_count`, `add_date`) values (%s)""" \
				% ( self.negative_domain_competitor_revieweveryday_table,  ",".join(["%s"]*3) )
			self.coupon_cur.executemany( sql_insertto_neg_comp_everyday, [[comp_domain[0], comp_domain[1], time3, ] for comp_domain in comp_domains] )
			self.coupon_con.commit()

			time4 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
			############competitor insert to reviewing negative domain
			sql_comp_insertto_negative_domain_reviewing = """INSERT INTO %s (domain, appear_time, `type`) values (%s)""" \
				% ( self.negative_domain_reviewing_table, ",".join(["%s"]*3) )
			self.coupon_cur.executemany( sql_comp_insertto_negative_domain_reviewing, [[comp_domain[0], time4, "competitor"] for comp_domain in comp_domains] )
			self.coupon_con.commit()


		#############  judge black or not, red url
		# 0:fine; 1:black; 2:need handled further; 3:reviewing; 4:sensetive, if not review == black
		print "update get_redurl table black or not........"
		sql_update = """UPDATE %s set is_black=%s, black_reason=%s where id=%s""" \
			% ( self.red_url_table, "%s", "%s", "%s" )
		self.coupon_cur.executemany( sql_update, id_update_fields )
		self.coupon_con.commit()


		self.close_db()
		print """get_redurl table update success"""
		print


	def white_brand_forreview(self, limit_tld=config.count_review_tld, count_url=config.count_review_url):
		self.connect_db()

		################ get all suburls
		sql_select_subdomainsall = """SELECT a.domain_tld, concat(a.domain,".",a.suffix), a.red_url, a.guid from %s as a 
			LEFT JOIN %s as b on a.domain_tld=b.domain where b.id is null and a.domain<>a.domain_tld and a.domain_tld 
			<> '' and a.domain_tld is not null order by a.id desc""" \
			% (self.red_url_table, self.domain_whitebrand_reviewed_list)


		################get suburls we needed
		self.coupon_cur.execute( sql_select_subdomainsall )
		subdomains = self.coupon_cur.fetchall()


		################ get top tlds
		tld_counts_count = Counter()
		for sub_domain_a in subdomains:
			is_black = self.judge_black( sub_domain_a[2].lower() )
			if tuple == type(is_black) and is_black[0] == 1:
				continue
			tld_counts_count.update([sub_domain_a[0], ])
		tld_counts = tld_counts_count.most_common( limit_tld )

		####{
		####tld:{
		####	subdomain:[
		####			  		[url1, guid1],
		####              		[url2, guid2],
		####			  ]
		####	}
		####}
		top_tlds_needreviewed = {tld_count[0]:{} for tld_count in tld_counts}
		for sub_domain in subdomains:
			if sub_domain[0] in top_tlds_needreviewed:

				# print sub_domain
				if sub_domain[1] in top_tlds_needreviewed[sub_domain[0]]:
					if len(top_tlds_needreviewed[sub_domain[0]][sub_domain[1]]) < count_url:
						top_tlds_needreviewed[sub_domain[0]][sub_domain[1]].append( [sub_domain[2], sub_domain[3]] )
				else:
					top_tlds_needreviewed[sub_domain[0]][sub_domain[1]] = [ [sub_domain[2], sub_domain[3]], ]

		time1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		subdomain_needreviewed = []
		for tld in tld_counts:
			for sub in top_tlds_needreviewed[tld[0]]:
				for url in top_tlds_needreviewed[tld[0]][sub]:
					subdomain_needreviewed.append([tld[0], sub, tld[1], time1] + url)


		################insert to needreview table everyday
		sql_insert_needreviewtable = """INSERT INTO %s (domain, subdomain, url_count, add_date, url, guid) values (%s)""" \
			% ( self.domain_whitebrand_needreviewed_list, ", ".join(["%s"]*6) )
		self.coupon_cur.executemany( sql_insert_needreviewtable, subdomain_needreviewed )
		self.coupon_con.commit()
		print "subdomain insert to needreview table everyday"

		################insert to reviewing table
		time2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		sql_insert_to_reviewedtable = """INSERT INTO %s (`domain`, `appear_time`) values (%s)""" \
			% (self.domain_whitebrand_reviewed_list, ", ".join(["%s"]*2) )
		self.coupon_cur.executemany( sql_insert_to_reviewedtable, [[tld[0], time2] for tld in tld_counts] )
		self.coupon_con.commit()
		print "subdomain insert to reviewing table"
		print
		self.close_db()


	def auto_increment(self, table_name):
		sql_change_autoincrement = "ALTER TABLE %s AUTO_INCREMENT = 1" % (table_name, )
		re_auto = self.coupon_cur.execute( sql_change_autoincrement )
		self.coupon_con.commit()
		return re_auto


	def insertto_destination_black(self):
		self.connect_db()
		if False:#statistic domain, not ness
			###########   insert to domain list
			sql_insert_to_expanddomain = """INSERT into %s (domain, create_time) SELECT expand_domain, update_time_format from \
				(select  expand_domain, update_time_format from (SELECT expand_domain, update_time_format from %s where \
				expand_domain<>"" and expand_domain is not null ORDER BY update_time_format ) aa group by expand_domain \
				ORDER BY update_time_format) as bb LEFT JOIN %s as cc on bb.expand_domain=cc.domain where cc.domain is null \
			""" % (self.twitter_expand_domain_table, self.couponinfohandle_table, self.twitter_expand_domain_table)
			self.coupon_cur.execute(sql_insert_to_expanddomain)
			self.coupon_con.commit()
			self.auto_increment(self.twitter_expand_domain_table)

			# sql_insert_to_destinationdomain = """INSERT into %s (domain, create_time) SELECT  dd.domain, dd.update_time_format from 
			# 	(SELECT update_time_format, domain from (SELECT aa.update_time_format, \
			# 	lower(TRIM( LEADING 'www.' FROM  SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX(SUBSTR(lower(bb.red_url) , locate("://", bb.red_url  )+3), "/", 1), "****", 1 ) , "?", 1), ":", 1) )) as domain \
			# 	from %s as aa INNER JOIN %s as bb on aa.guid=bb.guid where bb.red_url<>"" and bb.red_url is not null \
			# 	ORDER BY aa.update_time_format) cc group by cc.domain ORDER BY cc.update_time_format) as dd
			# 	LEFT JOIN %s as ee on dd.domain=ee.domain where ee.domain is null
			# """ % (self.twitter_destination_domain_table, self.couponinfohandle_table, self.red_url_table,self.twitter_destination_domain_table)
			sql_insert_to_destinationdomain = """INSERT into %s (domain, create_time, brand) \
				SELECT  dd.domain, dd.update_time_format, dd.brand from (SELECT update_time_format, \
				domain, brand from (SELECT aa.update_time_format, concat(bb.domain,".",bb.suffix) \
				as domain, bb.brand as brand from %s as aa INNER JOIN %s as bb on aa.guid=bb.guid \
				where bb.red_url<>"" and bb.red_url is not null ORDER BY aa.update_time_format) cc \
				group by cc.domain ORDER BY cc.update_time_format) as dd LEFT JOIN %s as ee on \
				dd.domain=ee.domain where ee.domain is null""" \
				% (self.twitter_destination_domain_table, self.couponinfohandle_table, self.red_url_table,\
				self.twitter_destination_domain_table)
			self.coupon_cur.execute(sql_insert_to_destinationdomain)
			self.coupon_con.commit()
			self.auto_increment(self.twitter_destination_domain_table)
			print("insert to domain list")


			####new#######   update oc_times
			sql_update_domain_octimes = """UPDATE %s as a inner join (SELECT CONCAT(domain,".",suffix) \
				as dm, count(*) as oc_times from %s where red_url<>"" and red_url is not null GROUP by dm) \
				as b on a.domain = b.dm set a.oc_times=b.oc_times""" \
				% (self.twitter_destination_domain_table, self.red_url_table)
			self.coupon_cur.execute(sql_update_domain_octimes)
			self.coupon_con.commit()
			print("update domain oc_times")


		###########   update fields in handle_table
		### sql_update_domain = """UPDATE %s set expand_domain = lower( TRIM( LEADING 'www.' FROM  SUBSTRING_INDEX( SUBSTRING_INDEX( SUBSTRING_INDEX(SUBSTR(expand_url, locate("://", expand_url )+3), "/", 1), "****", 1 ) , "?", 1)) )""" % (self.couponinfohandle_table)
		### sql_update_guid = """UPDATE %s set guid = MD5( lower(SUBSTRING_INDEX(expand_url, "****", 1) ) )" % (self.couponinfohandle_table)
		### self.coupon_cur.execute(sql_update_domain)
		### self.coupon_cur.execute(sql_update_guid)
		### self.coupon_con.commit()


		############   select to black_table
		handled_coupon_fields = self.handle_to_black_commonfileds
		fields_aliname = ",".join(["b."+a.strip() for a in handled_coupon_fields.split(",")])
		sql_selectinto = """INSERT into %s (%s, black_reason, black_url) SELECT 
			%s, a.black_reason, a.red_url from %s as a INNER JOIN %s as b \
			on a.guid=b.guid  where a.is_black = 1""" % ( self.couponblacklist_table, \
			handled_coupon_fields, fields_aliname, self.red_url_table, self.couponinfohandle_table )
		self.coupon_cur.execute(sql_selectinto)
		self.coupon_con.commit()
		self.auto_increment(self.couponblacklist_table)
		print("insert to black table")


		# ###########   insert to need handled table e.g.:etsy.com; instagram.com
		# sql_selectinto_needhandled = """INSERT into %s (%s, black_reason, black_url) \
		# 	SELECT %s, a.black_reason, a.red_url from %s as a INNER JOIN %s as b \
		# 	on a.guid=b.guid  where a.is_black = 2""" % (self.couponneedhandled_list_table, \
		# 	handled_coupon_fields, fields_aliname, self.red_url_table, self.couponinfohandle_table)
		# self.coupon_cur.execute(sql_selectinto_needhandled)
		# self.coupon_con.commit()
		# self.auto_increment(self.couponneedhandled_list_table)
		# print("insert to need handled table e.g.:etsy.com; instagram.com")


		## pending url temporary block/pending
		###########   insert to black pending table for handle tomorrow
		sql_selectinto_needhandled = """INSERT into %s (%s, black_reason, black_url) \
			SELECT %s, a.black_reason, a.red_url from %s as a INNER JOIN %s as b \
			on a.guid=b.guid  where a.is_black = 3 or a.is_black = 4""" % (self.couponblackpending_table, \
			handled_coupon_fields, fields_aliname, self.red_url_table, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_selectinto_needhandled)
		self.coupon_con.commit()
		self.auto_increment(self.couponblackpending_table)
		print("insert to pending table for handle tomorrow")


		########### insert data to do later table
		sql_selectinfo_todo_later = """INSERT INTO %s (%s) SELECT %s from %s where
		 	`code` = "" and `discount`="" and `price`='' and `freeshipping`=0""" % ( \
		 		self.couponinfohandle_todo_later_table, handled_coupon_fields, \
		 		handled_coupon_fields, self.couponinfohandle_table
		 	)
		self.coupon_cur.execute(sql_selectinfo_todo_later)
		self.coupon_con.commit()
		self.auto_increment(self.couponinfohandle_todo_later_table)
		print("insert to todo later table for handle tomorrow")


		###########   delete todolater and black and pending from handle_table
		sql_delete_todo_later = """DELETE from %s where `code` = "" and `discount`="" 
			and `price`='' and `freeshipping`=0""" % (self.couponinfohandle_table,)
		self.coupon_cur.execute(sql_delete_todo_later)
		self.coupon_con.commit()
		print("delete todo later data from handle_table")

		sql_delete = """DELETE from %s where id in (SELECT id from (
			SELECT  b.id from  %s as a inner JOIN %s as b on a.guid=b.guid 
			where a.is_black = 1 or a.is_black = 3 or a.is_black = 4) as c )""" \
			% (self.couponinfohandle_table, self.red_url_table, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()
		print("delete black data from handle_table")


		###########   insert to tablefordup
		sql_insert_to_duptable = """INSERT into %s (source, guid, advertiserid, advertisername, `text`, full_text, title, description, \
			code, discount, price, freeshipping, coupon_requirement, linkid, brand, landing_page, url_status, url, media_url, media_type, retweet_count, \
			favorite_count, start_date, end_date, first_create_date, adddate ) SELECT "twitter_raw",a.guid, a.user_id, a.user_name, a.text, \
			a.full_text, a.show_title, a.show_description, a.code, a.discount, a.price, a.freeshipping, a.coupon_requirement, a.tweet_id, b.brand, b.red_url, b.url_status, \
			a.expand_url, a.media_url, a.media_type, a.retweet_count, a.favorite_count, a.coupon_start_date, a.coupon_end_date, a.create_time_format, a.update_time_format \
			from %s as a left join %s as b on a.guid=b.guid""" \
			% (self.dup_base_table, self.couponinfohandle_table, self.red_url_table)
		self.coupon_cur.execute(sql_insert_to_duptable)
		self.coupon_con.commit()
		print("insert to tablefordup")


		############   insert to infohandled all table
		sql_init_infotable = "INSERT INTO %s (%s) select %s from %s" % (self.couponinfohandle_all_table, \
			self.handle_to_all_fields, self.handle_to_all_fields, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_init_infotable)
		self.coupon_con.commit()
		self.auto_increment(self.couponinfohandle_all_table)
		print("insert to infohandled all table")


		############   init infohandled table
		sql_init_infotable = "TRUNCATE %s" % (self.couponinfohandle_table)
		self.coupon_cur.execute(sql_init_infotable)
		self.coupon_con.commit()
		print("init infohandled table")


		self.close_db()
		print "judge black first time done...."


	def judge_white_tld(self, url):
		for white_domain in self.white_tld_domains:
			for white_pattern in self.white_tld_domains[white_domain]:
				if white_pattern.lower() in url.lower():
					return white_domain
		return "False False"


	def get_whitetld_list(self):
		self.connect_db()
		sql_select = "SELECT domain from %s" % (self.domain_whitebrand_list, )
		self.coupon_cur.execute( sql_select )
		self.white_tld_domains = {domain[0].lower():["."+domain[0].lower()+".", "/"+domain[0].lower()+"."] \
			for domain in self.coupon_cur.fetchall()}
		self.close_db()


	def get_reviewed_blackdomain(self):
		self.connect_db()
		sql_select = "SELECT domain from %s" % (self.negative_domain_reviewed_table, )
		self.coupon_cur.execute( sql_select )
		self.reviewed_blackdomains = {domain[0].lower():["."+domain[0].lower(), "/"+domain[0].lower()] \
			for domain in self.coupon_cur.fetchall()}
		for d in self.reviewed_blackdomains:
			if "blogspot." in d:
				self.reviewed_blackdomains[d] = ["."+d.split("blogspot.", 1)[0]+"blogspot.com", "/"+d.split("blogspot.", 1)[0]+"blogspot.com"]

		sql_select_reviewing = "SELECT domain from %s" % (self.negative_domain_reviewing_table, )
		self.coupon_cur.execute( sql_select_reviewing )
		reviewingall_blackdomains = {domain[0].lower():["."+domain[0].lower(), "/"+domain[0].lower()] \
			for domain in self.coupon_cur.fetchall()}
		for d in reviewingall_blackdomains:
			if "blogspot." in d:
				reviewingall_blackdomains[d] = ["."+d.split("blogspot.", 1)[0]+"blogspot.com", "/"+d.split("blogspot.", 1)[0]+"blogspot.com"]

		self.reviewing_blackdomains = {
			domain:reviewingall_blackdomains[domain] for domain in reviewingall_blackdomains \
				if ( not self.reviewed_blackdomains.has_key(domain) and not self.black_domains.has_key(domain) and not self.needhandled_domains.has_key(domain) )
		}

		self.reviewed_negative_blackdomains = {
			domain:self.reviewed_blackdomains[domain] for domain in self.reviewed_blackdomains \
				if ( not self.black_domains.has_key(domain) and not self.needhandled_domains.has_key(domain) )
		}
		self.close_db()



if "__main__" == __name__:
	redurl = Redurl()
	# print self.black_domains
	# print self.needhandled_domains
	# print self.reviewed_blackdomains
	# print self.reviewing_blackdomains

	redurl.get_blacklist()
	redurl.get_whitetld_list()
	redurl.get_reviewed_blackdomain()

	redurl.handle_redurl()
	if config.review_whitedomain:
		redurl.white_brand_forreview()
	redurl.insertto_destination_black()
