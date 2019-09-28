# -*- coding: utf-8 -*-
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

class R(Redurl):
	def __init__(self):
		super(R,self).__init__()
		self.twitter_etsy_table = config.twitter_etsy_table
		self.twitter_instagram_table = config.twitter_instagram_table
		self.couponneedhandled_list_all_table = config.couponneedhandled_list_all_table
		# print self.DB_HOST


	def judge_black(self, url):
		url_lower = url.lower()
		subdomain, domain_full, domain, suffix = self.extract_domain( url_lower )
		if domain in ("blogspot", ):
			pre_url = url_lower.split("blogspot.", 1)[0]
			url_lower = pre_url + "blogspot.com"
			return ( 1, domain )
		for black_domain in self.black_domains:
			for black_pattern in self.black_domains[black_domain]:
				if black_pattern in url_lower:
					return ( 1, black_domain )

		sensitive_words = ["coupon", "deal", "sale", "saving", "promo", "code", "save", "offer", "discount", "news", "blog", "voucher"]
		for s_word in sensitive_words:
			if s_word in domain:
				return ( 1, s_word )
		if subdomain in ("app",):
                        return (1,subdomain)
		if "@" in subdomain or "@" in domain:
			return ( 1, "@" )
		if suffix in ("edu", "gov"):
                        return ( 1, "no_retail" )

		return "False False"


	def handle_redurl_etsy(self):
		self.get_blacklist()
		self.get_whitetld_list()
		self.connect_db()
		sql_select_url = """SELECT a.id, a.ext5, a.ext7 from %s as a inner join %s as b on a.guid=b.guid""" \
		 	% (self.twitter_etsy_table, self.couponneedhandled_list_table)
		self.coupon_cur.execute( sql_select_url )
		id_url_urls = self.coupon_cur.fetchall()
		id_urls = []
		for a in id_url_urls:
			url = a[1]
			if "None" == url:
				url = a[2]
			if "None" == url:
				url = ""
			id_urls.append( (a[0], url) )

		id_update_fields = []
		for id_url in id_urls:
			id_update_fields.append([])

			##############tld extarct
			subdomain, domain_full, domain, suffix = self.extract_domain( id_url[1].lower() )

			##############black or not
			is_black = self.judge_black( id_url[1].lower() )
			if "False False" == is_black:
				black_reason = ""
			else:
				black_reason = is_black[1]

			brand = self.get_brand(subdomain, domain_full, domain, suffix, id_url[1])

			id_update_fields[-1] += [brand, black_reason, id_url[0]]

		sql_update = """UPDATE %s set domain=%s, `ignore`=%s where id=%s""" % (self.twitter_etsy_table, "%s", "%s", "%s")
		print "update etsy_redurl table black or not........"
		self.coupon_cur.executemany(sql_update, id_update_fields)
		self.coupon_con.commit()
		self.close_db()


	def handle_redurl_instagram(self):
		# self.get_blacklist()
		# self.get_whitetld_list()
		self.connect_db()
		sql_select_url = """SELECT a.id, a.ext8_dest from %s as a inner join %s as b on a.guid=b.guid """ \
			% (self.twitter_instagram_table, self.couponneedhandled_list_table)
		self.coupon_cur.execute(sql_select_url)
		id_url_urls = self.coupon_cur.fetchall()
		id_urls = []
		for a in id_url_urls:
			url = a[1]
			if "None" == url:
				url = ""
			id_urls.append( (a[0], url) )

		id_update_fields = []
		for id_url in id_urls:
			id_update_fields.append([])

			##############tld extarct
			subdomain, domain_full, domain, suffix = self.extract_domain( id_url[1].lower() )

			##############black or not
			is_black = self.judge_black( id_url[1].lower() )
			if "False False" == is_black:
				black_reason = ""
			else:
				black_reason = is_black[1]

			brand = self.get_brand(subdomain, domain_full, domain, suffix, id_url[1])
			id_update_fields[-1] += [brand, black_reason, id_url[0]]

		############ print id_update_fields
		sql_update = """UPDATE %s set domain=%s, `ignore`=%s where id=%s""" % (self.twitter_instagram_table, "%s", "%s", "%s")
		print "update inst_redurl table black or not........"
		self.coupon_cur.executemany(sql_update, id_update_fields)
		self.coupon_con.commit()
		self.close_db()


	def insertto_destination_black(self):

		###########insert to tablefordup
		self.etsy_insert_fordup()
		self.inst_insert_fordup()
		
		self.connect_db()
		###########insert to needhandled all table
		sql_to_needhandledall_table = "INSERT INTO %s (%s, black_reason, black_url) select %s, black_reason, black_url from %s" \
			% (self.couponneedhandled_list_all_table, self.handle_to_black_commonfileds, 
				self.handle_to_black_commonfileds, self.couponneedhandled_list_table)
		self.coupon_cur.execute( sql_to_needhandledall_table )
		self.coupon_con.commit()
		self.auto_increment(self.couponneedhandled_list_all_table)
		print "insert to needhandled all table"

		###########init needhandled table
		sql_init_needhandled_table = "truncate %s" % (self.couponneedhandled_list_table)
		self.coupon_cur.execute( sql_init_needhandled_table )
		self.coupon_con.commit()
		print "init needhandled table"
		
		self.close_db()

	def etsy_insert_fordup(self):

		ignore_domains_andsub = ["Etsy.me", "Fb.me", "Ow.ly", "Buff.ly", "Small.bz", 
			"Tuppu.net", "Goo.gl", "Bit.ly", "Jto.li", "Ift.tt", "Dld.bz", "twitter.com", 
			"Eepurl.com", "s.ripl.com", "Tuppu.net", "Tinyurl.com", "Dlvr.it", "qps.ru", 
			"Youtube.com", "Pinterest.com", "Amazon.com", "Instagram.com", "Eepurl.com",
			"tumblr.com", "Google.com"]
		ignore_domains_andsub_lower = [a.lower() for a in ignore_domains_andsub]

		self.connect_db()
		handled_coupon_fields = self.handle_to_black_commonfileds
		fields_aliname = ",".join(["a."+a.strip() for a in handled_coupon_fields.split(",")])
		sql_select_url = """SELECT "etsy",a.guid, a.user_id, a.clean_text, a.text, 
			a.code, a.discount, a.freeshipping, a.tweet_id, c.red_url, c.url_status, a.expand_url, a.retweet_count, 
			 a.favorite_count, a.create_time_format, a.update_time_format, a.expand_domain, b.domain
			 from %s as a inner join %s as b on a.guid=b.guid inner join %s as c on a.guid=c.guid 
			 where a.black_reason ='etsy.com' and b.domain<>'' """ \
		 % (self.couponneedhandled_list_table, self.twitter_etsy_table, self.red_url_table)
		self.coupon_cur.execute(sql_select_url)
		results_etsy = self.coupon_cur.fetchall()

		etsy_coupons = []
		for tweet in results_etsy:
			for i in range(2):
				domain_index = 16+i
				for neg_domain in ignore_domains_andsub_lower:
					if neg_domain == tweet[domain_index] or tweet[domain_index].endswith( "."+neg_domain ):
						break
				else:
					if "etsy.com" != tweet[domain_index] and "@" not in tweet[domain_index]:
						subdomain, domain_full, domain, suffix = self.extract_domain( tweet[domain_index].lower() )
						brand = self.get_brand(subdomain, domain_full, domain, suffix, tweet[domain_index])
						if tweet[domain_index]:
							is_black = self.judge_black( "www."+tweet[domain_index] )
							if "False False" == is_black:
								etsy_coupons.append( tweet[:16] + (brand,) )


		###########insert to tablefordup
		sql_insert_to_duptable = """INSERT into %s (source, guid, advertiserid,title,
		 description, code, discount, freeshipping, linkid, landing_page, url_status, url,
		  retweet_count, favorite_count, adddate, start_date, brand) values (%s)""" \
		% (self.dup_base_table, ", ".join(["%s"]*17))
		self.coupon_cur.executemany(sql_insert_to_duptable, etsy_coupons)
		self.coupon_con.commit()
		print "etsy to dupbase"
		self.close_db()

	def inst_insert_fordup(self):
		ignore_domains_andsub = ["Facebook.com", "Etsy.com", "Bit.ly", "Youtube.com", 
			"Twitter.com", "M.facebook.com", "Have2have.it", "Like2have.it", "M.youtube.com", 
			"Instagram.com", "Lyft.com", "Uber.com",]
		ignore_domains_andsub_lower = [a.lower() for a in ignore_domains_andsub]

		self.connect_db()
		handled_coupon_fields = self.handle_to_black_commonfileds
		fields_aliname = ",".join(["a."+a.strip() for a in handled_coupon_fields.split(",")])
		sql_select_url = """SELECT "instagram",a.guid, a.user_id, a.clean_text, a.text, 
			a.code, a.discount, a.freeshipping, a.tweet_id, b.ext8_dest, b.status_code, a.expand_url, a.retweet_count, 
			a.favorite_count, a.create_time_format, a.update_time_format, b.domain, b.ext3
			from %s as a inner join %s as b on a.guid=b.guid where a.black_reason ='instagram.com' 
			and b.domain <> '' and b.ignore = ''""" \
		 	% (self.couponneedhandled_list_table, self.twitter_instagram_table)
		self.coupon_cur.execute(sql_select_url)
		results_etsy = self.coupon_cur.fetchall()

		inst_coupons = []
		for tweet in results_etsy:
			for neg_domain in ignore_domains_andsub_lower:
				if neg_domain == tweet[16] or "."+neg_domain in tweet[16]:
					break
			else:
				inst_coupons.append( list(tweet[:17]) )
				if "%" in tweet[17] or "off" in tweet[17] or "$" in tweet[17]:
					inst_coupons[-1][3], inst_coupons[-1][4] = common_functions.del_linkat(tweet[17]), tweet[17]
					inst_coupons[-1][5] = common_functions.get_code(tweet[17])
					inst_coupons[-1][6] = common_functions.get_discount(tweet[17])
					inst_coupons[-1][7] = common_functions.check_freeshipping(tweet[17])

		###########insert to tablefordup
		sql_insert_to_duptable = """INSERT into %s (source, guid, advertiserid,title,
				description, code, discount, freeshipping, linkid, landing_page, url_status, url,
				retweet_count, favorite_count, adddate, start_date, brand) values (%s)""" \
		% (self.dup_base_table, ", ".join(["%s"]*17))
		self.coupon_cur.executemany(sql_insert_to_duptable, inst_coupons)
		self.coupon_con.commit()
		print "instagram to dupbase"

		self.close_db()


if "__main__" == __name__:
	redurl = R()
	redurl.handle_redurl_etsy()
	redurl.handle_redurl_instagram()
	redurl.insertto_destination_black()
