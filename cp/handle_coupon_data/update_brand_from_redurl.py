# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import tldextract
import config_couponapi

class Update_Brand_From_Redurl():
	def __init__(self):
		self.DB_HOST = config_couponapi.DB_HOST
		self.DB_NAME = config_couponapi.DB_NAME
		self.DB_USER = config_couponapi.DB_USER
		self.DB_PSWD = config_couponapi.DB_PSWD
		self.CHARSET = config_couponapi.CHARSET
		self.red_url_table = config_couponapi.api_redirect_url_table
		self.coupon_api_table = config_couponapi.coupon_apifresh_table
		self.exception_suffix = set(["uk.com"])

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()


	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def extract_domain(self, url):
		subdomain, domain, suffix = tldextract.extract(url)
		if "%s.%s" % (domain, suffix) in self.exception_suffix and subdomain:
			subdomain, domain, suffix_wrong = tldextract.extract(subdomain + ".com")
			suffix = "%s.%s" % (domain, suffix)

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

		if subdomain.startswith("store") or subdomain.startswith("shop"):
			brand = "%s.%s" % ( domain_full,suffix_fix )
		else:
			if domain and suffix:
				brand = "%s.%s" % ( domain,suffix_fix )
			else:
				brand = ""

		if "etsy" == domain and "/shop/" in url:
			sub = url.split("/shop/")[-1].split("/")[0].split("?")[0]
			if sub:
				brand = sub + ".etsy." + suffix_fix
				brand = brand.lower()
		return brand

	def handle_redurl(self):
		self.connect_db()
		sql_select_url = """SELECT a.id, a.red_url from %s as a inner join %s as b on a.guid=b.guid""" \
			% (self.red_url_table, self.coupon_api_table)
		self.coupon_cur.execute(sql_select_url)
		id_urls = self.coupon_cur.fetchall()
		id_update_fields = []
		for id_url in id_urls:
			id_update_fields.append([])
			subdomain, domain_full, domain, suffix = self.extract_domain(id_url[1].lower())
			brand = self.get_brand( subdomain, domain_full, domain, suffix, id_url[1] )
			id_update_fields[-1] += [ domain_full, domain, brand, suffix, id_url[0] ]

		# print id_update_fields
		print "extract red_url table's domain........"
		sql_update = """UPDATE %s set  domain=%s, domain_tld=%s, brand=%s, suffix=%s where id=%s""" \
			% ( self.red_url_table, "%s", "%s", "%s", "%s", "%s" )
		self.coupon_cur.executemany( sql_update, id_update_fields )
		self.coupon_con.commit()

		############## update brand ##################
		update_brand = """UPDATE %s t1, %s t2 set t1.brand=t2.brand where t1.guid=t2.guid and t2.url_status!=0""" \
			% ( self.coupon_api_table, self.red_url_table)
		self.coupon_cur.execute(update_brand)
		self.coupon_con.commit()

		self.close_db()
		print "red_url table extract success"
		print

if "__main__" == __name__:
	redurl = Update_Brand_From_Redurl()
	redurl.handle_redurl()