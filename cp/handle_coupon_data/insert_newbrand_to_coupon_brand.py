import MySQLdb as mdb
import re
from HTMLParser import HTMLParser
import config_couponapi


class Join_redirect_url():
	def __init__(self):
		self.DB_HOST = config_couponapi.DB_HOST
		self.DB_NAME = config_couponapi.DB_NAME
		self.DB_USER = config_couponapi.DB_USER
		self.DB_PSWD = config_couponapi.DB_PSWD
		self.CHARSET = config_couponapi.CHARSET

		self.brand_domain_table = config_couponapi.coupon_domain_brand_table
		self.cad_domain_brand_table = config_couponapi.coupon_ad_domain_brand_table

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()
		self.coupon_cur.execute("SET NAMES utf8")

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def insert_newbrand_to_couponbrand_table(self):
		sql_insert = """INSERT INTO %s (domain, brand) SELECT a.domain, a.brand FROM %s as a LEFT JOIN %s as b ON a.domain = b.domain WHERE b.domain is NULL AND  a.domain is NOT NULL AND a.domain <> '' GROUP BY domain""" % (self.brand_domain_table, self.cad_domain_brand_table, self.brand_domain_table)
		# print sql_insert
		self.coupon_cur.execute(sql_insert)
		self.coupon_con.commit()


if "__main__" == __name__:
	red_url = Join_redirect_url()
	red_url.connect_db()
	red_url.insert_newbrand_to_couponbrand_table()
	red_url.close_db()
