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

		self.today_fresh_table = config_couponapi.coupon_apifresh_table
		self.ad_brand_table = config_couponapi.coupon_ad_domain_brand_table


	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()
		self.coupon_cur.execute("SET NAMES utf8")

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def update_brand(self):
		target_table = self.today_fresh_table
		brand_table = self.ad_brand_table
		#sql_update = """UPDATE %s INNER JOIN %s ON %s.advertiserid = %s.advertiserid AND %s.source = %s.source SET %s.brand = %s.domain	WHERE %s.advertisername is not null and %s.advertisername <> '' and %s.advertisername <> '*' and %s.brand is null""" % (target_table, brand_table, target_table, brand_table, target_table, brand_table, target_table, brand_table, target_table, target_table, target_table, target_table)
		sql_update = """UPDATE %s INNER JOIN %s ON %s.advertiserid = %s.advertiserid AND %s.source = %s.source SET %s.brand = %s.domain	WHERE %s.brand is null""" % (target_table, brand_table, target_table, brand_table, target_table, brand_table, target_table, brand_table, target_table)
		self.coupon_cur.execute(sql_update)
		self.coupon_con.commit()


if "__main__" == __name__:
	red_url = Join_redirect_url()
	red_url.connect_db()
	red_url.update_brand()
	red_url.close_db()
