# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config_couponapi

class FreshAPI():
	def __init__(self):
		self.DB_HOST = config_couponapi.DB_HOST
		self.DB_NAME = config_couponapi.DB_NAME
		self.DB_USER = config_couponapi.DB_USER
		self.DB_PSWD = config_couponapi.DB_PSWD
		self.CHARSET = config_couponapi.CHARSET
		self.coupon_apifresh_table = config_couponapi.coupon_apifresh_table

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def update_guid(self):
		self.connect_db()
		##########update guid in coupon_apifresh_table
		sql_update_guid = """UPDATE %s set guid = MD5( url )""" % (self.coupon_apifresh_table)
		self.coupon_cur.execute(sql_update_guid)
		self.coupon_con.commit()
		self.close_db()


if "__main__" == __name__:
	today_table = FreshAPI()
	today_table.update_guid()