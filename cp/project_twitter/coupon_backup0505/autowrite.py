#-*- coding: utf-8 -*-
import os
import time
import config
import MySQLdb as mdb


class Today_table():
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET
		self.couponinfohandle_table = config.couponinfohandle_table
		self.dup_base_table = config.dup_base_table

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def check_table(self, table):
		self.connect_db()
		sql_ckeck = """select exists( SELECT * from %s)""" % (table, )
		self.coupon_cur.execute(sql_ckeck)
		coupon_count = self.coupon_cur.fetchone()
		self.close_db()
		return coupon_count[0]

	def check_datahandle_finished(self):
		num_a = self.check_table( self.couponinfohandle_table )
		if not num_a:
			num_b = self.check_table( self.dup_base_table )
			if not num_b:
				return True
		return False


tt = Today_table()
#for i in range(22, 32):
for i in range(60, 62):
	while not tt.check_datahandle_finished():
		time.sleep(50)

	print "file: ", i
	t1 = time.time()
	
	os.system("python ./twitter_json_todb_all.py %s" % i)
	os.system("python ./getcode_discount.py")

	t2 = time.time()
	print
	print
	print
#	time.sleep(200 - (t2 - t1))

