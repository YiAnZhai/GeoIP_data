# -*- coding: utf-8 -*-
import os
import MySQLdb as mdb
import time
import config

class Today_table():
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET
		self.couponinfohandle_table = config.couponinfohandle_table

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def log_status(self, table, fields, datas):
		self.connect_db()
		sql_insert = """INSERT INTO `%s` (`%s`) values (%s)""" % (table, "`,`".join(fields), ",".join(["%s"]*len(fields)) )
		self.coupon_cur.execute(sql_insert, datas)
		self.coupon_con.commit()
		self.close_db()

	def check_table(self):
		self.connect_db()
		sql_ckeck = """select exists( SELECT * from %s)""" % (self.couponinfohandle_table)
		self.coupon_cur.execute(sql_ckeck)
		coupon_count = self.coupon_cur.fetchone()
		self.close_db()
		return coupon_count[0]

	def execute_sql(self, sql):
		self.connect_db()
		self.coupon_cur.execute( sql )
		self.coupon_con.commit()
		results = self.coupon_cur.fetchall()
		self.close_db()
		return results



if "__main__" == __name__:
	today_table = Today_table()
	
	while True:
		if not today_table.check_table():
			print "no data in handled table yet, sleep..."
			time.sleep(100)
		else:
			print "$$$$$$$$$$$$$$$$$$$ attention!!! everybody * show * begins $$$$$$$$$$$$$$$$$$$"
			print "begin to get data from handled table, for blacklist, get_redurl, dup_remove..."
			time.sleep(10)
			break

	os.system("./coupon_auto_run.sh")