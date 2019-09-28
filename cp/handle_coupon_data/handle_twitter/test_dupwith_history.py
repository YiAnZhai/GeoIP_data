# -*- coding: utf-8 -*-
import os
from datetime import datetime as dt
import datetime
import config
import MySQLdb as mdb
import time

class Today_table():
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET
		self.dup_innertwitterall_table = config.dup_innertwitterall_table
		self.dup_base_table = config.dup_base_table
		self.twitter_today_copy_toall = config.twitter_today_copy_toall

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def insert_to_today(self, today, next_day):
		self.connect_db()
		sql_ckeck = """SELECT min(id) from %s where adddate like "%s%s" """ % (self.dup_innertwitterall_table, today, r"%")
		sql_ckeck = """SELECT min(id) from %s where adddate > "%s 17:37" """ % (self.dup_innertwitterall_table, today)
		self.coupon_cur.execute(sql_ckeck)
		id1 = self.coupon_cur.fetchone()[0]

		sql_ckeck = """SELECT min(id) from %s where adddate like "%s%s" """ % (self.dup_innertwitterall_table, next_day, r"%")
		sql_ckeck = """SELECT min(id) from %s where adddate > "%s 17:37" """ % (self.dup_innertwitterall_table, next_day)
		self.coupon_cur.execute(sql_ckeck)
		id2 = self.coupon_cur.fetchone()[0]

		if not id1 or not id2:
			return
		# sql_ckeck = """SELECT count(*) from %s where id BETWEEN %s and %s """ % (self.dup_innertwitterall_table, id1, id2-1)
		# self.coupon_cur.execute(sql_ckeck)
		# count = self.coupon_cur.fetchone()[0]
		# print count
		# print

		sql_insert = """INSERT INTO %s (%s) SELECT %s from %s where id BETWEEN %s and %s""" % (self.dup_base_table, self.twitter_today_copy_toall, self.twitter_today_copy_toall, self.dup_innertwitterall_table, id1, id2-1)
		count = self.coupon_cur.execute(sql_insert)
		self.coupon_con.commit()
		self.close_db()
		return count

	def delete_today_table(self):
		self.connect_db()
		sql_delete = "delete from %s" % (self.dup_base_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()
		self.close_db()

	# def insert_log(self, field, value):
	# 	"INSERT INTO coupon_twitter_tongji_dupfilter_with_history (%s)"

def write_log(text):
	f = open("dup_remove_count.txt", "a")
	f.write(text)
	f.close()


if "__main__" == __name__:
	a = Today_table()
	mintime = "2016-06-16"
	mintime = "2016-06-15"

	for i in range(21):
	# for i in range(1):
		
		print "please make sure your data is OK first!!!"
		exit()


		day = (dt.strptime(mintime, "%Y-%m-%d") + datetime.timedelta(days=int(i))).strftime("%Y-%m-%d")
		next_day = (dt.strptime(mintime, "%Y-%m-%d") + datetime.timedelta(days=int(i+1))).strftime("%Y-%m-%d")
		print next_day
		count = a.insert_to_today(day, next_day)

		write_log(next_day + "(%s days):\n    total: " % i + str(count) + "\n")


		time1 = time.time()
		os.system("python ./dup_remove_innertwitter.py %s" % count)
		time2 = time.time()
		
		write_log("time_take: " + str(round((time2 - time1)/60,1)) + " mins\n\n")

		a.delete_today_table()
