# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config
import tldextract
import common_functions 

class Coupon_dupbase(object):
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET

		self.dup_basenegwordall_table = config.dup_basenegwordall_table
		self.dup_base_table = config.dup_base_table
		self.negative_words = config.negative_words
		self.twitter_today_copy_toall = config.twitter_today_copy_toall


	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def auto_increment(self, table_name):
		sql_change_autoincrement = "ALTER TABLE %s AUTO_INCREMENT = 1" % (table_name, )
		re_auto = self.coupon_cur.execute(sql_change_autoincrement)
		self.coupon_con.commit()
		return re_auto
	
	def delete_negaword(self):
		self.connect_db()
		
		###############detect black in title or not
		sql_select_url = "SELECT id, description from %s" % (self.dup_base_table, )
		self.coupon_cur.execute(sql_select_url)
		id_descriptions = self.coupon_cur.fetchall()
		id_update_fields = []
		for id_desc in id_descriptions:
			word_list = common_functions.word_sep(id_desc[1])
			title_flag = self.isblack_title(word_list)
			if title_flag:
				id_update_fields.append([title_flag, "negative_w_in_title", id_desc[0]])

		# print id_update_fields
		sql_update = """UPDATE %s set `keywords`=%s, category=%s where id=%s""" % (self.dup_base_table, "%s", "%s", "%s")
		print "update base dup table black title or not........"
		self.coupon_cur.executemany(sql_update, id_update_fields)
		self.coupon_con.commit()


		##############back up black title to table
		sql_backupblack = """INSERT INTO %s (%s) SELECT %s from %s where category = 'negative_w_in_title'""" \
			% (self.dup_basenegwordall_table, self.twitter_today_copy_toall, \
				self.twitter_today_copy_toall, self.dup_base_table)
		self.coupon_cur.execute(sql_backupblack)
		self.coupon_con.commit()
		self.auto_increment(self.dup_basenegwordall_table)

		##############delete black in basedup table
		sql_deleteblack = """DELETE FROM %s where category = 'negative_w_in_title'""" \
			% (self.dup_base_table, )
		self.coupon_cur.execute(sql_deleteblack)
		self.coupon_con.commit()


		self.close_db()


	def isblack_title(self, word_list):
		for w in word_list:
			if w in self.negative_words:
				return w
		return False



if "__main__" == __name__:
	coupon = Coupon_dupbase()
	coupon.delete_negaword()