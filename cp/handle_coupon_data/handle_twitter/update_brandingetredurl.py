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

class Red_domain(Redurl):
	def __init__(self):
		super(Red_domain,self).__init__()


	def handle_redurl(self):

		self.connect_db()
		# sql_select_url = "SELECT a.id, a.red_url from %s as a" \
		# 	% (self.red_url_table)
		sql_select_url = """SELECT a.id, a.red_url from %s as a inner join %s as b on a.guid=b.guid""" \
			% (self.red_url_table, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_select_url)
		id_urls = self.coupon_cur.fetchall()

		id_update_fields = []
		for id_url in id_urls:
			id_update_fields.append([])
			subdomain, domain_full, domain, suffix = self.extract_domain(id_url[1].lower())

			is_black = self.judge_black( id_url[1].lower() )
			if "False False" != is_black and 2 == is_black[0]:
				id_update_fields[-1] += [is_black[0], is_black[1]]
			else:
				id_update_fields[-1] += [-1, ""]

			brand = self.get_brand( subdomain, domain_full, domain, suffix, id_url[1] )
			id_update_fields[-1] += [ domain_full, domain, brand, suffix, id_url[0] ]

		# print id_update_fields
		print "extract red_url table's domain........"
		sql_update = """UPDATE %s set is_black=%s, black_reason=%s, domain=%s, domain_tld=%s, brand=%s, suffix=%s where id=%s""" \
			% ( self.red_url_table, "%s", "%s", "%s", "%s", "%s", "%s", "%s" )
		self.coupon_cur.executemany( sql_update, id_update_fields )
		self.coupon_con.commit()


		self.close_db()
		print "red_url table extract success"
		print

	def handle_etsy_instagram(self):
		self.connect_db()
		
		handled_coupon_fields = self.handle_to_black_commonfileds
		fields_aliname = ",".join(["b."+a.strip() for a in handled_coupon_fields.split(",")])
		###########insert to need handled table e.g.:etsy.com; instagram.com
		sql_selectinto_needhandled = """INSERT into %s (%s, black_reason, black_url) \
			SELECT %s, a.black_reason, a.red_url from %s as a INNER JOIN %s as b \
			on a.guid=b.guid  where a.is_black = 2""" % (self.couponneedhandled_list_table, \
			handled_coupon_fields, fields_aliname, self.red_url_table, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_selectinto_needhandled)
		# self.coupon_con.commit()
		self.auto_increment(self.couponneedhandled_list_table)
		print("insert to need handled table e.g.:etsy.com; instagram.com")
		print

		###########   delete black from handle_table
		sql_delete = """DELETE from %s where id in (SELECT id from (
			SELECT  b.id from  %s as a inner JOIN %s as b on a.guid=b.guid 
			where a.is_black = 2) as c )""" \
			% (self.couponinfohandle_table, self.red_url_table, self.couponinfohandle_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()
		print("delete black from handle_table")

		self.close_db()


	def add_pending_data(self):
		self.connect_db()

		handled_coupon_fields = self.handle_to_black_commonfileds
		############   black_pending data back to handle list
		sql_insert_backto_handledtable = """INSERT INTO %s (%s) SELECT %s from %s""" \
			% (self.couponinfohandle_table, handled_coupon_fields, handled_coupon_fields, self.couponblackpending_table)
		self.coupon_cur.execute(sql_insert_backto_handledtable)
		self.coupon_con.commit()
		print("black_pending data back to handle list")


		###########    init black pending table
		sql_init_infotable = "TRUNCATE %s" % (self.couponblackpending_table)
		self.coupon_cur.execute(sql_init_infotable)
		self.coupon_con.commit()
		print("init infohandled pending table")

		self.close_db()


	def update_domain_titledes(self):
		self.connect_db()
		sql_update_domaintable = """UPDATE %s set is_new=0 where is_new=1""" \
			% ( self.domain_red_url_table )
		self.coupon_cur.execute( sql_update_domaintable )
		self.coupon_con.commit()
		print "UPDATE domain_store_table set is_new=0 where is_new=1 :)"
		print
		self.close_db()



if "__main__" == __name__:
	redurl = Red_domain()
	redurl.get_blacklist()
	redurl.get_whitetld_list()
	redurl.get_reviewed_blackdomain()

	redurl.handle_redurl()
	redurl.add_pending_data()
	redurl.handle_etsy_instagram()
	redurl.update_domain_titledes()