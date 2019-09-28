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


def sep_word(text):
	words = re.findall("[\w]+", text)
	return words


class Red_domain_porn(Redurl):
	def __init__(self):
		super(Red_domain_porn,self).__init__()

		self.porn_words_all = [pw.lower() for pw in config.porn_words]


	def is_porn(self, text):
		text_words = sep_word( text )
		# words_set = set( text_words + [a[:-1] for a in text_words] + [a[:-2] for a in text_words] )
		words_set = set( text_words )
		for porn_word in self.porn_words_all:
			if 0 == porn_word.count(","):
				if porn_word in words_set:
					return porn_word
			else:
				for pw in porn_word.split(","):
					if pw not in words_set:
						break
				else:
					return porn_word
		return False


	def handle_redurl(self):
		self.connect_db()
		sql_select_url = "SELECT id, title, description, domain from %s where is_black=-1" % (self.domain_red_url_table, )
		self.coupon_cur.execute( sql_select_url )
		id_texts = self.coupon_cur.fetchall()

		id_update_fields = []
		porn_domains = []
		for id_text in id_texts:
			is_porn = self.is_porn( "%s %s" % ( id_text[1].lower(), id_text[2].lower() ) )

			if is_porn:
				id_update_fields.append( [1, is_porn, id_text[0]] )
				porn_domains.append( [is_porn, id_text[1], id_text[2], id_text[3]] )
			else:
				id_update_fields.append( [0, "", id_text[0]] )

		# print id_update_fields
		sql_update = """UPDATE %s set is_black=%s, black_reason=%s where id=%s""" \
			% ( self.domain_red_url_table, "%s", "%s", "%s" )

		self.coupon_cur.executemany( sql_update, id_update_fields )
		self.coupon_con.commit()
		print "update domain_get_redurl table PORN or not........"

		#############directly insert into blacklist
		sql_insert_black_list = """INSERT INTO %s (domain, `type`) values (%s)""" \
			% (self.negative_domain_table, ",".join(["%s"]*2) )
		self.coupon_cur.executemany( sql_insert_black_list, [[a[3], "porn_pending"] for a in porn_domains] )
		self.coupon_con.commit()


		#############insert into reviewing table
		# time1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		# sql_insert_reviewing_list = """INSERT INTO %s (domain, appear_time, `type`) values (%s)""" \
		# 	% (self.negative_domain_reviewing_table, ",".join(["%s"]*3) )
		# self.coupon_cur.executemany( sql_insert_reviewing_list, [[a[3], time1,"porn_pending"] for a in porn_domains] )
		# self.coupon_con.commit()

		self.close_db()
		print "domain_get_redurl table update success"
		print

	def init_dupbase(self):
		self.connect_db()
		sql_init_duobase_table = "TRUNCATE %s" % (self.dup_base_table, )

		self.coupon_cur.execute( sql_init_duobase_table )
		self.coupon_con.commit()

		self.close_db()
		print "init dupbase and begin insert data"
		print


if "__main__" == __name__:
	redurl = Red_domain_porn()

	redurl.handle_redurl()
	redurl.init_dupbase()