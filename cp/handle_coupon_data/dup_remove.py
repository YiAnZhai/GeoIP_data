# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config_couponapi

class Dup_remove():
	def __init__(self):
		self.DB_HOST = config_couponapi.DB_HOST
		self.DB_NAME = config_couponapi.DB_NAME
		self.DB_USER = config_couponapi.DB_USER
		self.DB_PSWD = config_couponapi.DB_PSWD
		self.CHARSET = config_couponapi.CHARSET

		# self.origin_table = "coupon_api"
		# self.fresh_table = "coupon_api_today_copy"
		# self.dup_remove_table = "coupon_api_chijiao"
		# self.fresh_dup_remove_table = "coupon_api_today_remdup"
		# self.fields = "brand, `code`, title, description, source, url, restriction, linkid, start_date, end_date, adddate, advertiserid, redirect_url, landing_page, url_status, category, keywords, htmlofdeal, promotiontypes, freeshipping, discount, guid, advertisername"

		self.origin_table = config_couponapi.api_base_origin_table
		self.fresh_table = config_couponapi.api_fresh_copy_table
		self.dup_remove_table = config_couponapi.api_dup_remove_table
		self.fresh_dup_remove_table = config_couponapi.api_fresh_dup_remove_table
		self.fields = config_couponapi.api_dup_remove_fields

		self.dealwith_basetable = 0
		self.topkeywords = {}

		self.split_reg = r"[\.\(\)\|\\\-\+\'\"_!,&:;/]"
		self.sub_reg = r"\."
		self.sub_reg1 = r"(\d+)\.00"
		self.sub_reg2 = r"(\d+)\.(\d+)"

		words = ['A','an', 'The', 'With', 'Of', 'In', 'On', 'At', 'For', 'valid', 'Coupon', 'coupons', 'Code', 'codes', 'Only', 'As', 'And', 'Or', 'Promo', 'Voucher', 'vouchers', 'when', 'While']
		self.stopWord = ''
		for aWord in words:
			aWord = r'\b' + aWord + r'\b'
			self.stopWord += aWord + '|'
		self.stopWord = self.stopWord[:-1]
		# 

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def get_word_count(self, allwords):
		self.keywords_dict = {}
		for keywords in allwords:
			keywords = re.sub(self.sub_reg1, "\g<1>", keywords)
			keywords = re.sub(self.sub_reg2, "\g<1>\g<2>", keywords)
			for keyword in re.subn(r"[\s]+", " ", re.subn(self.split_reg, " ", keywords.lower())[0])[0].split():
				if self.keywords_dict.has_key(keyword):
					self.keywords_dict[keyword] += 1
				else:
					self.keywords_dict[keyword] = 1

		self.sorted_list = sorted(self.keywords_dict.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
		if self.sorted_list:
			common_word_index = len(self.sorted_list) / 4
			if common_word_index > 100:
				common_word_index = 100

			self.ignore_index = self.sorted_list[common_word_index][1]
		else:
			# just take a place, that will used behind
			self.ignore_index = 100
		# print len(self.sorted_list)
		# for key in self.sorted_list[:100]:
		# 	print key

	def handle_dup(self):
		time_start = time.time()
		self.connect_db()
		sql_select_codecoupon = """SELECT %s, 0 as fresh from %s \
						UNION ALL SELECT %s, 1 as fresh from %s \
						ORDER BY brand, `code` desc, fresh desc, adddate desc, title """ % (self.fields, self.origin_table, self.fields, self.fresh_table)
		# sql_select_codecoupon = """select brand, `code`, title, description, source from %s  where `code` is not null and `code` <> '' order by brand, `code` desc, title """ % (self.origin_table)
		print sql_select_codecoupon
		num_select_codecoupon = self.coupon_cur.execute(sql_select_codecoupon)
		print num_select_codecoupon

		num_dupcode = 0
		num_dupcoupon = 0
		ids_dup = {}
		ids_dup_disjoint = {}
		self.coupons = []
		if num_select_codecoupon >= 1:
			coupons_all = self.coupon_cur.fetchall()

			#get common words
			text_array_to_filter = []
			for coupon in coupons_all:
				title_f = self.str_to_lower(coupon[2])
				description_f = self.str_to_lower(coupon[3])
				if "linkshare" == coupon[4]:
#					print description_f, 'yyyyyyyyyyyyyyy'
					description_f += " " + self.str_to_lower(coupon[6])
#					print description_f, 'nnnnnnnnnnnnnnn'
				text_array_to_filter += [[title_f, description_f]]

			self.get_word_count(["%s %s" % (ii[0], ii[1]) for ii in text_array_to_filter])

			self.coupons_common_words = ["" for i in range(len(coupons_all))]
			self.coupons_useful_words = ["" for i in range(len(coupons_all))]
			#-1 for dup_flag, -2 for common words
			self.coupons = [list(coupons_all[index]) + [self.handle_field_text(text_array_to_filter[index][0]), self.handle_field_text(text_array_to_filter[index][1]), self.handle_field_text("%s %s" % (text_array_to_filter[index][0], text_array_to_filter[index][1]), index), self.coupons_common_words[index], self.coupons_useful_words[index], ""] for index in range(0, len(coupons_all))]

#			print coupons_all[0], 'xxxxxxxxxx'
#			print self.coupons[0], 'ooooooooo'
#			for coupon in self.coupons:
#				print coupon[-2], 'yyyyyyyyyy'
#				print 0 == coupon[-2], 'nnnnnnnnnn'
			#calc top word to ignore
			#handle text to calc dup
			index, index_next = 0, 1
			coupon_cur = self.coupons[index]

			# print self.coupons[:10]
			while True:
				if not self.dealwith_basetable:
#					print type(coupon_cur[-7])
#					print coupon_cur[-7]
					while 0 == coupon_cur[-7]:
						if index_next >= num_select_codecoupon:
							break
						index += 1
						coupon_cur = self.coupons[index]
						index_next = index + 1
				if index_next >= num_select_codecoupon:
					break
				coupon_next = self.coupons[index_next]

				parent_value = index
				child_value = index_next
				while ids_dup_disjoint.has_key(child_value):
					child_value = ids_dup_disjoint[child_value]
				while ids_dup_disjoint.has_key(parent_value):
					parent_value = ids_dup_disjoint[parent_value]	

				if parent_value != child_value and coupon_cur[0] == coupon_next[0]:
					num_dupcode += 1
					if self.is_dup(coupon_cur, index, coupon_next, index_next):
						num_dupcoupon += 1

						if parent_value != child_value:
							parent_value, child_value = sorted([child_value, parent_value])
							ids_dup_disjoint[child_value] = parent_value

					index_next += 1
					if index_next >= num_select_codecoupon:
						index += 1
						coupon_cur = self.coupons[index]
						index_next = index + 1

				else:
					index += 1
					index_next = index + 1
					coupon_cur = self.coupons[index]
		print
		print
		# self.merge_dup(ids_dup)
		self.merge_disjoint(ids_dup_disjoint)
		print "number of compare times(same brand): ", num_dupcode
		print "number of same cps: ", num_dupcoupon
		print self.coupons
		if self.coupons:
			sql_insert_dupfilter = "INSERT INTO " + self.dup_remove_table + " (" + self.fields + ", fresh, common_words, useful_words, dup_flag) values (" + self.fields.count(",") * "%s," + "%s, %s, %s, %s, %s)"
			re_insert_all = self.execute_sql(self.coupon_con, self.coupon_cur, sql_insert_dupfilter, many="yes", items=[coupon[:-6] + coupon[-3:] for coupon in self.coupons])
		print "INSERT INTO dup_remove_table"

		self.parse_fresh()
		self.insert_into_base()
		self.del_temp_table()
		self.close_db()
		print "Take Time: ", int(time.time() - time_start)
	

	def parse_fresh(self):
		sql_parse_fresh = """INSERT INTO %s (%s, fresh, common_words, useful_words, dup_flag) SELECT %s, fresh, common_words, useful_words, dup_flag from %s where fresh=1 or dup_flag <> ''""" % (self.fresh_dup_remove_table, self.fields, self.fields, self.dup_remove_table)
		re_parse_all = self.execute_sql(self.coupon_con, self.coupon_cur, sql_parse_fresh)

	def insert_into_base(self):
		update_sql = "UPDATE %s as a inner join (SELECT id, dup_flag from %s where fresh=0) as b \
				on a.id=b.id inner join (select end_date, dup_flag from %s where fresh=1 and end_date is not null group by dup_flag ) as c \
				on b.dup_flag=c.dup_flag set a.update_date= str_to_date(c.end_date,'%s'), a.update_times=update_times+1, a.refresh_update=1" % (self.origin_table, self.fresh_dup_remove_table, \
					self.fresh_dup_remove_table,'%m/%d/%Y %H:%i')
		re_update_origin = self.execute_sql(self.coupon_con, self.coupon_cur, update_sql)

		sql_insert_to_base = """INSERT INTO %s (%s) SELECT %s from (SELECT %s, dup_flag from %s where dup_flag <>'' and dup_flag is not null and dup_flag not in (SELECT DISTINCT dup_flag from %s where fresh = 0) order by source desc) aa GROUP BY dup_flag \
				union \
				SELECT %s from %s where dup_flag = '' and fresh = 1""" % (self.origin_table, self.fields, self.fields, self.fields, self.fresh_dup_remove_table, self.fresh_dup_remove_table, self.fields, self.fresh_dup_remove_table)
		# print sql_insert_to_base
		re_insert_to_base = self.execute_sql(self.coupon_con, self.coupon_cur, sql_insert_to_base)

		sql_adapt_primarykey = "ALTER TABLE %s AUTO_INCREMENT = 1" % (self.origin_table)
		re_adapt_primarykey = self.execute_sql(self.coupon_con, self.coupon_cur, sql_adapt_primarykey)

	def del_temp_table(self):
		sql_del_chijiao = "truncate %s" % (self.dup_remove_table)
		re_del_chijiao = self.execute_sql(self.coupon_con, self.coupon_cur, sql_del_chijiao)

		sql_del_remdup = "truncate %s" % (self.fresh_dup_remove_table)
		re_del_chijiao = self.execute_sql(self.coupon_con, self.coupon_cur, sql_del_remdup)


	def execute_sql(self, con, cur, sql, many="", items=[]):
		try:
			if many:
				result = cur.executemany(sql, items)
			else:
				result = cur.execute(sql)
			con.commit()
			return result
		except Exception, e:
			print "\n*****Error_occured with sql:*****\n", """%s""" % sql
			print "*****Reason:*****\n","""%s""" % e ,"\n"
			exit()

	def merge_dup(self, ids_dup):
		result_array = []
		keys = ids_dup.keys()
		while True:
			if not keys:
				break
			id_dup = keys[0]
			result = self.merge_ita(ids_dup, id_dup, keys)
			for id_spec in result:
				self.coupons[id_spec][-1] = result[0]
			print result
			# print id_dup
			# print keys
	def merge_disjoint(self, ids_dup_disjoint):
		#######################        """merge disjoint"""
		results = {}
		for key in ids_dup_disjoint:
			child_set = set()
			while ids_dup_disjoint.has_key(key):
				child_set.add(key)
				key = ids_dup_disjoint[key]

			if results.has_key(key):
				results[key] = results[key].union(child_set)
			else:
				results[key] = child_set

		for result_dict in results:
			result = results[result_dict]
			result.add(result_dict)
			# print result
			for id_spec in result:
				self.coupons[id_spec][-1] = result_dict


	def merge_ita(self, ids_dup, id_index_cur, keys):
		ids_index = ids_dup[id_index_cur]
		result = ids_dup[id_index_cur] + [id_index_cur]
		keys.remove(id_index_cur)
		for id_index in ids_index:
			if id_index in keys:
				result = list(set(result).union(set(self.merge_ita(ids_dup, id_index, keys))))
			else:
				continue
		else:
			return result


	def is_dup(self, item1, index1, item2, index2):
		# the same coupon, the same linkid
		if item1[7] == item2[7] and item1[4] == item2[4]:
			return True
		# calc the same coupon by other coupon fields
		if self.is_dup_bytd((item1[-6], item1[-5] ,item1[-4], item1[1], item1[4]), (item2[-6], item2[-5], item2[-4], item2[1], item2[4])):
			if self.is_dup_byurl(item1[5], item2[5], item1[4], item2[4], item1[1], item2[1]) and self.is_dup_bysrc(item1[4], item2[4], item1[1], item2[1]):
				if self.is_dup_bydate(item1[8], item1[9], item2[8], item2[9]):
					return True

		return False

	def calc_persent(self, fz, fm):
		if 0 == fm:
			return 0
		else:
			return fz / float(fm)

	def handle_field_text(self, text, index=-1):
		str1 = self.str_to_lower(text)
		str1 = re.sub(self.sub_reg1, "\g<1>", str1)
		str1 = re.sub(self.sub_reg2, "\g<1>\g<2>", str1)
#		str1 = re.sub(self.sub_reg, "", str1)
		word_list = re.subn(r"[\s]+", " ", re.subn(self.split_reg, " ", str1)[0])[0].split()
		word_dict = self.mk_dict(word_list, index)
#		print word_dict, 'hahahahhahaha'
		return word_dict

	def mk_dict(self, array, index=-1):
		result_dict = {}
		for a in array:
			if (self.keywords_dict.has_key(a) and self.keywords_dict[a] < self.ignore_index  or re.subn("[0-9]", "", a)[1]) and not a.isdigit():
				weight = 1
				if not a.isalnum():
					weight = 3
				if result_dict.has_key(a):
					result_dict[a] += weight
				else:
					result_dict[a] = weight
				if -1 != index:
					self.coupons_useful_words[index] += "%s " % a
			else:
				if -1 != index:
					self.coupons_common_words[index] += "%s " % a
		return result_dict

	def formate_date(self, date):
		date_str = self.str_to_lower(date)
		pattern = re.compile(r"[\d]{2}/[\d]{2}/[\d]{4}")
		re_date = pattern.search(date_str)
		if re_date:
			return re_date.group(0)
		if date == "ongoing":
			return "12/12/2222"
		return "01/01/0001"


	def is_dup_bydate(self, date1_start, date1_end, date2_start, date2_end):
		d1_start = datetime.strptime(self.formate_date(date1_start), "%m/%d/%Y")
		d1_end = datetime.strptime(self.formate_date(date1_end), "%m/%d/%Y")
		d2_start = datetime.strptime(self.formate_date(date2_start), "%m/%d/%Y")
		d2_end = datetime.strptime(self.formate_date(date2_end), "%m/%d/%Y")
		if abs((d2_end - d1_end).days) < 2 and abs((d2_start - d1_start).days) < 2:
			return True
		elif d2_start >= d1_end or d1_start >= d2_end:
			return False
		else:
			# define some other possibility
			return True

	def is_dup_bytd(self, item1, item2):
		item1_titles_dict = item1[0]
		item1_descriptions_dict = item1[1]
		item1_dict = item1[2]
		code1 = item1[3]
		src1 = item1[4]
		item2_titles_dict = item2[0]
		item2_descriptions_dict = item2[1]
		item2_dict = item2[2]
		code2 = item2[3]
		src2 = item2[4]

		persent_join = self.jaccard_index(item1_dict, item2_dict)
		persent_t_t = self.jaccard_index(item1_titles_dict, item2_titles_dict)
		persent_d_d = self.jaccard_index(item1_descriptions_dict, item2_descriptions_dict)
		persent_t_d = self.jaccard_index(item1_titles_dict, item2_descriptions_dict)
		persent_d_t = self.jaccard_index(item1_descriptions_dict, item2_titles_dict)
		results = [persent_join, persent_t_t, persent_d_d, persent_t_d, persent_d_t]
		if src1 == src2:
			targets = [(0.6, 0.7), (0.7, 0.8), (0.7, 0.8), (0.7, 0.8), (0.7, 0.8)]
		else:
			targets = [(0.5, 0.6), (0.6, 0.7), (0.6, 0.7), (0.6, 0.7), (0.6, 0.7)]
		if self.jaccard_judge(targets, results):
			print persent_join, persent_t_t, persent_d_d, persent_t_d, persent_d_t
			return True
		return False

	def is_dup_bysrc(self, src1, src2, code1, code2):
		if code1 and code2:
			return True
		else:
			if "linkshare" == src1 and "linkshare" == src2 :
				return False
		return True

	def is_dup_byurl(self, url1, url2, src1, src2, code1, code2):
		if src1 == src2:
			if code1 and code2:
				if url1 and url2:
					url1_filter = url1.lower().split("//", 1)[-1].strip("/")
					url2_filter = url2.lower().split("//", 1)[-1].strip("/")
					if url1_filter == url2_filter:
						return True
					elif "/" not in url1_filter or "/" not in url2_filter:
						if url1_filter.split("?", 1)[0].split("/")[0] == url2_filter.split("?", 1)[0].split("/")[0]:
							return True
				else:
					return True
			else:
				if url1 and url2:
					url1_filter = url1.lower().split("//", 1)[-1].strip("/")
					url2_filter = url2.lower().split("//", 1)[-1].strip("/")
					if url1_filter == url2_filter:
						return True
				else:
					return False
		else:
			if code1 and code2:
				if url1 and url2:
					url1_filter = url1.lower().split("//", 1)[-1].split("?", 1)[0].strip("/")
					url2_filter = url2.lower().split("//", 1)[-1].split("?", 1)[0].strip("/")
					if url1_filter == url2_filter:
						return True
					elif "/" not in url1_filter or "/" not in url2_filter:
						if url1_filter.split("/")[0] == url2_filter.split("/")[0]:
							return True
				else:
					return True
			else:
				if url1 and url2:
					url1_filter = url1.lower().split("//", 1)[-1].split("?", 1)[0].strip("/")
					url2_filter = url2.lower().split("//", 1)[-1].split("?", 1)[0].strip("/")
					if url1_filter == url2_filter:
						return True
				else:
					return False
		return False

	def jaccard_judge(self, values, results):
		for index in range(len(values)):
			if values[index][0] <= results[index][0] or values[index][1] <= results[index][1]:
				return True
		return False

	def jaccard_index(self, dict1, dict2):
		result = (0.0, 0.0)
		if not (dict1 and dict2):
			return result
		num_union = sum(dict1.values()) + sum(dict2.values())
		# if num_union < 8:
		# 	return result
		num_intersection = 0
		for i1 in dict1:
			if dict2.has_key(i1):
				num_intersection += min(dict1[i1], dict2[i1])
		persent1 = float(num_intersection) / (num_union - num_intersection)
		persent2 = float(num_intersection) / min(sum(dict1.values()), sum(dict2.values()))
		result = (persent1, persent2)
		return result

	def str_to_lower(self, var):
		if type("") == type(var) or type(u"") == type(var):
			rePattern = self.stopWord
			var = re.sub(rePattern,'',var,flags = re.I)
			# replace multi spaces into one
			var = re.sub(r'[\s]+',' ',var)
			return var.lower()
		else:
			return ""


if '__main__' == __name__:
	sys.setrecursionlimit(15000)
	coupon_dup = Dup_remove()
	coupon_dup.handle_dup()
