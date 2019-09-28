# -*- coding: utf-8 -*-
import hashlib
import time
from datetime import datetime
import MySQLdb as mdb
import re
import sys
import config
import os

class Dup_remove():
	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET

		# self.origin_table = "coupon_twitter"
		# self.inner_table = "coupon_twitter_innertwitter"
		# self.fresh_table = "coupon_twitter_today_copy"
		# self.dup_remove_table = "coupon_twitter_chijiao"
		# self.fresh_dup_remove_table = "coupon_twitter_today_remdup"
		# self.fields = "brand, `code`, title, description, source, url, restriction, linkid, start_date, end_date, adddate, advertiserid, redirect_url, landing_page, url_status,  category, keywords, htmlofdeal, promotiontypes, freeshipping, discount, guid, advertisername, retweet_count, favorite_count"		

		self.origin_table = config.coupon_twitter_table
		self.inner_table = config.coupon_twitter_table
		self.fresh_table = config.dup_base_table
		self.dup_remove_table = config.dup_after_all_table
		self.fresh_dup_remove_table = config.dup_remove_table
		self.fresh_dup_remove_backup_table = config.dup_remove_backup_table
		self.fields = config.dupremove_fields
		self.dup_back_fields = config.to_dup_remove_back_fields

		self.dealwith_basetable = 0 #######0 represent history data need not to compare each other
		self.topkeywords = {}

		self.split_reg = r"[\.\(\)\|\\\-\+\'\"\[\]_!,&:;/]"
		self.sub_reg = r"\."
		self.sub_reg1 = r"(\d+)\.00"
		self.sub_reg2 = r"(\d+)\.(\d+)"

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def get_word_count(self, allwords):
		self.keywords_dict = {}
		for keywords in allwords:
			# keywords = re.sub(self.sub_reg1, "\g<1>", keywords)
			# keywords = re.sub(self.sub_reg2, "\g<1>\g<2>", keywords)
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
			self.ignore_index = 0
		# print len(self.sorted_list)
		# for key in self.sorted_list[:100]:
		# 	print key

	def handle_dup(self):
		time_start = time.time()
		self.connect_db()
		# sql_select_codecoupon = """SELECT %s, 1 as fresh from %s \
		# 				group by title ORDER BY advertiserid, `code` desc, fresh desc, adddate desc, title """ % (self.fields, self.fresh_table)

		# sql_select_codecoupon = """SELECT %s, 1 as fresh from %s where landing_page <> "" and landing_page is not null \
		# 				ORDER BY brand, `code` desc, fresh desc, adddate desc, title """ % (self.fields, self.fresh_table)

		# sql_select_codecoupon = """SELECT %s, 1 as fresh from %s where landing_page <> "" and landing_page is not null \
		# 				group by title ORDER BY brand, `code` desc, fresh desc, adddate desc, title """ % (self.fields, self.fresh_table)

		domain_condition = r""" brand <> "" and brand is not null """
		sql_select_codecoupon = """SELECT %s, `id`, 0 as fresh from %s  where brand in (SELECT DISTINCT brand from %s where %s) \
						UNION ALL SELECT %s, `id`, 1 as fresh from %s where %s \
						ORDER BY brand, fresh desc, `code` desc, adddate desc, title """ % (self.fields, self.origin_table, self.fresh_table, domain_condition, self.fields, self.fresh_table, domain_condition)


		# sql_select_codecoupon = """SELECT %s, 0 as fresh from %s \
		# 				UNION ALL SELECT %s, 1 as fresh from %s where code <> '' and code is not null and url not like '%scoupon%s' and url not like '%sdisscount%s' and url not like '%scode%s' \
		# 				ORDER BY brand, `code` desc, fresh desc, adddate desc, title """ % (self.fields, self.origin_table, self.fields, self.fresh_table, "%", "%", "%", "%", "%", "%")

		print sql_select_codecoupon
		num_select_codecoupon = self.coupon_cur.execute(sql_select_codecoupon)
		print "number of tweet to dupfilter: ", num_select_codecoupon


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
				text_array_to_filter += [title_f]

			self.get_word_count([ii for ii in text_array_to_filter])
			
			self.coupons_common_words = ["" for i in range(len(coupons_all))]
			self.coupons_useful_words = ["" for i in range(len(coupons_all))]
			#-1 for dup_flag, -2 for common words
			self.coupons = [list(coupons_all[index]) + [self.handle_field_text(text_array_to_filter[index], index), self.coupons_common_words[index], self.coupons_useful_words[index], ""] for index in range(0, len(coupons_all))]
			
			# print self.coupons

			#calc top word to ignore
			#handle text to calc dup
			index, index_next = 0, 1
			coupon_cur = self.coupons[index]
			# print self.coupons[:10]
			while True:
				if not self.dealwith_basetable:
					# print type(coupon_cur[-5])
					# print coupon_cur[-5]
					while 0 == coupon_cur[-5]:
						if index_next >= num_select_codecoupon:
							break
						index += 1
						coupon_cur = self.coupons[index]
						index_next = index + 1
				if index_next >= num_select_codecoupon:
					break
				coupon_next = self.coupons[index_next]
				# if coupon_cur[11] == coupon_next[11]:

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

#						print coupon_cur
#						print coupon_next
						# print
						# print

					index_next += 1
					if index_next >= num_select_codecoupon:
						index += 1
						coupon_cur = self.coupons[index]
						index_next = index + 1
						
				else:
					index += 1
					index_next = index + 1
					coupon_cur = self.coupons[index]
					
				# print "index:", index
				# print index_next
#		print ids_dup
		print
		print
		# self.merge_dup(ids_dup)
		self.merge_disjoint(ids_dup_disjoint)
		print "number of compare times(same brand): ", num_dupcode
		print "number of same cps: ", num_dupcoupon
		if self.coupons:
			sql_insert_dupfilter = "INSERT INTO " + self.dup_remove_table + " (" + self.fields + ",id, fresh, common_words, useful_words, dup_flag) values (" + self.fields.count(",") * "%s," + "%s, %s, %s, %s, %s, %s)"
			re_insert_all = self.execute_sql(self.coupon_con, self.coupon_cur, sql_insert_dupfilter, many="yes", items=[coupon[:-4] + coupon[-3:] for coupon in self.coupons])
		print "INSERT INTO dup_remove_table-chijiao"

		self.parse_fresh()
		self.insert_into_base()
		self.del_temp_table()
		
		self.close_db()
		print "Take Time: ", int(time.time() - time_start)

		############ os.system("python init_today_table.py")

	def parse_fresh(self):
		sql_parse_fresh = """INSERT INTO %s (%s,id, fresh, common_words, useful_words, dup_flag) SELECT %s,id, fresh, common_words, useful_words, dup_flag from %s where fresh=1 or dup_flag <> ''""" % (self.fresh_dup_remove_table, self.fields, self.fields, self.dup_remove_table)
		re_parse_all = self.execute_sql(self.coupon_con, self.coupon_cur, sql_parse_fresh)
		print "INSERT INTO dup_remove_fresh_table-remdup"

	def insert_into_base(self):
		update_sql = """UPDATE %s as a inner join (SELECT id, dup_flag from %s where fresh=0) as b \
				on a.id=b.id inner join (select start_date, dup_flag from %s where fresh=1 group by dup_flag ) as c \
				on b.dup_flag=c.dup_flag set a.update_date=c.start_date, a.update_times=update_times+1, a.refresh_update=1""" % \
				(self.origin_table, self.fresh_dup_remove_table, self.fresh_dup_remove_table)
		re_update_origin = self.execute_sql(self.coupon_con, self.coupon_cur, update_sql)
		print "UPDATE base table"


		sql_insert_to_base = """INSERT INTO %s (%s) SELECT %s from (SELECT %s, fresh from (SELECT %s, fresh, dup_flag from %s where dup_flag <>'' and dup_flag is not null order by fresh) aa GROUP BY dup_flag having fresh=1) aaa\
				union \
				SELECT %s from %s where dup_flag = '' and fresh = 1""" % (self.inner_table, self.fields, self.fields, self.fields, self.fields, self.fresh_dup_remove_table, self.fields, self.fresh_dup_remove_table)
		# sql_insert_to_base = """INSERT INTO %s (%s) SELECT %s from (SELECT %s, dup_flag from %s where dup_flag <>'' and dup_flag is not null and dup_flag not in (SELECT DISTINCT dup_flag from %s where fresh = 0) order by source desc) aa GROUP BY dup_flag \
		# 		union \
		# 		SELECT %s from %s where dup_flag = '' and fresh = 1""" % (self.inner_table, self.fields, self.fields, self.fields, self.fresh_dup_remove_table, self.fresh_dup_remove_table, self.fields, self.fresh_dup_remove_table)
		# print sql_insert_to_base
		re_insert_to_base = self.execute_sql(self.coupon_con, self.coupon_cur, sql_insert_to_base)

		sql_adapt_primarykey = "ALTER TABLE %s AUTO_INCREMENT = 1" % (self.inner_table)
		re_adapt_primarykey = self.execute_sql(self.coupon_con, self.coupon_cur, sql_adapt_primarykey)
		print "INSERT INTO insert_into_base table"

	def del_temp_table(self):
		sql_backup_temp_table = "INSERT INTO %s (%s) SELECT %s FROM %s" % (self.fresh_dup_remove_backup_table, \
					self.dup_back_fields, self.dup_back_fields, self.fresh_dup_remove_table, )
		re_backup_dup = self.execute_sql(self.coupon_con, self.coupon_cur, sql_backup_temp_table)

		sql_del_chijiao = "truncate %s" % (self.dup_remove_table, )
		re_del_chijiao = self.execute_sql(self.coupon_con, self.coupon_cur, sql_del_chijiao)

		sql_del_remdup = "truncate %s" % (self.fresh_dup_remove_table, )
		re_del_chijiao = self.execute_sql(self.coupon_con, self.coupon_cur, sql_del_remdup)
		print "del_temp_table "


	def execute_sql(self, con, cur, sql, many="", items=[]):
		print sql
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


	def merge_dup(self, ids_dup):
		""""""
		result_array = []
		keys = ids_dup.keys()
		while True:
			if not keys:
				break
			id_dup = keys[0]
			# result = self.merge_ita(ids_dup, id_dup, keys)
			result = self.merge_for(ids_dup, id_dup, keys)
			for id_spec in result:
				self.coupons[id_spec][-1] = result[0]
			print result
			# print id_dup
			# print keys

	def merge_for(self, ids_dup, id_index_cur, keys):
		ids_index = ids_dup[id_index_cur]
		result = ids_dup[id_index_cur] + [id_index_cur]
		result_set = set(result)
		for result_index in result:
			if result_index in keys:
				keys.remove(result_index)
				extra_index = set(ids_dup[result_index]).difference(result_set)
				if extra_index:
					result += list(extra_index)
					result_set = set(result)
		return result


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
		# print "aaaaaaaaaaaaaaaaaaaaa"
		# the same coupon, the same linkid
		if item1[7] == item2[7] or item1[2] == item2[2]:
			return True
		#discount and code are the same
		if item1[1] != "" and item1[20]==item2[20] and item1[1]==item2[1]:
			return True

		# calc the same coupon by other coupon fields
		if self.is_dup_bytd(item1[-4], item2[-4], item1[20]==item2[20] or item1[1] != "" and item1[1]==item2[1]):
			# if self.is_dup_byurl(item1[5], item2[5]):
				return True

		if item1[2][:45] == item2[2][:45] or item1[3][:45] == item2[3][:45]:
			return True
		################ calc lss(longest common substring) 
		# lss_clean = self.lcs_dp(item1[2], item2[2])
		# lss_all = self.lcs_dp(item1[3], item2[3])
		# lss = max(lss_clean, lss_all)
		# if 45 < lss or lss_clean > 0.85 * min(len(item1[2]),len(item2[2])) or lss_all > 0.85 * min(len(item1[3]),len(item2[3])):
		# 	return True
			
		return False

	def calc_persent(self, fz, fm):
		if 0 == fm:
			return 0
		else:
			return fz / float(fm)

	def handle_field_text(self, text, index=-1):
		str1 = self.str_to_lower(text)
		# str1 = re.sub(self.sub_reg1, "\g<1>", str1)
		# str1 = re.sub(self.sub_reg2, "\g<1>\g<2>", str1)
#		str1 = re.sub(self.sub_reg, "", str1)
		word_list = re.subn(r"[\s]+", " ", re.subn(self.split_reg, " ", str1)[0])[0].split()
		word_dict = self.mk_dict(word_list, index)
#		print word_dict, 'hahahahhahaha'
		return word_dict

	def mk_dict(self, array, index=-1):
		result_dict = {}
		for a in array:
			if (self.keywords_dict.has_key(a) and self.keywords_dict[a] <= self.ignore_index  or re.subn("[0-9]", "", a)[1]) and not a.isdigit():
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


	def is_dup_bytd(self, item1, item2, likely=False):
		item1_titles_dict = item1
		item2_titles_dict = item2

		persent_t_t = self.jaccard_index(item1_titles_dict, item2_titles_dict)

		results = [persent_t_t]

		targets = [(0.7, 0.9)]
		if likely:
			min_len = max( min(len(item1_titles_dict), len(item2_titles_dict)), 3)
			mod_para = 0.05 * (min_len - 3)/min_len
			targets = [(0.65 - mod_para, 0.85 - mod_para)]

		if self.jaccard_judge(targets, results):
			# print persent_t_t
			return True

		return False


	def lcs_dp(self, input_x, input_y):
		# input_y as column, input_x as row
		dp = [([0] * len(input_y)) for i in range(len(input_x))]
		maxlen = maxindex = 0
		for i in range(0, len(input_x)):
			for j in range(0, len(input_y)):
				if input_x[i] == input_y[j]:
					if i!=0 and j!=0:
						dp[i][j] = dp[i - 1][j - 1] + 1
					if i == 0 or j == 0:
						dp[i][j] = 1
					if dp[i][j] > maxlen:
						maxlen = dp[i][j]
						maxindex = i + 1 - maxlen
		return maxlen
		# return input_x[maxindex:maxindex + maxlen]


	def is_dup_byurl(self, url1, url2):
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
			return var.lower()
		else:
			return ""


if '__main__' == __name__:
	sys.setrecursionlimit(15000)
	coupon_dup = Dup_remove()
	coupon_dup.handle_dup()
