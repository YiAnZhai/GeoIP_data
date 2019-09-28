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
		self.today_fresh_copy_table = config_couponapi.api_fresh_copy_table
		self.redirect_url_table = config_couponapi.api_redirect_url_table
		self.fresh_to_copy_fields = config_couponapi.api_fresh_to_copy_common_fileds


		self.special_sep_f = r"[:\s\'\"\(=]+"
		self.special_sep_e = r"[:\s\'\"\),\.;!]?"
		self.code_common_format  = r"(no)?\s*(use|using|with|through)?\s*(coupon|promo)?\s*\(*codes?%s([A-Z0-9\$\-]+)%s"
		self.reg_code = self.code_common_format % (self.special_sep_f, self.special_sep_e)

		self.spe_reg_code = self.code_common_format % (r",\s*\"*\'*", r"[\"\'\.,]+")

		self.regax_time = r"[\d]+[\/\-][\d]+([\/\-][\d]+)?"
		self.regax_time_phase = r"(valid|end|until)?.{,4}?%s.{,4}?\-%s%s" % (self.regax_time, self.regax_time, self.special_sep_e)


	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()
		self.coupon_cur.execute("SET NAMES utf8")

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def join_red_url(self):
		from_table = self.today_fresh_table
		to_table = self.today_fresh_copy_table
		redirecturl_table = self.redirect_url_table
		common_fields = self.fresh_to_copy_fields

		common_fields2 = "a.%s" % re.subn("," , ", a.", re.subn(",\s+", ",", common_fields)[0])[0]

		# sql = "INSERT INTO %s (%s, redirect_url, landing_page, url_status ) select %s, a.url, b.red_url, b.url_status from %s as a left join %s as b on (b.guid=MD5(a.url))" % (to_table, common_fields, common_fields2, from_table, redirecturl_table)
		# print sql

		sql_select = " SELECT %s, a.url, b.red_url, b.url_status from %s as a left join %s as b on (b.guid=MD5(a.url))" % (common_fields2, from_table, redirecturl_table)

		coupon_re = self.coupon_cur.execute(sql_select)
		coupons = [list(a) for a in self.coupon_cur.fetchall()]

		for cp in coupons:
			if not cp[5]:
				cp[5] = self.get_code("%s *-*-sep-*-* %s" % (cp[3], cp[4]))
			cp[3] = self.handle_text(cp[3])
			cp[4] = self.handle_text(cp[4])
			if not cp[4]:
				cp[4] = cp[3]
			else:
				if not cp[3]:
					if len(cp[4]) < 70:
						cp[3] = cp[4]
					else:
						cp[3] = self.get_title_fromd(cp[4])
			# print cp[5]
		sql_update = "INSERT INTO %s (%s, redirect_url, landing_page, url_status) values (%s)" % (to_table, common_fields, ",".join(["%s" for i in range(common_fields.count(",") + 4)]))
		print sql_update
		self.coupon_cur.executemany(sql_update, coupons)
		self.coupon_con.commit()

		# self.coupon_cur.execute(sql)
		# self.coupon_con.commit()

	def get_title_fromd(self, des):
		des_a = re.sub(r"\. |\!", r"\g<0>----", des)
		des_array = des_a.split("----")
		for d in des_array:
			if r"%" in d or r"$" in d:
				return d.strip()
		return des_array[0]

	def get_code(self, text):
		# text_a, num = re.subn("code", "CODE", text, flags=re.I)
		text_a = text
		result = ""
		pattern = re.compile(self.reg_code, re.I)
		code_re = pattern.search(HTMLParser().unescape(text_a))
		if code_re and not code_re.group(1):
			return code_re.group(4)
		else:
			pattern_2 = re.compile(self.spe_reg_code, re.I)
			code_re_2 = pattern_2.search(HTMLParser().unescape(text_a))
			if code_re_2 and not code_re_2.group(1):
				return code_re_2.group(4)
			return result

	def handle_text(self, text):
		# text_a, num = re.subn("code", "CODE", text, flags=re.I)
		text_a = text
		result = re.subn(self.reg_code, " ", HTMLParser().unescape(text_a), flags = re.I)[0]
		result = re.subn(self.spe_reg_code, " ", result, flags = re.I)[0]
		result = re.subn("%s%s%s" % (r"\([^\)]*", self.regax_time, r"[^\)]*\)"), " ", result, flags = re.I)[0]
		result = re.subn(self.regax_time_phase, " ", result, flags = re.I)[0]
		result = re.subn(r"\s+", " ", result.strip("- "), flags = re.I)[0]
		return result

if "__main__" == __name__:
	red_url = Join_redirect_url()
	red_url.connect_db()
	red_url.join_red_url()
	red_url.close_db()
