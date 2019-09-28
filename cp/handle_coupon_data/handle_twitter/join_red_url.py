# -*- coding: utf-8 -*-
import MySQLdb as mdb
import re
from HTMLParser import HTMLParser
from urlparse import urlparse
import datetime

class Join_redirect_url():
	def __init__(self):
		self.special_sep_f = r"[:\s\'\"\(=]+"
		self.special_sep_e = r"[:\s\'\"\),\.;!]?"
		self.code_common_format  = r"(no)?\s*(use|using|with|through)?\s*(coupon|promo)?\s*codes?%s([A-Z0-9\$\-]+)%s"
		self.reg_code = self.code_common_format % (self.special_sep_f, self.special_sep_e)

		self.spe_reg_code = self.code_common_format % (r",\s*\"*\'*", r"[\"\'\.,]+")
		self.ignore_brands = ["ebay.com", "amazon.com", "deals.ebay.com", "instagram.com", "couponslinks.com", "twitter.com", "facebook.com", "youtube.com", "google.com", \
					"login.aliexpress.com", "t.co", "goo.gl"]

	def connect_db(self):
		self.coupon_con = mdb.connect(host='192.168.8.222', user='root', passwd='moma', db='coupon_datacenter', charset='utf8')
		self.coupon_cur = self.coupon_con.cursor()
		self.coupon_cur.execute("SET NAMES utf8")

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def update_brand(self):
		target_table = "coupon_twitter_today"
		brand_table = "cj_sas_linkshare_brand"
		sql_update = """UPDATE %s INNER JOIN %s ON %s.advertiserid = %s.advertiserid AND %s.source = %s.source
			SET %s.brand = %s.brand
			WHERE %s.advertisername is not null and %s.advertisername <> '' and %s.advertisername <> '*' """ % (target_table, brand_table, target_table, brand_table,
				target_table, brand_table, target_table, brand_table, target_table, target_table, target_table)
		self.coupon_cur.execute(sql_update)
		self.coupon_con.commit()

	def join_red_url(self):
		from_table = "coupon_twitter_today"
		to_table = "coupon_twitter_today_copy"
		redirecturl_table = "get_red_url"
		common_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, id, advertisername, linkid"

		common_fields2 = "a.%s" % re.subn("," , ", a.", re.subn(",\s+", ",", common_fields)[0])[0]

		# sql = "INSERT INTO %s (%s, redirect_url, landing_page, url_status ) select %s, a.url, b.red_url, b.url_status from %s as a left join %s as b on (b.guid=MD5(a.url))" % (to_table, common_fields, common_fields2, from_table, redirecturl_table)
		# print sql

		sql_select = " SELECT %s, a.url, b.red_url, b.url_status from %s as a left join %s as b on (b.guid=MD5(a.url))" % (common_fields2, from_table, redirecturl_table)

		coupon_re = self.coupon_cur.execute(sql_select)
		coupons = [list(a) for a in self.coupon_cur.fetchall()]

		coupons_useful = []
		for cp in coupons[:]:
			cp[2] = self.get_brand(cp[-2])
			if cp[2].lower() in self.ignore_brands:
				continue
			cp[3] = self.del_linkat(cp[3])
			cp[5] = self.get_code(cp[3])
			if cp[5]:
				cp[3] = re.sub(cp[5], "", cp[3])
			if not cp[3]:
				continue
			cp[4] = cp[3]
			cp[6] = self.get_expire_time(cp[13])
			cp[15] = self.get_discount(cp[3])
			coupons_useful += [cp]
		sql_update = "INSERT INTO %s (%s, redirect_url, landing_page, url_status) values (%s)" % (to_table, common_fields, ",".join(["%s" for i in range(common_fields.count(",") + 4)]))
		print sql_update
		self.coupon_cur.executemany(sql_update, coupons_useful)
		self.coupon_con.commit()

		# self.coupon_cur.execute(sql)
		# self.coupon_con.commit()

	def get_brand(self, url):
		# print url
		try:
			domain = urlparse(url).netloc.strip()
		except Exception, e:
			print "get domain error: ", e
			domain = ""
		if domain.lower().startswith("www."):
			domain = domain[4:]
		if domain.count(".") > 2:
			domain = ".".join(domain.split(".")[-3:])
		return domain

	def get_code(self, text):
		text_a, num = re.subn("code", "CODE", text, flags=re.I)
		text_a, num = re.subn("coupon", "COUPON", text_a, flags=re.I)
		result = ""
		pattern = re.compile(r"""(CODE|COUPON\s?'([\S]+)')|(CODE|COUPON\s?"([\S]+)")|(CODE:\s?([\S]+))|(CODE\s([0-9A-Z]+)([,\.!]|\s|$))|\"([0-9A-Z]+)\" """)
		code_re = pattern.search(text_a)
		if code_re:
			if code_re.group(2):
				return code_re.group(2).strip(",.:)(\'\"` \n")
			elif code_re.group(4):
				return code_re.group(4).strip(",.:)(\'\"` \n")
			elif code_re.group(6):
				return code_re.group(6).strip(",.:)(\'\"` \n")
			elif code_re.group(8):
				return code_re.group(8).strip(",.:)(\'\"` \n")
			elif code_re.group(10):
				return code_re.group(10).strip(",.:)(\'\"` \n")
		else:
			return result

	def get_expire_time(self, text):
		#add 30 days
		# print text
		try:
			time = datetime.datetime.strftime(datetime.datetime.strptime(str(text), "%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=30), "%m/%d/%Y")
			return time
		except Exception, e:
			print "get expire time error: ", e
		return ""

		# result = ""
		# pattern = re.compile(r"((expire)|(through)|(valid))[a-z]*[:\s]*([\S]+)", re.I)
		# expire_re = pattern.findall(text)
		# if expire_re:
		# 	result = self.get_priority_value(expire_re, {"expire":0, "through":1, "valid":2}, 4)
		# return result.strip(",.:)(\'\"` \n")

	def get_priority_value(self, values, priority, num):
		min_index = 100
		result = values[0][num]
		for value in values:
			if priority[value[0].lower()] < min_index:
				min_index = priority[value[0].lower()]
				result = value[num]
		return result

	def get_discount(self, text):
		result = ""
		pattern = re.compile(r"(\$?\s?[\d\.]{1,6}%?)\s(off|discount)", re.I)
		discount_re = pattern.findall(text)
		if discount_re:
			# result = ";".join([a[0].strip(",.:)(\'\"` \n") for a in discount_re])
			result = re.sub(r" ", "", discount_re[0][0].strip(",.:)(\'\"` \n"))
			if result.isdigit():
				result = ""
		return result

	def del_linkat(self, text):
		result_dellink, num_dellink = re.subn(r"(http(s)?)[\S]+(\s)?", "", HTMLParser().unescape(text))
		result_delat, num_delat = re.subn(r"(RT |Via |on |in |with |at |by )?@[\S]+(:|\s)?", " ", result_dellink)
		result_delat = re.sub("↺RT", "", result_delat)
		result_delat = re.sub(u"[=❤<>�♢♠◕♧♯»✔★↺♡☞]", " ", result_delat.lstrip(u"*^+?\.[]^|)}~!/@£%&=`´;><: ❤�♢♠◕♧♯»✔★"))
		result_delat = re.sub(r" RT ", " ", result_delat)
		result_delat = re.sub("Product Code.*?List", "List", result_delat)
		result_delat = re.sub(r"\((exp|end).*?\)" ,"", result_delat, flags=re.I)
		result_delat = re.sub("(exp\.?|expires?|end|ends) (by )?\d{1,2}/\d{1,2}(/\d{2,4})?", "", result_delat, flags = re.I)
		result_delat = re.sub(r"(\(\)|\[\])", "", result_delat)
		
		
		if result_delat.endswith(" via"):
			result_delat = result_delat[:-4]

		pattern = re.compile("#coupon|%|$|€|£|shipping|w/|bogo|promo ", re.I)
		flag = False
		if 'coupon' in result_delat and '?' not in result_delat:
			flag = True
		if flag or pattern.search(result_delat):
			result_delat = re.sub("(#\S+(\s+)?)+$", "", result_delat)	
			result_delat = re.sub("#", "", result_delat)

		result_delat = re.sub(r"\s+", " ", result_delat.strip())	
		result_delat = re.sub(r"\.\.\.", " ", result_delat)	
		
		result_delat = result_delat.strip().strip().strip('-: ')
		result_delat = re.sub(u"…", "", result_delat).strip()	
		return result_delat

	def handle_text(self, text):
		# text_a, num = re.subn("code", "CODE", text, flags=re.I)
		text_a = text
		result = re.subn(self.reg_code, " ", HTMLParser().unescape(text_a), flags = re.I)[0]
		result = re.subn(self.spe_reg_code, " ", result, flags = re.I)[0]
		result = re.subn(r"\s+", " ", result.strip("- "), flags = re.I)[0]
		return result

if "__main__" == __name__:
	red_url = Join_redirect_url()
	red_url.connect_db()
	# red_url.update_brand()
	red_url.join_red_url()
	red_url.close_db()
