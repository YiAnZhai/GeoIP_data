# -*- coding: utf-8 -*-
import MySQLdb as mdb
import re
import sys
from HTMLParser import HTMLParser
from urlparse import urlparse
import datetime
import config

class Join_redirect_url():
	def __init__(self):
		self.couponinfo_table = config.couponinfo_table
                self.couponinfohandle_table = config.couponinfohandle_table
                self.couponinfo_common_fields = config.couponinfo_tohandled_common_fields

		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET

		self.special_sep_f = r"[:\s\'\"\(=]+"
		self.special_sep_e = r"[:\s\'\"\),\.;!]?"
		self.code_common_format  = r"(no)?\s*(use|using|with|through)?\s*(coupon|promo)?\s*codes?%s([A-Z0-9\$\-]+)%s"
		self.reg_code = self.code_common_format % (self.special_sep_f, self.special_sep_e)

		self.spe_reg_code = self.code_common_format % (r",\s*\"*\'*", r"[\"\'\.,]+")
#		self.ignore_brands = ["ebay.com", "amazon.com", "deals.ebay.com", "instagram.com", "couponslinks.com", "twitter.com", "facebook.com", "youtube.com", "google.com"]

	def connect_db(self):
		#self.coupon_con = mdb.connect(host='192.168.8.222', user='root', passwd='moma', db='coupon_datacenter', charset='utf8')
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()
		self.coupon_cur.execute("SET NAMES utf8")

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

#	def update_brand(self):
#		target_table = "coupon_twitter_today"
#		brand_table = "cj_sas_linkshare_brand"
#		sql_update = """UPDATE %s INNER JOIN %s ON %s.advertiserid = %s.advertiserid AND %s.source = %s.source
#			SET %s.brand = %s.brand
#			WHERE %s.advertisername is not null and %s.advertisername <> '' and %s.advertisername <> '*' """ % (target_table, brand_table, target_table, brand_table,
#				target_table, brand_table, target_table, brand_table, target_table, target_table, target_table)
#		self.coupon_cur.execute(sql_update)
#		self.coupon_con.commit()

	def join_red_url(self):
		from_table = self.couponinfo_table
		to_table = self.couponinfohandle_table
		common_fields = self.couponinfo_common_fields

		sql_select = "SELECT %s from %s" % (common_fields, from_table)

		coupon_re = self.coupon_cur.execute(sql_select)
		coupons = [list(a) for a in self.coupon_cur.fetchall()]

		print "after get data, start running"
		coupons_useful = []
		for cp in coupons[:]:
			code = self.get_code(cp[0])
			discount = self.get_discount(cp[0])
			clean_text = self.del_linkat(cp[0])
			expand_domain = self.get_brand(cp[3]).lower()
			freeshipping = self.check_freeshipping(cp[0])
			coupons_useful += [cp + [freeshipping, code, discount, clean_text, expand_domain]]
		sql_update = "INSERT INTO %s (%s,freeshipping, code, discount, clean_text, expand_domain) values (%s)" % (to_table, common_fields, ",".join(["%s" for i in range(common_fields.count(",") + 6)]))
		print sql_update
		self.coupon_cur.executemany(sql_update, coupons_useful)

		sql_update_guid = """UPDATE %s set guid = MD5( SUBSTRING_INDEX(expand_url, "****", 1) )""" % (to_table,)
		# self.coupon_cur.execute(sql_update_domain)
		self.coupon_cur.execute(sql_update_guid)
		self.coupon_con.commit()


		sql_delete_couponinfo_table = """truncate %s""" % (from_table)
		self.coupon_cur.execute(sql_delete_couponinfo_table)
		self.coupon_con.commit()

	def get_brand(self, url):
		# print url
		url = url.split("****")[0]
		try:
			domain = urlparse(url).netloc.strip()
		except Exception, e:
			print "get domain error: ", e
			domain = ""
		while domain.count(".") > 1 and domain.lower().startswith("ww"):
			domain = domain.split(".", 1)[-1]
		# if domain.count(".") > 2:
		# 	domain = ".".join(domain.split(".")[-3:])
		return domain


	def check_freeshipping(self, text):
		if "free ship" in text.lower():
			return 1
		return 0

	def get_code(self,meta_text):
	    """
	    created by hailing on 2017-05-04
	    main idea:
	    1.split a coupon text into small pieces by some KEYWORDS or URL or PREP or some adverbs
	    2.remove hashtags and @someone
	    3.get code from these splits using re rules
	    """
	    try:
		    # some accurate rules , especially for twitter data
		    prefixs = ['using','uses?','with','enter']
		    middle_rules = ['coupon','promo','discount','']
		    ellipses_signs = r"(?:\x85|\.{3}|\xe2\x80\xa6|\s)?"
		    code_rule = '[0-9A-Za-z\-]+'
		    re_code_string = ''
		    for a_pre in prefixs:
		        for a_mid in middle_rules:
		            re_code_string += r"%s[\s]*%s[\s]*code[\s\:]*%s[\s]*%s\b|"%(a_pre, a_mid, code_rule, ellipses_signs)
		    re_code_string = re_code_string[:-1]

		    # common rules
		    keywords = ['#?code[Ss]?','#?coupon[Ss]?','#?using','#?promo[\s]?code','#?discount[\s]?code','#?couponcode','#?promo[Ss]?','#?use[SsDd]?','#?vocher[Ss]?']
		    prep_words = ['at','on','in','for','from','by','to','and']
		    split_signs = ['&','\+',r'\n']
		    filter_words = ['necessary','needed','required','off','is','the']
		    num_patt = r"^[\-\"0-9\s]+$"
		    removes_patt = r'@[\S]+|#[\S]+|https?'
		    keywords_patt = '|'.join(r"\b%s\b"%word for word in keywords)
		    prep_patt = '|'.join(r"\b%s\b"%word for word in prep_words)
		    prep_patt += ''.join(['|%s'%word for word in split_signs])
		    url_patt = r"https?://[\d\-a-zA-Z]+\.[\S]*"
		    left_signs = r"(?:\*|\x91|\x92|\"|\'|\:|\:\s|\[)+"
		    right_signs = r"(?:\*|\x91|\x92|\"|\'|\)|\])+"
		    code_patt_with_lower = r"\b[a-zA-Z0-9\-\"]+[A-Z]{2,50}\b"+ellipses_signs
		    code_patt_only_upper = r"\b[A-Z0-9\-\"]{3,50}\b"+ellipses_signs
		    code_patt_match_all = r"[a-zA-Z0-9\-\"\_]{2,50}"+ellipses_signs
		    code_rules = [
		        r"(?:[\s]*|is|=)?%s%s%s"%(left_signs,code_patt_match_all,right_signs),
		        r"(?:[\s]*|is|=)?%s%s%s*$"%(left_signs,code_patt_match_all,right_signs[:-1]),
		        code_patt_only_upper,
		        code_patt_with_lower]
		    code_patt = '|'.join([ r"%s"%a_code_rule for a_code_rule in  code_rules])

		    # remove urls and get sentence before the first prep
		    url_splits_list = []
		    keywords_splits_list = []
		    url_splits_list = re.split(url_patt,meta_text)

		    #using the accurate rules 
		    pre_code_result = []
		    for a_sentence in url_splits_list:
		        pre_code_result += re.findall(re_code_string,a_sentence,re.I)
		    if len(pre_code_result) > 0:
		        formated_code = self.format_code(pre_code_result)
		        if formated_code != '':
		            return formated_code

		    #if accurate rule not found a code , then use common rules
		    for a_sentence in url_splits_list:
		        for a_split in re.split(keywords_patt,a_sentence,flags = re.I)[1:]:
		            a_split = re.sub(removes_patt,' ',a_split,flags = re.I)
		            a_split = re.split( prep_patt,a_split,flags = re.I)[0]
		            if a_split != '':
		                keywords_splits_list.append(a_split)

		    # extract code+
		    code_collection = []
		    for a_sentence in keywords_splits_list:
		        code_collection += re.findall(code_patt,a_sentence)
		    result_collection = []
		    for a_code in code_collection:
		        if not re.match(num_patt,a_code) and a_code.lower().replace(' ','') not in filter_words:
		            result_collection.append(a_code)

		    formated_code = self.format_code(result_collection)
		    return formated_code
	    except Exception as e:
	    	s=sys.exc_info()
	        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
	        print '=====> get_code Err', e
	        return ''

	def format_code(self,code_list):
	    num_patt = r"^[\-\"0-9\s]+$"
	    filter_words = ['at','on','in','for','from','by','to','and','necessary','needed','required','off','is','the']
	    if len(code_list) == 0:
	        return ''

	    formated_code_list = []
	    for a_code in code_list:
	        a_code = re.split('code[s]?',a_code,flags = re.I)[-1]
	        a_code = re.sub(r"^[\W]*",'',a_code)
	        a_code = re.sub(r"[\W]*$",'',a_code)
	        if not re.match(num_patt,a_code) and a_code.lower().replace(' ','') not in filter_words:
	            formated_code_list.append(a_code)

	    # chose the longest code
	    if len(formated_code_list) > 0:
	        formated_code_list.sort(key=lambda x:len(x))
	        return formated_code_list[-1]
	    return ''

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

	def get_discount(self,meta_txt):
	    """
	    createb by hailing on 2017-05-04
	    @para: A sentence
	    @return:
	        if there is a discount, return discount
	        else: return empty string
	    """
	    try:
		    billSign = ['\$','\€','\£','\xa3','\xe2\x82\xac']
		    pricePatt = '\s?[\$|\€|\£|\xa3|\xe2\x82\xac][\d\.\,]+\s?'
		    percentPatt = '\s?[\d\.]+\%\s?'
		    moneyPatt = '(?:%s|%s)'%(pricePatt,percentPatt)

		    beforeRuleList = ['%sback',
		        '%srebate',
		        '%sdiscount',
		        '%scash[\s]?back',
		        '%s[^\$]*(?:saved|savings|saving)',
		        '%s[^\$]*cut[^\$]*off',
		        '%s[^\$]*dollar[s]?\soff',
		        '%s[^\$]*extra[^\$]*off',
		        '%s[^\$]*off',
		        '%s[^\$]*(?:reduction|reduct|reducted|reducts)',
		        '%s[^\$]*(?:deduct|deducts|deduction|deducting|deducted)',
		        '%s[^\$]*coupon[s]?']

		    afterRuleList = ['Save more than%s',
		        'Discount[\s]?(?:of|at)?%s',
		        'save[^\$]*%s',
		        'Extra[^\$]*%s',
		        'Additional[^\$]*%s',
		        'savings are[^\$]*%s',
		        'saving is[^\$]*%s',
		        'Discount[^\$]*by[^\$]*%s',
		        'cut[^\$]*by[^\$]*%s']

		    extraRuleList = [
		    '(?:up to|over)%s'%percentPatt,
		    '(?:up to|over)%soff'%pricePatt,
		    ]
		    
		    discount = []
		    reString = '|'.join( [aRule%moneyPatt for aRule in beforeRuleList+afterRuleList] )
		    reString += '|'+'|'.join(extraRuleList)
		    # re搜索
		    collection = re.findall(reString,meta_txt,re.I)
		    # return str(collection)

		    resultPercent = []
		    resultPrice = []
		    result =[]
		    formatDiscount = ''
		    #re去除无用字符
		    for item in collection:
		        moneyRes = re.findall(moneyPatt,item)
		        for element in moneyRes:
		            if element.find('%') != -1:
		                resultPercent.append(element)
		            else:
		                resultPrice.append(element)

		    result = resultPercent
		    if len(resultPercent) == 0:
		        result = resultPrice

		    #获取最大值
		    if len(collection) == 0:
		        formatDiscount = ''
		    else:
		        formatDiscount = result[0]
		        for aDiscount in result:
		            maxnum = self.string_to_float(formatDiscount)
		            nowNum = self.string_to_float(aDiscount)
		            if  nowNum > maxnum:
		                formatDiscount = aDiscount
		    formatDiscount = re.sub('[\.]+|\.$','.',formatDiscount)
		    formatDiscount = re.sub('^\.+|\.+$','',formatDiscount)
		    formatDiscount = re.sub('[\,\s\!\?]','',formatDiscount)
		    return formatDiscount
	    except Exception as e:
	    	s=sys.exc_info()
	        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
	        print '=====> get_discount Err', e
	        return ''
	def string_to_float(self,string):
	    try:
	        string = re.sub('[\.]+|\.$','.',string)
	        string = re.sub('^\.+|\.+$','',string)
	        floatNum = float(filter(lambda ch: ch in '0123456789.', string))
	        return floatNum
	    except Exception, e:
	        return 0
	        s=sys.exc_info()
	        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
	        print '=====> string_to_float Err', e

	def del_linkat(self, text):
		result_dellink, num_dellink = re.subn(r"(http(s)?)[\S]+(\s)?", "", HTMLParser().unescape(text).encode('ascii','ignore'))
		result_delat, num_delat = re.subn(r"(RT |Via |on |in |with |at |by )?@[\S]+(:|\s)?", " ", result_dellink)
		# result_delat = re.sub(u"[=❤<>�♢♠◕♧♯»✔★]…", " ", result_delat.lstrip(u"*^+?\.[]^|)}~!/@£%&=`´;><: ❤�♢♠◕♧♯»✔★"))
		result_delat = re.sub(r"(#[^\s]+\s*)*$", "", result_delat)
		result_delat = re.sub(r" RT ", " ", result_delat)
		result_delat = re.sub(r"(\?{2,})|(_{2,})|(\-{2,})", " ", result_delat)
		result_delat = re.sub(r"\s+", " ", result_delat.strip())
		return result_delat

	def handle_text(self, text):
		# text_a, num = re.subn("code", "CODe", text, flags=re.I)
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
