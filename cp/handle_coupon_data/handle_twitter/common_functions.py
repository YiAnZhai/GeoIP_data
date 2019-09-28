# -*- coding: utf-8 -*-
import re
from HTMLParser import HTMLParser


def debug_error(error_str, reason=""):
    print "\n\n"
    print "!"*200
    print "error: %s" % reason
    print error_str
    print "!"*200
    print "\n\n"


#### for log
def time_period_spend(t1, t2, time_format, time_unit="Min"):
    unit_time = [("S",60), ("Min",60), ("H",60), ("D",24)]

    time_period = "%s -- %s" % (t1.strftime(time_format), t2.strftime(time_format))
    
    t_spend = (t2 - t1).total_seconds()
    for u_i in unit_time:
        if time_unit and time_unit != u_i[0] or not time_unit and t_spend >= u_i[1]:
            t_spend = t_spend / u_i[1]
        else:
            break
    time_spend = "%s %s" % (round(t_spend, 2), u_i[0])

    # time_spend = "%s Min" % round( total_seconds / 60, 2)
    return (time_period, time_spend)


def log_status( table, fields, data, msl ):
    msl.insert_data(table, fields, data)





def get_discount(meta_txt):
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
		            maxnum = string_to_float(formatDiscount)
		            nowNum = string_to_float(aDiscount)
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
def string_to_float(string):
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




def get_code(meta_text):
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
		        formated_code = format_code(pre_code_result)
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

		    formated_code = format_code(result_collection)
		    return formated_code
	    except Exception as e:
	    	s=sys.exc_info()
	        print "Error '%s' happened on line %d" % (s[1],s[2].tb_lineno)
	        print '=====> get_code Err', e
	        return ''

def format_code(code_list):
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


def check_freeshipping(text):
		if "free ship" in text.lower():
				return 1
		return 0


def del_linkat(text):
		result_dellink, num_dellink = re.subn(r"(http(s)?)[\S]+(\s)?", "", HTMLParser().unescape(text).encode('ascii','ignore'))
		result_delat, num_delat = re.subn(r"(RT |Via |on |in |with |at |by )?@[\S]+(:|\s)?", " ", result_dellink)
		# result_delat = re.sub(u"[=❤<>�♢♠◕♧♯»✔★]…", " ", result_delat.lstrip(u"*^+?\.[]^|)}~!/@£%&=`´;><: ❤�♢♠◕♧♯»✔★"))
		# print result_delat
		result_delat = re.sub(r"(#[^\s]+\s*)$", "", result_delat)
		result_delat = re.sub(r" RT ", " ", result_delat)
		result_delat = re.sub(r"(\?{2,})|(_{2,})|(\-{2,})", " ", result_delat)
		result_delat = re.sub(r"\s+", " ", result_delat.strip())
		return result_delat



def word_sep(tweet):
	emoticons_str = r"""
	    (?:
	        [:=;] # Eyes
	        [oO\-]? # Nose (optional)
	        [D\)\]\(\]/\\OpP] # Mouth
	    )"""

	regex_str = [
	    emoticons_str,
	    r'<[^>]+>', # HTML tags
	    r'(?:@[\w_]+)', # @-mentions
	    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
	    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

	    r'(?:[%\$])',
	#    r'(?:[%])',
	    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
	    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
	    r'(?:[\w_]+)', # other words
	    r'(?:\S)' # anything else
	]

	tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
	emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)

	result = tokens_re.findall(tweet)
	return result