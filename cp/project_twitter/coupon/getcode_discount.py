# -*- coding: utf-8 -*-
import MySQLdb as mdb
import re
from HTMLParser import HTMLParser
from urlparse import urlparse
import datetime
import config
from dateutil import parser
from identify_coupon_fresh import is_coupon, preprocess, stop
import math, time
from multiprocessing import Pool
from nltk import ngrams
import string
import random
import tldextract

global_debug_print = True

config_title_material = {
    ("number_cjdx_mfdeer", "%", "off"): 5,
    ("upto", "number_cjdx_mfdeer", "%"): 5,
    ("up", "to", "number_cjdx_mfdeer", "%"): 5,
    ("save", "number_cjdx_mfdeer", "%"): 5,
    ("extra", "number_cjdx_mfdeer", "%"): 5,
    ("$", "number_cjdx_mfdeer", "off"): 5,
    ("upto", "$","number_cjdx_mfdeer"): 5,
    ("up", "to", "$","number_cjdx_mfdeer"): 5,
    ("save", "$","number_cjdx_mfdeer"): 5,
    ("extra", "$","number_cjdx_mfdeer"): 5,
    (u"\u20ac", "number_cjdx_mfdeer", "off"): 5,
    ("upto", u"\u20ac","number_cjdx_mfdeer"): 5,
    ("up", "to", u"\u20ac","number_cjdx_mfdeer"): 5,
    ("save", u"\u20ac","number_cjdx_mfdeer"): 5,
    ("extra", u"\u20ac","number_cjdx_mfdeer"): 5,
    (u"\xe2\x82\xac", "number_cjdx_mfdeer", "off"): 5,
    ("upto", u"\xe2\x82\xac","number_cjdx_mfdeer"): 5,
    ("up", "to", u"\xe2\x82\xac","number_cjdx_mfdeer"): 5,
    ("save", u"\xe2\x82\xac","number_cjdx_mfdeer"): 5,
    ("extra", u"\xe2\x82\xac","number_cjdx_mfdeer"): 5,
    (u"\xa3", "number_cjdx_mfdeer", "off"): 5,
    ("upto", u"\xa3","number_cjdx_mfdeer"): 5,
    ("up", "to", u"\xa3","number_cjdx_mfdeer"): 5,
    ("save", u"\xa3","number_cjdx_mfdeer"): 5,
    ("extra", u"\xa3","number_cjdx_mfdeer"): 5,
    (u"\xc2\xa3", "number_cjdx_mfdeer", "off"): 5,
    ("upto", u"\xc2\xa3","number_cjdx_mfdeer"): 5,
    ("up", "to", u"\xc2\xa3","number_cjdx_mfdeer"): 5,
    ("save", u"\xc2\xa3","number_cjdx_mfdeer"): 5,
    ("extra", u"\xc2\xa3","number_cjdx_mfdeer"): 5,

    ("number_cjdx_mfdeer", "%"):3,
    ("$","number_cjdx_mfdeer"):3,
    (u"\u20ac","number_cjdx_mfdeer"):3,
    ("\xe2\x82\xac","number_cjdx_mfdeer"):3,
    (u"\xa3","number_cjdx_mfdeer"):3,
    (u"\xc2\xa3","number_cjdx_mfdeer"):3,

    ("free", "shipping"): 3,
    ("free", "delivery"): 3,
    ("free", "postage"): 3,
    ("freeshipping"): 3,
    ("freedeliver"): 3,
    ("freepostage"): 3,

    ("first", "order"):1,
    ("first", "purchase"):1,
    ("all", "purchases"):1,
    ("use", "code"): 1,
    ("promo", "code"): 0.3,
    ("use", "promo"): 1,
    ("coupon", "code"): 0.3,
    ("with", "code"): 1,
    ("at", "checkout"):0.3,
    ("up", "to"):1,
    ("off",):0.3,
    ("$",):1,
    ("%",):1,
    (u"\u20ac",):1,
    (u"\xe2\x82\xac",):1,
    (u"\xa3",):1.5,
    (u"\xc2\xa3",):1.5,
    ("discount",):0.3,
    ("code",):0.3,
    ("promo",):0.3,
    ("shipping",):0.3,
    ("coupon",):0.3,
    ("upto",):0.3,
    ("free",):0.3,
    ("order",):0.3,
    ("save",):0.3,
    ("sale",):0.3,
    ("using",):0.3,
    ("extra",):0.3,
    ("checkout",):0.3,
    ("price",):0.3,
    ("today",):0.3,
    ("only",):0.3,
    ("all",):0.3,
}

self_strip_words = re.sub( r"%|\$", "", string.punctuation + " \t\n\r " )

##common regex of discount
self_discount_symbol = u"(\%|\$|\u20ac|\xe2\x82\xac|\xa3|\xc2\xa3|percent|usd|dollar|us dollar)"
self_discount_content = r"[\d]+([\.\-\,]?[\d]+)?"
self_discount_format = r"%s[ ]*%s|%s[ ]*%s" % (self_discount_symbol, self_discount_content, self_discount_content, self_discount_symbol)
##common regex of price
self_price_symbol = u"(\$|\u20ac|\xe2\x82\xac|\xa3|\xc2\xa3|usd|dollar|us dollar)"
self_price_content = r"[\d]+([\.\-\,]?[\d]+)?"
self_price_format = r"%s[ ]*%s|%s[ ]*%s" % (self_price_symbol, self_price_content, self_price_content, self_price_symbol)

def coupon_extract_run( cp ):
    try:
        t1 = time.time()
        full_text = cp[2] if cp[2] else cp[1]
        expand_url = cp[5]
        extend_tweet_expand_url = cp[8]
        user_url = cp[11]
        user_name = cp[12]
        user_screen_name = cp[13]

        choosed_url = choose_url( expand_url, extend_tweet_expand_url, user_url, user_name, user_screen_name )
        full_text = text_fix( full_text )
        expand_url = cp[5]
        extend_tweet_expand_url = cp[8]
        create_time_format = datetime.datetime.strptime(cp[-4], "%Y-%m-%d %H:%M:%S")
        code, full_text = get_code( full_text )
        discount, origin_disc = get_discount( full_text )
        price = get_price( full_text, origin_disc )
        coupon_requirement = get_coupon_requirement( full_text )
        t2 = time.time()
        coupon_start_date, coupon_end_date, backup_endsdate = get_expire_time( full_text, today=create_time_format )
        t3 = time.time()
        coupon_start_date = format_date(full_text, coupon_start_date, today=create_time_format)
        coupon_end_date = format_date(full_text, coupon_end_date, today=create_time_format)
        if not coupon_end_date:
            for t in backup_endsdate:
                coupon_end_date = format_date( full_text, t, today=create_time_format )
                if coupon_end_date:break
        if global_debug_print:print coupon_end_date
        expand_domain = get_brand( expand_url ).lower()
        extend_tweet_expand_domain = get_brand( extend_tweet_expand_url ).lower()
        freeshipping = check_freeshipping( full_text )
        # show_title = get_show_title( full_text )
        title_info, show_title = get_title( full_text, origin_disc, discount )
        show_description = get_description( full_text )
        if not is_like_title( show_title ):
            # show_title = "not like--%s" % (show_title)
            show_title_2 = gen_template_title(discount, code, freeshipping, price, coupon_requirement)
            show_title = show_title_2
            # show_title = "gen--%s" % (show_title_2, )

        # clean_text = clean_description( full_text )
        # title_s1,title_s2 = title( full_text, origin_disc, discount )
        # clean_text,title_s1,title_s2 = "", "", ""
        t4 = time.time()
        return cp + [choosed_url, freeshipping, code, discount, price, \
                title_info, show_title, show_description,  expand_domain, \
                extend_tweet_expand_domain, coupon_requirement, coupon_start_date, coupon_end_date]
    except Exception,e:
        return []


#
# must return unicode, fixed for the usage later
#
def text_fix( text ):
    text_a, num = re.subn( u'\u201d|\u201c', "\"", text, flags=re.I )
    text_a, num = re.subn( u"\u2018|\u2019", "\'", text_a, flags=re.I )
    text_a, num = re.subn( u"\u2013", "-", text_a, flags=re.I )
    text_a, num = re.subn( u"\uff1a", ":", text_a, flags=re.I )
    text_a, num = re.subn( u"\xa0", " ", text_a, flags=re.I )
    ## some special money unit
    list_aa = [(u"\u20ac", u"cjdx001_mfdeer"), (u"\xe2\x82\xac", u"cjdx002_mfdeer"),(u"\xa3", u"cjdx003_mfdeer"),(u"\xc2\xa3", u"cjdx004_mfdeer")]
    for l in list_aa:
        text_a, num = re.subn( l[0], l[1], text_a, flags=re.I )
    text_a = HTMLParser().unescape(text_a).encode('ascii','ignore').decode()
    for l in list_aa:
        text_a, num = re.subn( l[1], l[0], text_a, flags=re.I )
    return text_a


def choose_url( expand_url, extend_tweet_expand_url, user_url, user_name, user_screen_name ):
    # expand_url, extend_tweet_expand_url, user_url = expand_url.lower(), extend_tweet_expand_url.lower(), user_url.lower()
    url_list = [extend_tweet_expand_url.split("****"), expand_url.split("****"), ]#user_url.split("****")]
    result_url_infos, result_urls = [], []
    for u_list in url_list:
        for url in u_list:
            extract_domain = tldextract.extract(url)
            root_domain = extract_domain.domain.lower()
            registered_domain = extract_domain.registered_domain.lower()

            if not url.lower().startswith("http"):continue
            sub, domain, suffix = extract_domain
            if not suffix or not domain:continue
            if suffix.lower() in ("com","net","co.uk",) or url.lower().startswith("https") or url.count("/")>2:
                pass
            elif domain.isdigit() and ( not sub or sub.isdigit() ):continue
            elif domain.isdigit() and suffix.lower() in ("am","pm","ml","km"):continue

            ##lnkd.in neg, can jump to other sites
            if url and not registered_domain in ("twitter.com", "instagram.com", "facebook.com", 
                "youtu.be", "youtube.com", "linkedin.com", "reddit.com", "pinterest.com", "tumblr.com", 
                "mailchi.mp", "linktr.ee", "wordpress.com", "twitch.tv", "etsy.com", "etsy.me", 
                "talkable.com", "chat.whatsapp.com") \
                and not "blogspot" in root_domain \
                and not "coupon" in root_domain and not "promocode" in root_domain and not "discount" in root_domain \
                and not "saving" in root_domain and not "vouchercode" in root_domain:
                if url not in result_urls:
                    result_urls.append( url )
                    result_url_infos.append( (root_domain, registered_domain) )
    if global_debug_print:
        print result_url_infos
    ## if only one domain, choose the longest one
    if(1==len(set(result_url_infos))):
        result_urls = [max( result_urls, key=len ),]
    if len(result_urls) > 1:
        final_urls = []
        for i,url in enumerate(result_urls):
            root_domain, registered_domain = result_url_infos[i]
            if not "promo" in root_domain and not "deal" in root_domain and not "voucher" in root_domain and \
                ( suffix and suffix[0].islower() and len(domain) > 2 or suffix.lower() in ("com","net","co.uk") or \
                    url.lower().startswith("https") or url.count("/")>2 or "www" in url.lower() ) :
                final_urls.append( url )
        if final_urls:
            result_urls = final_urls

    return "****".join( result_urls[:1] )


def get_code( text ):
    # """noise: """ "Code: CD0461-401" "CI1184-146" "DB3328"

    if re.search(r"no ([ a-zA-Z]{0,9})?(code|coupon) (need|require)", text, re.I):
        return "", text
    text_a = text
    text_b, num = re.subn("code", "CODe", text_a, flags=re.I)
    text_b, num = re.subn(r"\buse", "USe", text_b, flags=re.I)
    text_b, num = re.subn(r"\busing", "USIng", text_b, flags=re.I)
    text_b, num = re.subn("coupon", "COUPOn", text_b, flags=re.I)
    text_b, num = re.subn(" promo ", " PROMo ", text_b, flags=re.I)
    if global_debug_print:
        print text_a
        print text_b
    result = ""
    char_num_code = r"[0-9]+[A-Za-z\-]+[0-9A-Za-z]*|[A-Za-z]+[0-9\-]+[0-9A-Za-z]*"
    char_num_code_just = r"[0-9]+[A-Za-z]+[0-9A-Za-z]*|[A-Za-z]+[0-9]+[0-9A-Za-z]*"
    upcasechar_num_code = r"[0-9]+[A-Z]+[0-9A-Z]*|[A-Z]+[0-9]+[0-9A-Z]*"
    after_prep_words = r"(for|as|at|during|to|and|or|on|when)"
    after_biaodian = r"\,\.!\)\]"
    regex_str = [
        r"(CODe|\bUSe|\bUSIng|COUPOns?)[\,;\:\s\(\[\'\"]+'(?P<code1>\s*[\S]+\s*)'",
        r'(CODe|\bUSe|\bUSIng|COUPOns?)[\,;\:\s\(\[\'\"]+"(?P<code2>\s*[\S]+\s*)"',
        r"CODe[\,; ]?\:[\s\(\[\'\"]*(?P<code3>[\S]+)",
        r"CODe[\,;\:#\s\(\[\'\"]+(?P<code4>[0-9A-Z]+[\-]?[0-9A-Z]+)([%s\s\:]|$)" % (after_biaodian, ),
        r'"(?P<code5>[0-9A-Z]+)"',
        r'"(?P<code6>%s)"' % (char_num_code_just, ),
        r"'(?P<code7>%s)'" % (char_num_code_just, ),
        r"\bUSe[SsDd]?[: \'\"]+(?P<code8>[0-9A-Z]+)([%s\s]|$)" % (after_biaodian,),
        r"\bUSIng[: \'\"]+(?P<code9>[0-9A-Z]+)([,\.!\s]|$)",
        r"(USe|USIng|with|nter| w) ([a-z]{0,10}[ ]?){0,3}CODe\b[ ]*(?P<code10>[^,\.!\s]+)(([%s\s]|$)+%s|\n)\s" % (after_biaodian, after_prep_words, ),
        r"(enter|Enter|ENTER) (.{0,9}CODe\s+)?(?P<code12>[\S]+)( +%s|\n)\s" % (after_prep_words, ),
        r"COUPOns?[ ]?[\:\(\[\'\"]+[\s]*(?P<code15>[\S]+)( +%s|\n|$)" % (after_prep_words, ),
        r"COUPOns?[ ]?[\,;\:\(\[\'\"]+[\s]*(?P<code15>[\S]+)( +%s|\n)\s" % (after_prep_words, ),
        r"COUPOns?[ ]?[\:\(\[\'\"]+[\s]*(?P<code16>%s)\s" % (char_num_code, ),
        r"COUPOns?[ ]?[\,;\:\s\(\[\'\"]+(?P<code11>[0-9A-Z]+)([%s\s]|$)" % (after_biaodian,),

        r"(USe|USIng|with|nter| w) ([a-zA-Z]{0,10}[ ]?){0,3}CODe\b[ ]*(?P<code17>%s|[A-Za-z0-9]+)[\s]*(([%s]|$)+|\n)" % (char_num_code, after_biaodian),
        r"CODe at check ?out[\.\:\,; ]+(?P<code14>%s)" % (char_num_code, ),
        r"CODe[\,;\:#\s\(\[\'\"\-]+(?P<code13>%s)(([%s\s]|$)+|\n)" % (char_num_code, after_biaodian),
    ]
    for r in regex_str:
        if "A" not in r:
            single_code_re = re.search( r, text_a, re.I )
        else:
            single_code_re = re.search( r, text_b )
        if single_code_re:
            code_group = single_code_re.groupdict()
            for k in sorted( code_group.keys() ):
                if global_debug_print:print code_group[k]
                if code_group[k]:
                    # if "vapingtruth" in code_group[k]:
                    #     print code_group
                    if re.search( r"(style|item|sku|upc|product|dress|jewelry)\s+code|sku:", get_before_text( text_a, code_group[k], 15 ), re.I ):
                        continue
                    if re.search( r"^\d+[\,\d]*%$|^\$\d+[\,\d]*$", code_group[k] ):
                        continue
                    result = code_group[k]
                    break
        if result:
            break
    # print result, "a"*100

    if not result:
        words = text_a.split()
        code_like_count = 0
        code_like = ""
        for i in range(len(words)):
            w = words[i]
            p_w = w.strip(",.:?!)(*-[]\'\"` \n").encode('ascii','ignore')
            if len(p_w)>2 and re.match( r"[0-9]+[A-Z]+[0-9A-Z]*$|[A-Z]+[0-9]+[0-9A-Z]*$", p_w ):
            # if re.match( r"[0-9]+[A-Za-z\-]+[0-9A-Za-z\-]*$|[A-Za-z]+[0-9\-]+[0-9A-Za-z\-]*$", p_w ):
                f_i = i-2 if i >=2 else 0
                for back_w in words[f_i:i+3]:
                    back_p_w = back_w.strip(",.:?!)(*-[]\'\"` \n").encode('ascii','ignore')
                    if back_p_w.lower() in set(["off","with","w","use","using","enter","code","check","checkout","coupon","discount","inputting"]):
                        break
                else:
                    continue
                # print p_w
                code_like_count += 1
                code_like = p_w
        if 1 == code_like_count:
            result = code_like

    ## test new rules
    # regex_str_add = [
        
    # ]
    # for r in regex_str_add:
    #     if "A" not in r:
    #         single_code_re = re.search( r, text_a, re.I )
    #     else:
    #         single_code_re = re.search( r, text_b )
    #     if single_code_re:
    #         code_group = single_code_re.groupdict()
    #         for k in sorted( code_group.keys() ):
    #             print code_group[k]
    #             if code_group[k]:
    #                 result = "aaaaa-%s" % (code_group[k],)
    #                 break
    #     if result:
    #         break

    # print result, "a"*100

    edge_symbol = ",.:?!)(*-#[]\'\"` \n<>\{\}\|_"
    result_clean = result.encode('ascii','ignore').strip( edge_symbol )
    result_clean = result_clean.split("?")[0]
    result_clean = result_clean.split("..")[0].strip( edge_symbol )
    # if 0<len(result_clean) < 3:result_clean = "aaaa-%s" % (result_clean, )
    if "http" in result_clean or len(result_clean) < 2 or re.match(r"\d+(GB|TB|AM|PM)$", result_clean):result_clean = ""
    if "lyft" == result_clean.lower():result_clean = ""
    if 2 == len(result_clean):
        text_bbb = text_a.replace(result,"cjdx_code_plhd")
        if not re.search( r"code\s*(\:\s+|\'|\")cjdx_code_plhd|code\s+cjdx_code_plhd(\.|\s+(for|and|at|to)\b)", text_bbb, re.I ):
            result_clean = ""

    if result_clean.isdigit() and len(result_clean) < 4:result_clean = ""
    if result_clean:
        text = text.replace( result, "cjdx_code_mfdeer" )
        text = re.sub( r"\bcode\b.{0,3}[\?]*.{0,3}cjdx_code_mfdeer('|\")?", " code ", text, flags=re.I|re.S )
        text = re.sub( r"(use|with|using|enter)['\"\: ]+cjdx_code_mfdeer['\"]*", r"\1 code", text, flags=re.I )
        # text = re.sub( r"(use|with|using|enter)(['\"\: ]+)cjdx_code_mfdeer(\.|\s+(for|and|at|to|in|as|until|during)\b)", r"\1 code\3", text, flags=re.I )
        # text = re.sub( r"(use|with|using)(['\"\: ]+)cjdx_code_mfdeer(\.|\s+(for|and|at|to)\b)", r"\1 code \2", text, flags=re.I )
        text = re.sub( r"['\"\: \[\(]*cjdx_code_mfdeer[\'\"\]\.\)$]*", "", text, flags=re.I )
    return result_clean, text
#       result = result.strip(",.:)(\'\"` \n").encode('ascii','ignore')
#       if result.isdigit():
#           result = ""
#       return result

def get_discount( text ):
    text = re.sub(r"free.{0,15}ship|free.{0,15}deliv|free.{0,15}postage|ship.{0,15}free|deliv.{0,15}free|postage.{0,15}free", "", text, flags=re.I)
    result = ""

    # discount_format = r"\$?[ ]*[\d]+([\.\-\,]?[\d]+)?[ ]*\%?"
    
    attach_word = r"^\d\$\%\n\(\+"
    regex_str = [
        r"(?P<discount1>%s)[ ]*(off|discount|free|coupon|rebate|back|credit|promo|gift)" % (self_discount_format, ),
        r"(?P<discount2>%s)[ ]*[%s]{0,10}[ ]?(off|discount|free|rebate|back|credit|gift)" % (self_discount_format, attach_word, ),
        # r"(save|up\s*to|win|code for|coupon for|discount)\s*(?P<discount3>\$?[ ]?[\d\.]{1,6}%?)", # not sale
        r"(save|up\s*to|win|code for|coupon for|discount|free|get|extra|earn)[ ]*(?P<discount3>%s)" % (self_discount_format, ), # not sale
        r"(save|up\s*to|win|code for|coupon for|discount|get|extra|earn)[ ]*[%s]{0,5}[ ]*(?P<discount3>%s)" % (attach_word, self_discount_format, ), # not sale
        r"(?P<discount2>%s)[ ]*bonus" % (self_discount_format, ),
    ]
    for r in regex_str:
        single_discount_re = re.search( r, text, re.I )
        if single_discount_re:
            discount_group = single_discount_re.groupdict()
            for k in sorted( discount_group.keys() ):
                if discount_group[k]:
                    # if "5807" in discount_group[k]:
                    #     print discount_group
                    result = discount_group[k]
                    break
        if result:
            break
    origin_disc = result
    # pattern = re.compile(r"(\$?\s?[\d\.]{1,6}%?)\s(off|discount|free|rebate)", re.I)
    # # pattern = re.compile(r"((\$?|\u20ac|\xa3|\xc2\xa3)\s*[\d][\d\.\-]*\s*%?)\s[a-zA-Z]{0,10}[\s]?(off|discount|free|rebate|back)", re.I)
    # discount_re = pattern.findall(text)
    # if discount_re:
    #     # result = ";".join([a[0].strip(",.:)(\'\"` \n") for a in discount_re])
    #     result = discount_re[0][0]
    # else:
    #     pattern2 = re.compile(r"(save|up\s*to)\s*(\$?\s?[\d\.]{1,6}%?)", re.I)
    #     discount_re2 = pattern2.findall(text)
    #     if discount_re2:
    #         result = discount_re2[0][1]
    re_symbol_map = {r"usd|dollar|us dollar":"$", r"percent":"%"}
    for k,v in re_symbol_map.items():
        result = re.sub(k, v, result, flags=re.I)
    result = format_discount( result )
    result = re.sub(r" ", "", result.strip(",.:)(\'\"` \n"))
    if is_illegal_discount(result):
        result = ""
    # if result.isdigit():
    #     result = ""
    return result, origin_disc

def get_price( text, discount="" ):
    result = ""
    # text, num = re.subn( u"\uff1a", ":", text, flags=re.I)

    attach_word = r"^\d\$\%\n\(\+"
    regex_str = [
        r"(only|only:|sale for|deal price:|discount price:|purchase at|purchase for|retail at|now|for|just|order)[ ]*(?P<price1>%s)" % (self_price_format, ),
        r"(only|only:|sale|deal price|discount price|purchase|retail at|now|for|just|order)[ ]*[%s]{0,7}(?P<price2>%s)" % (attach_word, self_price_format, ),
        r"price:[ ]*(?P<price6>%s)" % (self_price_format, ),
        r"(?P<price3>%s).{1,5}(%s)[ ]*off" % (self_price_format, self_discount_format),
        r"(%s)[ ]*off.{1,5}(?P<price4>%s)" % (self_discount_format, self_price_format),
        r"(?P<price5>%s)[ ]*\+[ ]*order" % (self_price_format, )
    ]
    for r in regex_str:
        single_price_re = re.search(r, text, re.I )
        if single_price_re:
            # if "71.97" in single_price_re.group():
            #     print r
            #     print single_price_re.group()
            #     print single_price_re.groupdict()
            price_group = single_price_re.groupdict()
            for k in sorted( price_group.keys() ):
                if price_group[k] and discount != price_group[k]:
                    # if "71.97" in price_group[k]:
                    #     print price_group
                    result = price_group[k]
                    break
        if result:
            break
    re_symbol_map = {r"usd|dollar|us dollar":"$"}
    for k,v in re_symbol_map.items():
        result = re.sub(k, v, result, flags=re.I)
    result = format_discount( result )
    # result = re.sub(r" ", "", result.strip(",.:)(\'\"` \n"))
    
    return result

def get_coupon_requirement( text ):
    result = []
    requirements = {
        "No Code Needed":[
            r"without\s+(promo)?code|no\s+code\s+(need|require)"
        ],
        "Today Only":[
            r"today only|only today|today.{0,8}last|just\s+today"
        ],
        # "daily deal":[
        #     r"daily deal", r"season sale"
        # ],
        # "limited":[
        #     r"limited",####################
        # ],
        "Limited Time":[
            r"limited time",
        ],
        "Limited Stock":[
            r"limited stock",r"limited number",
        ],
        "Clearance":[
            r"clearance",
        ],
        "Sitewide":[
            r"site[ \-]?wide", r"(all|any|every) (item|order)", "everything",  
        ],
        # "Minimum consumption":[
        # ],
        # "Register":[
        #     r"register"
        # ],
        # "Pre-order":[
        #     r"pre.?order"
        # ],
        # "Online sale":[
        #     r"online (sale|only)"
        # ],
        # "Shop sale":[
        #     r"Must Shop Sale", r"shop sale"
        # ],
        # "Next Order":[
        #     r"next order"
        # ],
        "First Order":[
            r"(first|1st)\s+(order|purchase)"
        ],
        "New Customer":[
            r"(new|fresh) (use|customer|member|regist|player)"
        ],
        "Student Only":[
            r"student"
        ],
    }
    for r in requirements:
        for reg in requirements[r]:
            if re.search( reg, text, re.I ):
                result.append( r )
                break
    return ";".join(result)

def get_expire_time( text, today=datetime.datetime.now() ):
    months = ["January","February","March","April","May","June","July","August","September","October","November","December",]
    month_re = "%s" % (r"|".join(months+[m[:3] for m in months]+["sept",]), )
    month_re_num = r"|".join([str(d)[-2:] for d in range(101, 113)] + [str(d) for d in range(1, 10)])
    year_re_strict = r"20[\d]{2}"
    year_re = r"(20)?(18|19)|20[\d]{2}"
    days = [str(i)[-2:] for i in range(101,132)] + [str(i) for i in range(1,10)]
    day_re = "%s" % (r"|".join(days+["%sth" % (d,) for d in days]+["1st","01st","11st","21st","31st"]), )
    # date_num_format = r"[01][\d]{0,1}([\/\-\.])[0-3][\d]{0,1}([\/\-\.])(%s)" % (year_re, )
    # date_num_format = r"[0-3][\d]{0,1}([\/\-\.])[0-3][\d]{0,1}([\/\-\.])(%s)" % (year_re, )
    date_num_format = r"[\d]{1,2}([\/\-\.])[\d]{1,2}([\/\-\.])(%s)" % (year_re, )

    weekdays = ["monday","tuesday","wednesday", "thursday","friday","saturday","sunday"]
    date_points = ["today", "tonight", "midnight", "tomorrow", "weekend", "week", "month"]
    period_format = r"|".join( weekdays+["%s." % (w[:3],) for w in weekdays]+date_points )
    tt = time.time()
    if is_no_end_date( text, month_re, day_re, month_re_num, date_num_format, period_format ):
        tt2 = time.time()
        if global_debug_print:print tt2 - tt
        return ("", "", [])
    tt2 = time.time()
    if global_debug_print:print tt2 - tt, "aa"
    expire_result = ""
    start_date_result = ""
    t1=time.time()
    ###########1 next XXX days
    eng_num = {"zero":0,"one":1,"a":1,"an":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10}
    num_re = r"|".join(eng_num.keys())
    after_next = r"only|left|to go|till|until|sale"
    before_next = r"next|just|last|clearance|only|only for"
    regex_left_days = [
        r"(^|[\W])(?P<exp_d>[\d]+|%s) +days? +(%s)" % (num_re, after_next),
        r"(^|[\W])(?P<exp_w>[\d]+|%s) +weeks? +(%s)" % (num_re, after_next),
        r"(^|[\W])(?P<exp_m>[\d]+|%s) +months? +(%s)" % (num_re, after_next),
        r"(^|[\W])(?P<exp_h>[\d]+|%s) *(hours?|hrs?) +(%s)" % (num_re, after_next),
        r"(%s) +(?P<exp_d>[\d]+|%s) +days?" % (before_next,num_re),
        r"(%s) +(?P<exp_w>[\d]+|%s) +weeks?" % (before_next,num_re),
        r"(%s) +(?P<exp_h>[\d]+|%s) *(hours?|hrs?)" % (before_next,num_re),
    ]
    exp_dict = {"exp_d":1, "exp_h":1/24., "exp_w":7, "exp_m":30}
    for r in regex_left_days:
        left_d_re = re.search( r, text, re.I )
        if left_d_re:
            left_d_dict = left_d_re.groupdict()
            k, v = left_d_dict.items()[0]
            if not v.isdigit():
                v = eng_num.get(v.lower(),1)
            v = int(v)
            # print "a"*199
            # print exp_dict[ k ]
            # print v
            v = str(int(math.ceil( exp_dict[ k ] * v )))
            # print v
            expire_result = "next %s" % (v, )

    #########2 ends/valid/through xx/xx/xxxx
    date_format = r"|".join([
        r"(%s)[^\d]{1,3}(%s)[^\d]{1,3}(%s)" % (month_re, day_re, year_re),
        r"(%s)[^\d]{1,3}(%s)[^\d]{1,3}(%s)" % (day_re, month_re, year_re),
        r"(%s)( of |[^\d]{1,3})(%s)" % (year_re_strict, month_re),
        r"(%s)( of |[^\d]{1,3})(%s)" % (month_re, year_re_strict),
        r"(%s)( of |[^\d]{1,3})(%s)" % (day_re, month_re),
        r"(%s)[^\d]{1,3}(%s)" % (month_re, day_re, ),
        r"(%s)( of |[^\d]{1,3})(%s)" % ( month_re, year_re ),
        r"(%s)[^\d]{1,3}(%s)" % (year_re, month_re, ),
        r"%s" % ( date_num_format )
    ])
    date_format_full = r"|".join([
        date_format,
        r"[0-3][\d]{0,1}([\/\-\.])[0-3][\d]{0,1}"
    ])
    # date_format = r"(%s).{1,3}(%s).{1,3}%s?" % (month_re, day_re, year_re, )
    regex_str = [
        r"(%s) *- *(?P<exp1>%s)\b" % (date_format_full, date_format_full),
        r"((expires?| exp|until|till| to|valid|through|throughout|thru|ends?| by|(ends?.{0,3})) +(late +)?|\-\s*)(?P<exp1>%s)\b" % (date_format_full, ),
        r"((expires?| exp|until|till| to|valid|through|throughout|thru|ends?| by|(ends?.{0,3})) +(late +)?|\-\s*)(this +)?(?P<exp2>(%s)(.{0,2} (%s))?)\b" % (period_format, date_format_full, ),
        r"(?P<exp2>(%s)(.{0,2} (%s))?).{0,2}(only|sale)" % (period_format, date_format_full, ),
        r"(expire| exp|until|till|valid|through|throughout|thru|ends?| by|(ends?.{0,3})) +(?P<exp3>((%s)[ \-\/\.\,]{1,2})+(%s))" % (r"|".join([month_re, day_re, year_re_strict]), year_re_strict),
        r"(?P<exp2>%s).{0,2}(only|sale)" % (month_re, ),
        r"month of (?P<exp2>%s).{0,2}" % (month_re, ),
    ]
    t2=time.time()
    for r in regex_str:
        single_expire_re = re.search(r, text, re.I )
        if single_expire_re:
            expire_date = single_expire_re.groupdict()
            for k in sorted( expire_date.keys() ):
                if expire_date[k]:
                    # if "Oct. 4, 201" in expire_date[k]:
                    #     print expire_date
                    expire_result = expire_date[k]
                    break
        if expire_result:
            break
    t3=time.time()
    ##########3 from begin_date to end_date
    regex_str_f2e = [
        r"\b(?P<exp_d1>%s) *(\-|to|&) *(?P<exp_d2>%s) +(?P<exp_m1>%s)\b" % (day_re, day_re, month_re, ),
        r"\b(?P<exp_m1>%s)[\.]? +(?P<exp_d1>%s) *(\-|to|&) *(?P<exp_d2>%s)\b" % (month_re, day_re, day_re, ),
        r"\b(?P<exp_m1>%s)[\.\-\/ ]+(?P<exp_d1>%s) *(\-|to|&) *(?P<exp_m2>%s)[\.\-\/ ]+(?P<exp_d2>%s)\b" % (month_re, day_re, month_re, day_re, ),
        r"\b(?P<exp_d1>%s)[\.\-\/ ]+(?P<exp_m1>%s) *(\-|to|&) *(?P<exp_d2>%s)[\.\-\/ ]+(?P<exp_m2>%s)\b" % (day_re, month_re, day_re, month_re, ),
        r"\b(?P<exp_m1>%s)[\.\-\/ ]+(?P<exp_d1>%s) *(\-|to|&) *(?P<exp_m2>%s)[\.\-\/ ]+(?P<exp_d2>%s)\b" % (month_re_num, day_re, month_re_num, day_re, )
    ]
    for r in regex_str_f2e:
        # print r
        f2e_re = re.search( r, text, re.I )
        if f2e_re:
            f2e_dict = f2e_re.groupdict()
            start_date_result = "%s %s" % (f2e_dict["exp_m1"], f2e_dict["exp_d1"])
            if "exp_m2" not in f2e_dict.keys():
                expire_result = "%s %s" % (f2e_dict["exp_m1"], f2e_dict["exp_d2"])
            else:
                expire_result = "%s %s" % (f2e_dict["exp_m2"], f2e_dict["exp_d2"])
    if global_debug_print:print expire_result
    
    t4=time.time()
    # print expire_result
    #########4 backup end date, words after expire/exp/valid ...
    backup_expire_flag = []
    # if not expire_result:
    ends_re = re.search(r"( expire| exp| until| till| valid| through|throughout| thru| ends?|( ends?.{0,3})) +(?P<exp1>.{30})", text, re.I )
    if ends_re:
        backup_expire_flag.append( ends_re.groupdict()["exp1"] )
    date_format_backup_re = re.findall(r"(?P<exp1>%s)\b" % (date_format, ), text, re.I )
    if not date_format_backup_re:
        date_format_backup_re = re.findall(r"(?P<exp1>%s)\b" % (date_num_format, ), text, re.I )
    if date_format_backup_re:
        re_text = date_format_backup_re[-1][0]
        before_text = get_before_text( text, re_text, 15 )
        if global_debug_print:print before_text
        if re.search( r"from|start|release date", before_text, re.I ):
            start_date_result = re_text
        else:
            backup_expire_flag.append( re_text )

    t5=time.time()
    ##########5 special time format
    time_re = [
        r"(this|all|the)\s+(?P<exp_t>%s)" % (r"|".join(["week","month"]+weekdays),),
        r"(?P<exp_t>tonight|daily deal|today.{1,8}last|last.{1,8}today)",
        r"(?P<exp_t>%s)" % (months[ int(today.month)-1 ], ),
    ]
    for r in time_re:
        t_re = re.search( r, text, re.I )
        if t_re:
            t_dict = t_re.groupdict()
            re_text = t_dict["exp_t"]
            before_text = get_before_text( text, re_text, 15 )
            if re.search( r"from|start|release date", before_text, re.I ):
                start_date_result = re_text
            else:
                backup_expire_flag.append( t_dict["exp_t"] )
    t6 = time.time()
    if global_debug_print:
        print backup_expire_flag
        print "aaaaaa"
        print t2-t1
        print t3-t2
        print t4-t3
        print t5-t4
        print t6-t5
        print (start_date_result, expire_result, backup_expire_flag)
    return (start_date_result, expire_result, backup_expire_flag)

def format_date(text, date_string, today=datetime.datetime.now(), time_format="%Y%m%d"):
    last_day_this_year = datetime.datetime(today.year, 12, 31)
    first_day_next_year = datetime.datetime(int(today.year)+1, 01, 01)
    time_fix = ""

    year_re = r"(20)?(18|19)|20[\d]{2}"
    date_num_format = r"[\d]{1,2}([\/\-\.])[\d]{1,2}([\/\-\.])(%s)" % (year_re, )

    if date_string:
        # if re.search(r"today|tonight|midnight", date_string, re.I):
        if re.search(r"today|tonight|daily", date_string, re.I):
            return today.strftime( time_format )
        if re.search(r"tomorrow", date_string, re.I):
            return (today + datetime.timedelta(days=1)).strftime( time_format )
        match_next_day_re = re.match(r"next ([\d]+)", date_string)
        if match_next_day_re:
            nextdays = int(match_next_day_re.group(1))
            return (today + datetime.timedelta(days=nextdays)).strftime( time_format )
        if re.search(r"month", date_string, re.I):
            default_t = datetime.datetime( 2019, 12, 31 )
            return parser.parse( "%s-%s" % (today.year, today.month), default=default_t ).strftime( time_format )
        try:
            date_string = re.sub(r"weekend", "sunday", date_string, re.I)
            date_string = re.sub(r"week", "sunday", date_string, re.I)
            if re.search( r"monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|fri|sat|sun", date_string, re.I ):
                time_fix = parser.parse( date_string, default=today, fuzzy=True )
            else:
                time1 = parser.parse( date_string, default=last_day_this_year, fuzzy=True )#.strftime( time_format )
                time2 = parser.parse( date_string, default=first_day_next_year, fuzzy=True )#.strftime( time_format )
                time_fix = handle_illegal_time(date_string, time1, time2, today, text)
        except Exception, e:
            print "Error time convert: ", e
        # return "%s|%s" % (time, date_string)
        if time_fix:
            if re.search( date_num_format, text, re.I ):
                pass
                # time_fix = switch_m_d( time_fix, today )

            time_fix = add_year( time_fix, today, text ).strftime( time_format )
    if not time_fix and re.search(r"midnight", date_string, re.I):
        time_fix = today.strftime( time_format )
    return time_fix

def get_brand( url ):
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
    #   domain = ".".join(domain.split(".")[-3:])
    return domain

def check_freeshipping( text ):
    attach_word = r"^\n\(\)\[\]\+\.\!"
    free_ships = [
        r"free[ ]*[%s]{0,15}[ ]*ship" % (attach_word, ), 
        r"free[ ]*[%s]{0,15}[ ]*deliver" % (attach_word, ), 
        r"free[ ]*[%s]{0,15}[ ]*postage" % (attach_word, ), 
        r"ship[ ]*[%s]{0,20}[ ]*free" % (attach_word, ), 
        r"deliver[ ]*[%s]{0,15}[ ]*free" % (attach_word, ), 
        r"postage[ ]*[%s]{0,15}[ ]*free" % (attach_word, ), 
    ]
    for f in free_ships:
        if re.search(f, text, re.I):
            return 1
    return 0

def get_show_title( text ):
    lines = sep_lines( text )
    line = choose_perfect_line( lines )
    # print text
    # print lines
    if perfect_coupon_title( line ):
        # print "a"* 50
        # print line
        return line
    else:
        return ""

def get_title( text, origin_discount, formate_discount ):
    if global_debug_print:print repr( text )
    lines = sep_lines( text, level=2 )
    if global_debug_print:print lines
    lines = clean_all_lines( lines )
    line_scores = calc_line_score( lines, origin_discount )
    if global_debug_print:print line_scores
    title, show_title = get_perfect_score_line( line_scores, origin_discount )
    return title, show_title

def get_description( text ):
    seps = [r"\n", r"!", r"\. ", r"\? ", r"\?\?", r"…", r"\.\.\."]
    description = recursive_handle_line( text, seps )
    if global_debug_print:print repr(description)
    sep_re = r"cjdx_plchod_mfdeer(%s|$)" % (r"|".join( seps ), )
    if global_debug_print:print repr( sep_re )
    description = re.sub( sep_re, " ", description )
    if global_debug_print:print repr(description)
    description = clean_description( description )
    if global_debug_print:print repr(description)
    return description


def recursive_handle_line( text, split_reg_list ):
    if not text or not split_reg_list:
        return text
    useless_words = [
        "click","visit","at","join","buy","follow","shop","using","to","see","send","learn","for",
        "link","bio","email","message","phone","whatsapp","dm",
        "this","here","more","detail","details","info","information",
        "my","your", "yours", "our", "it","group", "her","via","a","me","the",
        "please","now","today","retweet","re-tweet","just",
        "get","use","amazon", "order", "sale"
    ]
    useless_reg = r"\b(%s)\b" % (r"|".join( useless_words ), )
    this_reg = split_reg_list[0]
    lines = sep_lines( text, level=-1, reg_diy=this_reg )
    result_lines = []

    empty_hold = "cjdx_plchod_mfdeer"
    for l in lines:
        l_lower = l.lower()
        l_lower = re.sub(r"((\w+\s*){0,2}:\s*)?http(s)?[\S]+|\S+@\S+", "", l_lower)
        l_lower = re.sub( r"\w+\s*:\s*(\+)?[0-9]{3,}(\s|$)", "", l_lower)
        l_lower = re.sub(r"#\w+|@\w+", "", l_lower)
        l_lower = re.sub( useless_reg, "", l_lower )
        l_lower = l_lower.strip( self_strip_words )

        if 5 < len(l_lower):
            # print l, "b"*100
            handled_l = recursive_handle_line( l, split_reg_list[1:] )
        else:
            # print l,"a"*100
            handled_l = empty_hold
        result_lines.append( handled_l )
    recovery_reg = "\n" if r"\n"==this_reg else this_reg.replace( "\\", "" )
    return recovery_reg.join( result_lines )


def title( text, origin_discount, formate_discount ):
    lines = sep_lines( text, level=2 )
    lines = clean_all_lines( lines )
    if sum([len(l) for l in lines]) < 80:
        return "aa%s" % (clean_and_format_title(" ".join(lines)),), text
    for l in lines:
        if origin_discount and formate_discount:
            origin_discount = origin_discount.replace("$",r"\$")
            regex_str = [
                r"(get|save|up to|upto|extra|enjoy|take)\s+%s\s+(off|discount)" % (origin_discount, ),
                r"%s\s+off\b" % (origin_discount, ),
                r"(save|save up to|save upto)\s+%s" % (origin_discount, ),
            ]
            for regex_disc in regex_str:
                disc_re = re.search( regex_disc, l, flags=re.I )
                if disc_re:
                    # print disc_re.group()
                    clean_title = ""
                    clean_title = clean_and_format_title( l )
                    if len(clean_title) > 80 or len(clean_title) < 15:
                        clean_title = ""
                    return clean_title, l
    return "", ""

def format_discount( text ):
    text = re.sub(r"[\,\s]","",text)
    text = re.sub(r"(\d+)(\.\d{2})\d?", r"\1\2", text)
    forward_symbol = u"\$|\u20ac|\xe2\x82\xac|\xa3|\xc2\xa3|usd|dollar|us dollar"
    afterward_symbol = u"\%|percent"
    discount_content = r"[\d]+([\.\-]?[\d]+)?"

    miss_match1 = r"(%s)([ ]*)(%s)" % (discount_content, forward_symbol)
    miss_match2 = r"(%s)([ ]*)(%s)" % (afterward_symbol, discount_content)
    match_dis = r"(%s)([ ]*)(%s)|(%s)([ ]*)(%s)" % (discount_content, afterward_symbol, forward_symbol, discount_content )
    formated_discount = text
    if re.match(miss_match1, formated_discount, re.I):
        formated_discount = re.sub( miss_match1, r'\4\3\1', formated_discount, flags=re.I )
    elif re.match(miss_match2, formated_discount, re.I):
        formated_discount = re.sub( miss_match2, r'\3\2\1', formated_discount, flags=re.I )
    elif not re.match( match_dis, formated_discount, re.I):
        formated_discount = ""

    return formated_discount

def is_illegal_discount( discount):
    if discount.endswith("%"):
        discount = discount.rstrip("%")
        discount_list = discount.split("-")
        for d in discount_list:
            try:
                d = re.sub(",","",d)
                dis = float(d)
                if dis >= 100 or dis <= 0:
                    return True
            except Exception,e:
                pass
    else:
        off_d_list = re.findall(r"(\d+(\.\d+)?)", discount)
        min_dis = 0
        for off_d in off_d_list:
            dis = float(off_d[0])
            if dis <= min_dis:
                return True
            else:
                min_dis = dis
    return False

def is_no_end_date( text, month_re, day_re, month_re_num, date_num_format, period_format ):
    all_date_fragments = [
        r"hr|hour|day|week|month|today|tonight|daily",
        r"expire| exp|until|till|valid|through|throughout|thru| by|end",
        period_format, month_re, date_num_format, 
        r"[0-3][\d]{0,1}([\/\-\.])[0-3][\d]{0,1}", 
        r"(%s)[\.\-\/ ]+(%s) *(\-|to|&) *(%s)[\.\-\/ ]+(%s)" % (month_re_num, day_re, month_re_num, day_re, )
        # r"\b(?P<exp_m1>%s)[\.\-\/ ]+(?P<exp_d1>%s) *(\-|to|&) *(?P<exp_m2>%s)[\.\-\/ ]+(?P<exp_d2>%s)\b" % (month_re_num, day_re, month_re_num, day_re, )
    ]
    for frag in all_date_fragments:
        if re.search(frag, text, re.I):
            # print re.search(frag, text, re.I).group(), "aaaaaaaaaaa"*50
            return False
    return True

def handle_illegal_time( date_string, t1_f, t2_f, today, text ):
    next_year = str( int(today.year)+1 )
    t1 = t1_f.strftime( "%Y-%m-%d" )
    t2 = t2_f.strftime( "%Y-%m-%d" )
    y1, m1, d1 = t1.split("-")
    y2, m2, d2 = t2.split("-")
    # print "a"*100
    if y1==y2 and m1==m2 and d1==d2 and (y1==str(today.year) or y1==next_year):
        return t1_f
    ###global unreasonable
    if y1!=y2 and m1!=m2:return ""
    # elif str(today.year) != y1 and y1 not in text:
    #### unreasonable year
    elif str(today.year) != y1 and y1 not in text:
        if m1==m2 and d1==d2:
            ## this year or next year
            t1 = "%s%s" % (today.year, t1[4:])
        else:
            t1 = ''#"---%s" % (t1,)
    elif m1!=m2:
        return ""
    else:
        ## wrong month
        months = ["January","February","March","April","May","June","July","August","September","October","November","December",]
        month_full_re = r"%s" % (r"|".join(months), )
        month_brif_re = r"%s" % (r"|".join([m[:3] for m in months]), )
        # month_brif_re_wro = r"%s\w" % (r"\w|".join([r"sep[a-su-z]|sept\w"]+[m[:3] for m in months if m != "September"]), )
        month_brif_re_wro = r"%s\w" % (r"\w|".join([r"sep[a-su-z]|sept"]+[m[:3] for m in months if m != "September"]), )
        if re.search( month_brif_re, date_string, re.I ) and not re.search( month_full_re, date_string, re.I ) \
                and re.search( month_brif_re_wro, date_string, re.I ):
            return ""

        # year_fix = str(today.year)
    return datetime.datetime.strptime( t1, "%Y-%m-%d" )

def add_year( t, today, text ):
    ## xiecuole zenmeban
    ## can't subtract offset-naive and offset-aware datetimes
    if t:
        y = str(t.year)
        next_year = int(today.year)+1
        if t and y not in text and (today-t).total_seconds() > 3*30*24*3600:
            return t.replace(year=next_year)
    return t

def switch_m_d( t1, today ):
    y,m,d = t1.year, t1.month, t1.day
    if int(d) < 13:
        period_origin = abs( (t1-today).total_seconds() )
        switched_t1 = t1.replace( month=t1.day, day=t1.month )
        period_after_switch = abs( (switched_t1-today).total_seconds() )
        if global_debug_print:
            print period_origin / 3600. / 24
            print period_after_switch / 3600. / 24
        if period_origin > 3600*24*30 and period_after_switch <= 3600 * 24 * 15 or period_origin>period_after_switch+3600*24*60:
            return switched_t1
    return t1

def get_before_text( text, sub_varchars, leng ):
    try:
        index_before_end = text.index(sub_varchars)
        index_before_bg = index_before_end - leng
        index_before_bg = 0 if index_before_bg < 0 else index_before_bg
        before_text = text[ index_before_bg:index_before_end ]
        return before_text
    except Exception, e:
        return ""

def sep_lines( text, level=1, reg_diy="" ):
    if 1 == level:
        text_add_sep = re.sub(r"\n|(http(s)?)[\S]+(\s)?", "cjdx_sep_mfdeer", text)
    #for coupon title
    elif 2 == level:
        text_add_sep = re.sub(r"\n|(http(s)?)[\S]+(\s)?|[!?]|\. |\.\.|>>|<<|//|\*|~", "cjdx_sep_mfdeer", text)
    elif reg_diy:
        text_add_sep = re.sub(reg_diy, "cjdx_sep_mfdeer", text)
    sep_texts = text_add_sep.split("cjdx_sep_mfdeer")
    return sep_texts

def clean_all_lines( lines ):
    result_lines = []
    for l in lines:
        l = l.strip( u" ->:&*+|.@" )
        l = l.rstrip( u"#" )
        if l:
            result_lines.append( l )
    return result_lines

def calc_line_score( lines, origin_discount ):
    line_scores = []
    for l in lines:
        score = 0
        if origin_discount and origin_discount in l:
            score += 10
        comb_set = get_all_combination( l )
        # print comb_set
        for comb in config_title_material:
            if comb in comb_set:
                score += config_title_material.get( comb, 0 )
        line_scores.append( [l, score] )
    return line_scores

def get_perfect_score_line( line_scores, origin_discount ):
    result_line = ""
    show_title = ""
    total_score = 0.000001
    max_score = -1
    for l in line_scores:
        this_score = l[1]
        total_score += this_score
        if this_score > max_score:
            max_score = this_score
            result_line = l[0]
    rate = round(max_score/total_score,2)
    if 0.3 < max_score and rate > 0:
        show_title = clean_and_format_title( result_line, origin_discount )
        if global_debug_print:print show_title
    return "%s-%s-%s" % (rate, max_score, result_line), show_title
    

def get_all_combination( line ):
    line_handle = re.sub( self_discount_content, " number_cjdx_mfdeer ", line.lower() )
    split_words_list = [l.strip( self_strip_words ) for l in line_handle.split()]
    split_words_list = [l for l in split_words_list if l]
    result = set()
    for i in range(4):
        result.update( ngrams( split_words_list, i+1 ) )
    return result
    #split_word and format
    


def choose_perfect_line( lines ):
    line_is_coupon_list = []
    for l in lines:
        tweet = {"text":l, "entities":{"urls":[]}}
        terms_all = [term for term in preprocess( l ) if term not in stop]
        terms_hash = [term for term in terms_all if term.startswith('#')]
        terms_for_coupon = [term.strip("#@").lower() for term in terms_all] + [t.lower() for t in terms_hash]
        l_coupon_flag = is_coupon(tweet, terms_for_coupon, {})
        if 3 == l_coupon_flag:
            line_is_coupon_list.append( 0 )
        else:
            line_is_coupon_list.append( 1 )
    if 1 == sum( line_is_coupon_list ):
        l = lines[ line_is_coupon_list.index(1) ]
        # l = re.sub(r"(#[^\s]+\s*)*$", "", l)
        l = clean_description( l )
        l = re.sub(r"\s(on|in|with|at|by)\s*$", "", l, flags=re.I)
        return l
    else:
        return ""

def perfect_coupon_title( line ):
    l_lower = line.lower()
    words = l_lower.split()
    len_words = len(words)
    flag_start = False
    if re.search(r"%|\$| off\b", l_lower[:25]):
        flag_start = True
    if l_lower[:5] in ("save ", ) or l_lower[:4] in ("get ", ) or l_lower[:6] in ("up to", ) :
        flag_start = True
    if flag_start and len_words>4 and len_words<15:
        return True
    else:
        return False

def clean_description( text ):

    if global_debug_print:print type( HTMLParser().unescape(text) )
    result_dellink, num_dellink = re.subn(r"(\b(rt|via|on|in|with|at|by|visit)\b.{0,2})?http(s)?[\S]+|\S+@\S+", " ", text, re.I)
    result_delat, num_delat = re.subn(r"(\b(rt|via|on|in|with|at|by)\b.{0,2})@[\S]+(:|\s|$)?", " ", result_dellink, re.I)
    # result_delat = re.sub(u"[=❤<>�♢♠◕♧♯»✔★]…", " ", result_delat.lstrip(u"*^+?\.[]^|)}~!/@£%&=`´;><: ❤�♢♠◕♧♯»✔★"))
    result_delat = re.sub(r"(#[^\s]+\s*|@[^\s]+\s*|\$[a-zA-Z]+\s*){2,}", " ", result_delat)
    result_delat = re.sub(r"^@[^\s]+\s*", " ", result_delat.strip())
    result_delat = re.sub(r"\w+\s*:\s*(\+)?[0-9]{3,}(\s|$)", " ", result_delat)
    result_delat = re.sub(r" RT ", " ", result_delat)
    result_delat = re.sub(r"\"", "-", result_delat)

    result_delat = re.sub(r"[\n]+", ". ", result_delat)
    result_delat = re.sub(r"\?{2,}|_{2,}|\*{2,}|\.{2,}", ", ", result_delat)
    if global_debug_print:print result_delat
    result_delat = normal_multi_symbol( result_delat )
    result_delat = result_delat.strip()
    if not re.match(r"^[\[\(\"]\w", result_delat):
        result_delat = result_delat.lstrip( self_strip_words )
    if not re.search(r"\w[\]\)\"]$", result_delat):
        result_delat = result_delat.rstrip( self_strip_words )
    if "(" in result_delat and ")" not in result_delat:
        result_delat = "%s)" % (result_delat,)

    result_delat = re.sub(r"\b(when|if|make sure|while|for)\s+you(\b|\s+are\b|'re\b)|\byou\s+(can't|can|could|would|will)\b|you'll", " ", result_delat)
    result_delat = re.sub(r"[#@]", " ", result_delat)
    result_delat = re.sub(r"\b(you|we're|we are|my)\b", " ", result_delat)

    # result_delat = re.sub( r"[\*]", "", result_delat )
    result_delat = re.sub(r"\s+", " ", result_delat.strip())
    return result_delat

def normal_multi_symbol( text, symbols = r",\.\?!:;/\*\-_~><" ):
    re_sy = r"\s*([%s])(\s*[%s]\s*)+" % (symbols, symbols)
    return re.sub( re_sy, r"\1 ", text )


def clean_and_format_title( text, origin_discount="" ):
    # print text, "a"*100
    term_symbols = set(",-:;><")
    end_term_symbols = set(":;><")
    term_for_sent_2long = [r"\bfor\b", r"\band\b", r"\bwith\b", r"\bfrom\b", r"\buntil\b",
        r"\btill\b", r"\bexpire\b", r"\bexp\b", r"\bbefore\b", r"\bthru\b", r"\bthrough", r"\bso\b", 
        r'\busing\b', ', ', "~", "&",]#"-"
    text = text.lower()
    if len(text) > 70:
        #split // subsribe/you/my/we/if you/,
        index_begin = 0
        index_end = len( text )
        if origin_discount:
            index_dis = text.find( origin_discount )
            if -1 != index_dis:
                for i in range(30):
                    if index_dis-i >= 0 and text[index_dis-i] in term_symbols:
                        index_begin = index_dis-i+1
                        break
                else:
                    if re.search( r"\b(you|we|youself|i'm|i'll|i've|i'd|i)\b", text[:index_dis] ):
                        index_begin = index_dis
                    if index_dis < 50:
                        index_begin = 0
                    else:
                        index_begin = index_dis
                if len(text[index_begin:]) > 70:
                    words_list = break_words( text[index_dis:], term_for_sent_2long )
                    index_end = index_dis + len(words_list[0])
                if len(text[index_begin:index_end]) > 70:
                    words_list = break_words( text[index_dis:], [r"\boff\b"] )
                    index_end = index_dis + len(words_list[0]) + len("off")
                    # add 3 is too long
                    if index_end > len(text):
                        index_end = len(text)

                for i in range(index_dis, index_end):
                    if text[i] in end_term_symbols:
                        index_end = i
                        break
            else:
                #not found
                text = "%s" % (text, )
                pass
        else:
            text = "%s" % (text, )
        text = text[index_begin:index_end]

    # you can use my code to... use my code to... are you looking... do you want
    #anywhere (Use my code for|my code|We're giving you|We are giving away|u |you get|Try it using my code and we'll|You Will Receive)
    text = re.sub(r"(you\s+|u\s+)?(use|using|with|click|click on|clicking|w/|follow|following|via|enter|press|tap|checkout|check out)\s+(my\s+|the\s+|this\s+)?(promo *|coupon *|invite *)?(codes?|link|here)(\s+below|\s+in bio|\s+in my bio)?(\s+to |\s+for |\s+and )?", " ", text)
    #here is, don't forget
    text = re.sub(r"(the\s+|this\s+|shop\s+)?(link in (my\s+)?bio|this link)", " ", text)
    # @a @b #a #b
    text = re.sub(r"(#[\w]+ *)+(#[\w]+)|(@[\w]+ *)+(@[\w]+)", " ", text, re.I)
    strip_w = self_strip_words
    # strip_w = u" -><:&*+|,.@#/"
    text = text.strip( strip_w )
    head_tail_words = ["and","at","to","on","or","for","with","is","when","by","rt","visit","my"]
    head_tail_re = r"|".join( head_tail_words )
    text = re.sub(r"^\s?(%s)\b|\b(%s)\s?$" % (head_tail_re, head_tail_re), " ", text, re.I)
    text = text.strip( strip_w )

    text = re.sub(r"\?{2,}|_{2,}|\*{2,}|\.{2,}", ", ", text)
    text = normal_multi_symbol( text )

    #
    # with/w (my|promo) code|referal link
    text = re.sub(r"\b(when|if|make sure|while|for)\s+you(\b|\s+are\b|'re\b)|\byou\s+(can't|can|could|would|will)\b|you'll", " ", text)
    text = re.sub(r"[#@]", " ", text)
    text = re.sub(r"\b(you|you're|you are|we're|we are|my)\b", " ", text)
    # multi to single _-:
    text = re.sub(r"[\(\)\[\]\"\{\}]", ", ", text)
    text = re.sub(r"(\,\s+)+", ", ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip( strip_w )
    return text.title()

def break_words( text, words ):
    desc = re.sub( r"|".join(words), "-c-j-d-x-break-", text )
    desc_list = desc.split("-c-j-d-x-break-")
    return desc_list

def is_like_title( title ):
    title = title.lower()
    if len(title) <= 10:
        return False
    if len(title) <= 16 and not re.search( self_discount_format, title, re.I ):
        return False
    if "retweet" in title or "re-tweet" in title :
        return False
    if len(title) > 100:
        return False
    return True

def gen_template_title(discount, code, freeship, price, coupon_requirement):
    title = ""
    if discount:
        if code:
            title = u"{discount} Off".format( discount = discount )
        else:
            discount_temps = [u"Get {discount} Off", u"Save {discount} Off", u"Up to {discount} Off"] 
            title = random.choice(discount_temps).format( discount = discount )
    if freeship:
        freeship_syb = [u" + ", u" Plus "]
        title_items = [i for i in [title, u"Free Shipping"] if i]
        title = random.choice(freeship_syb).join( title_items )
    if not title and price:
        title = u"Price Drop: Only {price}".format( price=price )
    if coupon_requirement:
        requires = re.sub( u";", u", ", coupon_requirement )
        if title:
            title = u"{title} {coupon_req}".format( title=title, coupon_req=requires )
        else:
            title = u"Saving {coupon_req}".format( coupon_req=requires )
    if code:
        if title:
            if len(title) < 15:
                temp_codes = [u"{title} With Code", u"{title} With Promo Code", u"{title} With Promocode", u"Use Code For {title}"]
                title = random.choice( temp_codes ).format( title=title )
        else:
            just_code_items = [u"Use Code For Big Discount", u"Save With Promo Code", u"Shop With Coupon Code", u"Use Code At Checkout"]
            title = random.choice( just_code_items )
    return title


#######################################################################################################################################
#######################################################################################################################################
class Coupon_info_extractor():
    def __init__(self):
        self.couponinfo_table = config.couponinfo_table
        self.couponinfohandle_table = config.couponinfohandle_table
        self.likely_couponinfo_table = config.likely_couponinfo_table
        self.likely_couponinfohandle_table = config.likely_couponinfohandle_table
        self.couponinfo_common_fields = config.couponinfo_tohandled_common_fields
        self.config_process_num = config.config_process_num
        self.config_num_each = config.config_num_each

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
#       self.ignore_brands = ["ebay.com", "amazon.com", "deals.ebay.com", "instagram.com", "couponslinks.com", "twitter.com", "facebook.com", "youtube.com", "google.com"]
        
        ##common regex of discount
        self.discount_symbol = u"(\%|\$|\u20ac|\xe2\x82\xac|\xa3|\xc2\xa3|percent|usd|dollar|us dollar)"
        self.discount_content = r"[\d]+([\.\-\,]?[\d]+)?"
        self.discount_format = r"%s[ ]*%s|%s[ ]*%s" % (self.discount_symbol, self.discount_content, self.discount_content, self.discount_symbol)
        ##common regex of price
        self.price_symbol = u"(\$|\u20ac|\xe2\x82\xac|\xa3|\xc2\xa3|usd|dollar|us dollar)"
        self.price_content = r"[\d]+([\.\-\,]?[\d]+)?"
        self.price_format = r"%s[ ]*%s|%s[ ]*%s" % (self.price_symbol, self.price_content, self.price_content, self.price_symbol)


    def connect_db(self):
        #self.coupon_con = mdb.connect(host='192.168.8.222', user='root', passwd='moma', db='coupon_datacenter', charset='utf8')
        self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
        self.coupon_cur = self.coupon_con.cursor()
        self.coupon_cur.execute("SET NAMES utf8mb4")

    def close_db(self):
        self.coupon_cur.close()
        self.coupon_con.close()

#   def update_brand(self):
#       target_table = "coupon_twitter_today"
#       brand_table = "cj_sas_linkshare_brand"
#       sql_update = """UPDATE %s INNER JOIN %s ON %s.advertiserid = %s.advertiserid AND %s.source = %s.source
#           SET %s.brand = %s.brand
#           WHERE %s.advertisername is not null and %s.advertisername <> '' and %s.advertisername <> '*' """ % (target_table, brand_table, target_table, brand_table,
#               target_table, brand_table, target_table, brand_table, target_table, target_table, target_table)
#       self.coupon_cur.execute(sql_update)
#       self.coupon_con.commit()

    def store_back(self, coupons_useful, to_table, common_fields):
        sql_update = """INSERT INTO %s (%s, choosed_url, freeshipping, code, discount, price, \
                title_info, show_title, show_description, expand_domain, extend_tweet_expand_domain, \
                coupon_requirement, coupon_start_date, coupon_end_date) values (%s)""" % \
                (to_table, common_fields, ",".join(["%s" for i in range(common_fields.count(",") + 14)]))
        print sql_update
        if coupons_useful:
            self.coupon_cur.executemany(sql_update, coupons_useful)
            self.coupon_con.commit()

    def update_and_clean_table(self, from_table, to_table):
        sql_update_guid = """UPDATE {to_table} set guid = MD5( choosed_url )""".format(to_table=to_table)
        # self.coupon_cur.execute(sql_update_domain)
        self.coupon_cur.execute( sql_update_guid )
        self.coupon_con.commit()

        sql_delete_couponinfo_table = """truncate %s""" % (from_table)
        self.coupon_cur.execute(sql_delete_couponinfo_table)
        self.coupon_con.commit()


    def coupon_info_extract_distribute(self, likely=False):
        from_table = self.couponinfo_table
        to_table = self.couponinfohandle_table
        if likely:
            from_table = self.likely_couponinfo_table
            to_table = self.likely_couponinfohandle_table
        common_fields = self.couponinfo_common_fields
        sql_select = "SELECT %s from %s " % (common_fields, from_table)
        # sql_select = "SELECT %s from %s where id in (18608)" % (common_fields, from_table)
        
        coupon_re = self.coupon_cur.execute(sql_select)
        coupons = [list(a) for a in self.coupon_cur.fetchall()]

        print "after get data, start running distribute"

        coupons = coupons[:]
        if self.config_process_num > 1:
            processes = Pool(processes=self.config_process_num)
            params = []
            for i,cp in enumerate( coupons ):
                print i
                params.append( cp )
                if 0 == (i+1) % self.config_num_each or i == len(coupons)-1:
                    coupons_useful = processes.map( coupon_extract_run, params )
                    coupons_useful = [ c for c in coupons_useful if c ]
                    self.store_back(coupons_useful, to_table, common_fields)
                    params = []
        elif 1 == self.config_process_num:
            coupons_useful = []
            for i,cp in enumerate( coupons ):
                print i
                coupon_extract_item = coupon_extract_run( cp )
                if coupon_extract_item:
                    coupons_useful.append( coupon_extract_item )
                if 0 == (i+1) % self.config_num_each or i == len(coupons)-1:
                    self.store_back( coupons_useful, to_table, common_fields )
                    coupons_useful = []
        self.update_and_clean_table( from_table, to_table )



    def handle_text(self, text):
        # text_a, num = re.subn("code", "CODe", text, flags=re.I)
        text_a = text
        result = re.subn(self.reg_code, " ", HTMLParser().unescape(text_a), flags = re.I)[0]
        result = re.subn(self.spe_reg_code, " ", result, flags = re.I)[0]
        result = re.subn(r"\s+", " ", result.strip("- "), flags = re.I)[0]
        return result


if "__main__" == __name__:
    coupon_handler = Coupon_info_extractor()
    coupon_handler.connect_db()
    # coupon_handler.update_brand()
    coupon_handler.coupon_info_extract_distribute()
    # coupon_handler.coupon_info_extract_distribute( likely=True )
    coupon_handler.close_db()

    texts = [
        # """.xyz TLD Namecheap: Coupons, Coupon Codes, Shopping Deals - https://t.co/39Hq4bJf1B - Coupons, Coupon Codes for your online savings 90% OFF .xyz TLD for just $0.99 coupon/deal ends Wednesday 31st of October 2018 11:59:59 PM The post .xyz TLD Namecheap… https://t.co/Px3wmG8YRo https://t.co/VVLa4WwePO""",
        # """SALE 20% OFF - END 1.10.2018????Chakra Jewelry Seven Chakra Jewelry 7 Chakra Earrings and Necklace Basalt Color Jewelry One Of A Kind Polymer Clay Holiday Gift For Her https://t.co/dVl3kjcRD4 чрез @Etsy""",
        # """END 1.10.2018 """,
        # """???? Top Awarded Blends of 2016, 2017, &amp; 2018
        #     ???????? 30 Day Risk Free Money Back Guarantee
        #     ✨ First Order 20% Off With Discount: NEW20
        #     ???? 97% See Results in First 3 Days!
        #     ???? Over 1 Million Clients Worldwide
        #     ???? Free Shipping to Every Country
        #     ????…
        #     ➤ https://t.co/cvupmM2VHN
        #     via @outfy https://t.co/rGgVNyxv7Y""",
        #     """Coach up to 50% off special sale at Takashimaya from 3 – 7 Oct https://t.co/4q9OaP835k #singapore #deal #sale #coach #ngeeanncity #takashimaya""",
        #     """Make tonight pizza night!! Get a free large, one-topping pizza from @PapaJohns tonight and Sunday only! Use promo code BETTERTOGETHER on Papa John’s mobile app when you make a minimum $12 purchase! Perfect for family gatherings! #ad #BetterTogether &lt;&lt;&lt; https://t.co/Mi8ncaz2Nz&gt;&gt;&gt; https://t.co/BngDvt8jyI""",
        #     """https://t.co/xNm8Sr5VuF #freebies #freebiesinmail T-Mobile Tuesday 10-2-18: Free $2 Dunkin Donuts Card, $25 off tickets at Fanxchange, $15 to https://t.co/ryQak0nyHy https://t.co/MBYtAN9zte""",


        u"""???????? HUGE CLOSING DOWN SALE! ????????

There's just 27 days left until my shop closes, so as a treat there is 20% off EVERYTHING until Sunday!

Pins, artwork, tote bags, stationery &amp; more! 

???? https://t.co/DGR2dlR423  ????

@Cbeechat #bloggerstribe #lbloggers #etsy #sale https://t.co/rOtdvwAlV3""",
        u"""????October Regular Deadline! Submit your Feature, Short, Animation, Pilot/Web Series, Trailer, Music Video, and Scrip… https://t.co/5qBFyFuV2j""",
        u"""OCTOBER FEST

            $9.99 Monthly Subscription 

            coupon code : octoberfest

            https://t.co/WiIsAKAj76 https://t.co/6gX5M1oijT""",
        u"""ON DECK THIS WEEK

            Wednesday October 3rd - NHL SEASON BEGINS 1 DAY SALE!!
            - Everything NHL ON SALE For 10%-40% OFF!!!

            Thursday October 4th - 2018-19 Upper Deck Artifacts Release... https://t.co/WCcJcqmtyy""",
        u"""Halloween decor on sale! 10% off in all packs until 7th October #sale #Halloween #horror #movieposter… https://t.co/7MfVOcH0Cd""",
        u"""FAMILY DAY TODAY 25% OFF ALL DAY. ...SEE YOU AT FROYOLAND !

            Valid 10/10 4:00PM-10/10 10:00PM
            https://t.co/UbjCvfshWO https://t.co/3EhSa4n3Uu""",
        u"""To celebrate the launch of MV Fetish, select vids are now 50% off for the entire month of October!
        ???? https://t.co/veOvGj8yhq

        Only on @ManyVids #ManyVids https://t.co/cr7pLfmxEs""",
        u"""Thursday, Oct 4 Watch Brill Babes @brillbabesxxx Daisy Dawkins ????on https://t.co/GEd4yLMITG See this Over 2,000 Othe… https://t.co/XeytIUT8UA""",
        u"""Free Shipping SmartWool: Coupons, Coupon Codes, Shopping Deals - https://t.co/39Hq4bJf1B - Coupons, Coupon Codes fo… https://t.co/gyyLMy5U5f""",
        u"""Free shipping on RAB products with code BONUSRAB. Buy now! Deal ends on October 8th: https://t.co/X0gofzb1xf https://t.co/JL60xmt5CV""",
        u"""CHINA LUNG TSING U HAI RAILWAY 5% GOLD LOAN 1913 BOND 20£ WITH COUPONS: $715.00 (0 Bids) End Date: Sunday Oct-7-201… https://t.co/bt6k3kGF2i""",
        u"""Chope Deals’ Mega Sale (5 Oct) – With Over 100 Dining Deals At Up To 85% OFF https://t.co/3hgJzHTFDq""",
        u"""https://t.co/FOhSO6C9vt - 40% OFF ALL BEATS AND BEAT ALBUMS! CODE = flawless USE AS MANY TIMES AS WANTED TILL 2018!… https://t.co/omfEFOqs55""",


        u"""Just two weeks until #FHExpo18. NVFC members can enjoy FREE exhibit hall admission and $50 off the full conference when you register using promo code NVFC. Take advantage of this special offer - register here: https://t.co/e0u8Cfvp4Z https://t.co/VaBdhGamBr""",
        u"""game of thrones home decor_665_20180827134528_62 #home decor 50% off, rear v… – Jakarta Picture https://t.co/zF7mWIb54a https://t.co/lF5TrdgOIM""",
        u"""Thursday! Hear leaders at the #NSA, #DHS, #DISA &amp; other #FedGov agencies discuss national risk management offensive capabilities &amp; more at the @GCIOMedia CXO Tech Forum. Industry attendees can use code “GovEvents” for 50% off registration &gt;&gt; https://t.co/5Elwt9UBqM #GovTech https://t.co/IdsEpc5jYp""",
        u"""???? Ending late TONIGHT at 3:59 am CT, the racer blue/black-hyper crimson Nike Air Max 270 is over 35% OFF retail at $95.99 + ship!

            BUY HERE -&gt; https://t.co/kzQ3DqEYZe (use promo code CHAMPS20) https://t.co/kSRCasldsH""",
        u"""???? Xiaomi Yeelight Smart Light Strip RGB LED 1m ( No Adapter ) ????
            Prix : $9.99
            ✂ Coupon : China
            #Xiaomi #TomTop #deals #bonplan #promotions #réduc #discount 
            https://t.co/0XF3hVf1Zt""",
        u"""Hey Its Jenesaisquoi Fashion
            30% OFF Everything 
            + Free Shipping 
            Limited Time Only 
            Two Week Sale !
            Discount Code : 30%OFF
            https://t.co/cLKTz2xI0q https://t.co/oTOtgqhmS4"""
    ]

    # texts = [
    #     "Surprise!! We now have our 3 new Fall re-inkers in stock so we're doing a sale - buy any 2 Moonlight Duo ink pads and get a FREE re-inker. NO CODE - Discount will be applied automatically at checkout!!",
    #     "Deck the walls! The gallerywall is the perfect opportunity to showcase your favorite pieces and infuse personality into a room. Starting today through 11/11, we're giving you 15% off all art and accessories using code DECK15: (Credit:",
    #     "Xiaomi Airdots TWS Bluetooth 5.0 Earphone Youth Version Deal Price $$44.99 with Coupon: 3591bd Deal Link: From Banggood | Free Shipping JOIN GROUP TECH DEALS AND COUPONS -",
    #     "Join our friends on the 1st dec - as they look into redefining the galaxy and sharing the past, present and future of tech GGMUK18 Get our friends discount using code GGMFriends.",
    #     "Real Ones Bring Balance To The Fake Ones BewareOfFakeFriends HatCulture WeWearCulture HatGameProper KickGameProper RealOnesOnly FlyShitOnly Use Promo Code (bbSale25) On",
    #     "Authentic Vandy Vape Berserker V1.5 24mm MTL RTA Coupon Code MAPV15 Get it here What's New? 1.Convenient ring for refueling 2.Slot airflow 3.Improved liquid flow 4.Convenient airflow ring 5.Easy to setup 6.Real MTL experience",
    #     "is coming to tx_live and playing the arlingtonbackyard Friday Nov 9th Doors open at 7pm. Get your tickets at and use promo code BrianS at checkout. VIP",
    #     "LAST CHANCE! Sign up for Solution Zone in our San Francisco office for a chance to have your questions answered by our award-winning Support Team. Join us on November 8th and use the discount code TWITTER for free entry:",
    #     "Another huge day of football is ahead of us! Our games will be posted across our platforms from mid day. Best bookie to place them is 1XBET. If you don't have an account Sign up here Use this Promo code : 1x_26814",

    #     """EL AL EXCLUSIVE: Use code ELAL18 to receive up to 33% off tix at the 2018 International Film Festival running until 11/13. 80 films from Israel & 21 other countries with a filmmaker Q&A after many screenings. Purchase now:""",
    #     """keeps his pump going with Vascular BCAAs. Get yours today! - Get 30% off + FREE shipping with promo code FALL30. - Get explosive energy now at Get lean muscle gains now at""",
    #     """Click to start shopping now! Visit and save 25% on everything because its our birthday! Use code : 3YEARS today and today only! Gigi's House of Frills""",
    #     """Those magnetic shoe closures I love so much are on sale. Buy from use "fall18" at checkout, and get 25% off through November 14. Fiddly to lace right, but once you have them set, you never have to change them again.""",
    #     """Hey ReactNext 2018! Save 40% on react, ReactNative, redux, microservices, fp, CSS, components, and other books and videos with code ctwreactnext18 at""",
    #     """30% DISCOUNT ON SELECTED SWEATS, HOODIES AND TEES USE CODE : 30TODAY at checkout OFFER VALID UNTIL MIDNIGHT TONIGHT SAVE TIME AND MONEY WITH PLANET MONKEY""",

    #     ]
    # coupon_handler = Coupon_info_extractor()
    # for t in texts:
    #   try:
    #       print "-"*100
    #       t = coupon_handler.text_fix( t )
    #       print t
    #       a =  coupon_handler.get_expire_time( t )
    #       print a
    #       print coupon_handler.format_date( t, a[1] )
    #       print "+"*100
    #       print coupon_handler.get_code( t )
    #   except Exception,e:
    #       print e
