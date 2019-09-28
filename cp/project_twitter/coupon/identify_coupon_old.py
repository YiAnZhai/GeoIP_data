import re
import json
import string
# import pandas
# import collections
from collections import Counter
from nltk.corpus import stopwords
from nltk import bigrams 
from collections import defaultdict
import operator
import sys
import math
import os
from datetime import datetime as dt
import datetime
import MySQLdb as mdb
import config

# remember to include the other import from the previous post
 
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
 
#    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_%\$]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)


class Tweet():
    def __init__(self):
	self.DB_HOST = config.DB_HOST
	self.DB_NAME = config.DB_NAME
	self.DB_USER = config.DB_USER
	self.DB_PSWD = config.DB_PSWD
	self.CHARSET = config.CHARSET
	self.negative_domain_table = config.negative_domain_table

    def connect_db(self):
        self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
        self.coupon_cur = self.coupon_con.cursor()

    def close_db(self):
        self.coupon_cur.close()
        self.coupon_con.close()

    def get_blacklist_domain(self):
        self.connect_db()
        sql = "select domain,type from %s" % (self.negative_domain_table)
        self.coupon_cur.execute(sql)
        nagetive_domains = self.coupon_cur.fetchall()
#       for nagetive_domain in nagetive_domains:
#           if "porn" != nagetive_domain[1]:
        nagetive_domain_dict = {nagetive_domain[0]:["."+nagetive_domain[0], "/"+nagetive_domain[0]] for nagetive_domain in nagetive_domains}
#        nagetive_domain_list = [nagetive_domain[0] for nagetive_domain in nagetive_domains]
	self.close_db()
	return nagetive_domain_dict
#	return nagetive_domain_list


def get_sure_list(item_str_list):
    result_set = set()
    for item_str in item_str_list:
	item_model = item_str.split("---------")
	if 2 == len(item_model):
	    for word in item_model[1].split(","):
		list_others = word.split("&&")
		result_set.add( tuple([item_model[0].strip()] + list_others) )
    return result_set

def tokenize(s):
    return tokens_re.findall(s)
 
def preprocess(s, lowercase=True):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else (token.lower() if not token.startswith(('#', '@')) else token) for token in tokens]
    return tokens

def print_list(list_a, title="Default"):
    print
    print "%s shows Below: " % title
    for li in list_a[:]:
        print "%s: %s" % (li[0], li[1])


def print_listin(listin, title="Default"):
    print
    print "%s shows Below: " % title
    for li in listin[:]:
        print "(%s): %s" % (", ".join(li[0]), li[1])

def is_coupon(tweet, list_t, search_word_dict):
    text = tweet["text"]
#    list_sure = [["save up to","coupon"], ["promo code", "off"], ["use code", "off"], ["% off", "code"], ["% off", "coupon"], ["% off", "order"], ["% off", "up to"], ["% off", "sale"], ["% off", 'buy'], ["promo code"]]
#    print sure_list

#    for common_catword in common_cats:
#	if common_catword in text.lower():
#	    return 2
    for l in sure_list:
	for l_word in l:
	    if l_word not in text.lower():
		break
	else:
	    url = "****".join([ a["expanded_url"] for a in tweet["entities"]["urls"] ])
            for black_domain in domain_black_list:
		for black_domain_pattern in domain_black_list[black_domain]:
		    if black_domain_pattern in url:
			return (4, black_domain)

	    for common_catword in common_cats:
	        if common_catword in text.lower():
	            return 2
#	    coupon_model_all[l] += [text]   #for review coupon
	    return 0 #super Ture

    for search_word in search_word_dict:
        for word in search_word.split(" "):
            if word not in text.lower():
#           if word not in terms_all:
                break
#       if search_word in terms_all:
#       if search_word in tweet['text'].lower():
        else:
#	    search_word_dict[search_word].update(list_t)    #co word with search word
            return 1 #linkly True
    return 3

# with open('coupon_tweet.json', 'r') as f:
#     for line in f:
#         try:
#             tweet = json.loads(line)
#             tokens = preprocess(tweet['text'])
#             print tokens
#         except Exception, e:
#             pass
# tweet = "RT @marcobonzanini: just an example! :D http://example.com #NLP"
# print(preprocess(tweet))
# ['RT', '@marcobonzanini', ':', 'just', 'an', 'example', '!', ':D', 'http://example.com', '#NLP']

#coupon model(keyword and phase)
coupon_model_text = open("coupon_model.cfg", "r").read()
exec(coupon_model_text)
sure_list = set()
for sure_str_list in sure_dict.values():
    sure_list = sure_list.union(get_sure_list(sure_str_list))

coupon_feather_str = ",".join( coupon_feature )
coupon_fea_other_list = [a.split("---------")[0]+"---------"+coupon_feather_str for a in sure_dict["other"]]
sure_list = sure_list.union(get_sure_list(coupon_fea_other_list))
print sure_list
print

tweet = Tweet()
domain_black_list = tweet.get_blacklist_domain()
print domain_black_list
print

#coupon below everymodel
coupon_model_all = {a:[] for a in sure_list}

punctuation = list(string.punctuation)
#stop = stopwords.words('english') + punctuation + ['rt', 'via']
#stop = [",", ".", "-", "!", "/",":"]
stop = [u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs', u'themselves', u'what', u'which', u'who', u'whom', u'am', u'is', u'are', u'was', u'were', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'as', u'of', u'at', u'by', u'for', u'not'] + punctuation + ['rt', 'via']

search_word_list = likely_dict.split(",") # pass a term as a command-line argument
search_word_dict = {search_word_list[i]:Counter() for i in range(len(search_word_list))}
#com = defaultdict(lambda : defaultdict(int))
coupon_store_path = config.coupon_tweet_filter_filestore_path
dirname = config.tweet_common_coupon_filestore_path

for i in range(1, 1):
    n_docs = 0
    n_notOK = 0
    n_retweet = 0
    n_delete = 0
    n_nodelete = 0
    n_coupon = 0
    count_all = Counter()
    count_hash = Counter()
    count_user = Counter()
    count_only = Counter()
    count_bigram = Counter()
    
    
    yestoday = (dt.now() - datetime.timedelta(days=i)).strftime("%Y%m%d")
    f_coupon = open( os.path.join(coupon_store_path, "%s_coupon.txt" % yestoday), "w" )
    f_likecoupon = open(os.path.join(coupon_store_path, "%s_coupon_likely.txt" % yestoday), "w")
    f_coupon_bigcat = open(os.path.join(coupon_store_path, "%s_coupon_bigcat.txt"  % yestoday), "w")
    f_coupon_blacklist = open(os.path.join(coupon_store_path, "%s_coupon_blacklist.txt"  % yestoday), "w")
    
    f_coupon_retweet = open(os.path.join(coupon_store_path, "%s_retweet_coupon.txt" % yestoday), "w")
    f_likecoupon_retweet = open(os.path.join(coupon_store_path, "%s_retweet_coupon_likely.txt" % yestoday), "w")
    f_coupon_bigcat_retweet = open(os.path.join(coupon_store_path, "%s_retweet_coupon_bigcat.txt"  % yestoday), "w")
    f_coupon_blacklist_retweet = open(os.path.join(coupon_store_path, "%s_retweet_coupon_blacklist.txt"  % yestoday), "w")
    
    
    retweet_coupon = {}
    retweet_likelycoupon = {}
    retweet_bigcat = {}
    retweet_blacklist = {}
    
    dir_list = os.listdir(dirname)
    dir_list.sort()
    destination_dir =[file_name for file_name in dir_list[:] if file_name.startswith("coupon-%s" % yestoday)]
    #destination_dir = dir_list
    for child_file in destination_dir[:]:
        print "file", child_file
    #    fname = '%s%s' % (dirname, child_file)
        fname = os.path.join(dirname, child_file)
    #    fname = '/disk1/data/twitter/data_twitter_coupon_commonkeywords/coupon-20160616-1058->20160616-1322.txt'
        with open(fname, 'r') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                    
                    if tweet.has_key("retweeted_status"):
                        n_retweet += 1
                    if tweet.has_key("delete"):
                        n_delete += 1
    	            else:
    		        n_nodelete += 1
                    # if "healthcare" in tweet['text'].lower() and "en" == tweet["lang"]:
                    # if "nursing" in tweet['text'].lower() and "en" == tweet["lang"]:
                    # if "hiring" in tweet['text'].lower() and "en" == tweet["lang"]:
                    if "en" == tweet["lang"] and not tweet.has_key("retweeted_status"):
    	        # if "en" == tweet["lang"]: 
    		# if True:
                        n_docs += 1
                        # Create a list with all the terms
                        terms_all = [term for term in preprocess(tweet['text']) if term not in stop]
                        # Count hashtags only
                        terms_hash = [term for term in preprocess(tweet['text'])
                                      if term.startswith('#')]
                        # Count user only
                        terms_user = [term for term in preprocess(tweet['text'])
                                      if term.startswith('@')]
                        # Count terms only (no hashtags, no mentions)
                        terms_only = [term for term in preprocess(tweet['text'])
                                      if term not in stop and
                                      not term.startswith(('#', '@'))]
                                      # mind the ((double brackets))
                                      # startswith() takes a tuple (not a list) if
                                      # we pass a list of inputs
    		        coupon_flag = is_coupon(tweet, terms_all, search_word_dict)
    		        if 3 == coupon_flag:
    		            pass
    		        elif 0 == coupon_flag:
    			    f_coupon.write(line)
    			    f_coupon.write("\n")
    		        elif 1 == coupon_flag:
    			    f_likecoupon.write(line)
    			    f_likecoupon.write("\n")
    		        elif 2 == coupon_flag:
    			    f_coupon_bigcat.write(line)
    			    f_coupon_bigcat.write("\n")
    		        elif tuple == type(coupon_flag) and 4 == coupon_flag[0]:
    			    index_dict = line.find("{")
    			    if -1 != index_dict:
    			        f_coupon_blacklist.write(line[0:index_dict+1] + """"black_reason":"%s",""" % coupon_flag[1] + line[index_dict+1:])
    			        f_coupon_blacklist.write("\n")
    
                        terms_bigram = bigrams(terms_all)
                        
    #		    # Build co-occurrence matrix
    #		    for i in range(len(terms_all)-1):            
    #		        for j in range(i+1, len(terms_all)):
    #		            w1, w2 = sorted([terms_all[i], terms_all[j]])                
    #		            if w1 != w2:
    #		                com[w1][w2] += 1
    		    
                        # Update the counter
                        count_all.update(terms_all)
                        count_hash.update(terms_hash)
                        count_user.update(terms_user)
                        count_only.update(terms_only)
                        count_bigram.update(terms_bigram)
    		    elif "en" == tweet["lang"] and tweet.has_key("retweeted_status"):
    		        tweet = tweet["retweeted_status"]
    		        n_docs += 1
                        # Create a list with all the terms
                        terms_all = [term for term in preprocess(tweet['text']) if term not in stop]
                        # Count hashtags only
                        terms_hash = [term for term in preprocess(tweet['text'])
                                      if term.startswith('#')]
                        # Count user only
                        terms_user = [term for term in preprocess(tweet['text'])
                                      if term.startswith('@')]
                        # Count terms only (no hashtags, no mentions)
                        terms_only = [term for term in preprocess(tweet['text'])
                                      if term not in stop and
                                      not term.startswith(('#', '@'))]
                                      # mind the ((double brackets))
                                      # startswith() takes a tuple (not a list) if
                                      # we pass a list of inputs
                        coupon_flag = is_coupon(tweet, terms_all, search_word_dict)
                        if 3 == coupon_flag:
                            pass
    		        elif 0 == coupon_flag:
    			    retweet_coupon[tweet["id_str"]] = line
                        elif 1 == coupon_flag:
                            retweet_likelycoupon[tweet["id_str"]] = line
                        elif 2 == coupon_flag:
                            retweet_bigcat[tweet["id_str"]] = line
                        elif tuple == type(coupon_flag) and 4 == coupon_flag[0]:
    			    index_dict = line.find("{")
    			    if -1 != index_dict:
    			        retweet_blacklist[tweet["id_str"]] = line[0:index_dict+1] + """"black_reason":"%s",""" % coupon_flag[1] + line[index_dict+1:]
    
    #                    if 3 == coupon_flag:
    #                        pass
    #                    elif 0 == coupon_flag:
    #                        f_coupon_retweet.write(line)
    #                        f_coupon_retweet.write("\n")
    #                    elif 1 == coupon_flag:
    #                        f_likecoupon_retweet.write(line)
    #                        f_likecoupon_retweet.write("\n")
    #                    elif 2 == coupon_flag:
    #                        f_coupon_bigcat_retweet.write(line)
    #                        f_coupon_bigcat_retweet.write("\n")
    #                    elif tuple == type(coupon_flag) and 4 == coupon_flag[0]:
    #                        index_dict = line.find("{")
    #                        if -1 != index_dict:
    #                            f_coupon_blacklist_retweet.write(line[0:index_dict+1] + """"black_reason":"%s",""" % coupon_flag[1] + line[index_dict+1:])
    #                            f_coupon_blacklist_retweet.write("\n")
    
                        terms_bigram = bigrams(terms_all)
    		        count_all.update(terms_all)
                        count_hash.update(terms_hash)
                        count_user.update(terms_user)
                        count_only.update(terms_only)
                        count_bigram.update(terms_bigram)
    
                    else:
                        n_notOK += 1
                except Exception, e:
    #		print e
    #		print line
    		    pass
    
    f_coupon.close()
    f_likecoupon.close()
    f_coupon_bigcat.close()
    f_coupon_blacklist.close()
    
    for item_coupon in retweet_coupon:
        f_coupon_retweet.write(retweet_coupon[item_coupon])
        f_coupon_retweet.write("\n")
    for item_likely in retweet_likelycoupon:
        f_likecoupon_retweet.write(retweet_likelycoupon[item_likely])
        f_likecoupon_retweet.write("\n")
    for item_bigcat in retweet_bigcat:
        f_coupon_bigcat_retweet.write(retweet_bigcat[item_bigcat])
        f_coupon_bigcat_retweet.write("\n")
    for item_black in retweet_blacklist:
        f_coupon_blacklist_retweet.write(retweet_blacklist[item_black])
        f_coupon_blacklist_retweet.write("\n")
    
    f_coupon_retweet.close()
    f_likecoupon_retweet.close()
    f_coupon_bigcat_retweet.close()
    f_coupon_blacklist_retweet.close()
    
    
    
    
    ####emotion, cowords calc
    #p_t = {}
    #p_t_com = defaultdict( lambda : defaultdict(int) )
    #
    #for term, n in count_all.items():
    #    p_t[term] = float(n) / n_docs
    #    for t2 in com[term]:
    #        p_t_com[term][t2] = float(com[term][t2]) / n_docs
    #positive_vocab = [
    #    # 'coupon', 'discount', 'couponcode', 'code', #, ':)', ':-)',
    #    'good', 'nice', 'great', 'awesome', 'outstanding',
    #    'fantastic', 'terrific', ':)', ':-)', 'like', 'love',
    #    'amazing', 'god',
    #    # shall we also include game-specific terms?
    #    # 'triumph', 'triumphal', 'triumphant', 'victory', etc.
    #]
    #negative_vocab = [
    #    # ':(', ':-(',
    #    'bad', 'terrible', 'crap', 'useless', 'hate', ':(', ':-(',
    #    # 'defeat', etc.
    #]
    #
    #pmi = defaultdict( lambda : defaultdict(int) )
    #for t1 in p_t:
    #    for t2 in com[t1]:
    #        # print p_t[t1],  p_t[t2]
    #        denom = p_t[t1] * p_t[t2]
    #        pmi[t1][t2] = math.log(p_t_com[t1][t2] / denom, 2)
    #
    #semantic_orientation = {}
    #for term, n in p_t.items():
    #    # print n
    #    positive_assoc = sum(pmi[term][tx] for tx in positive_vocab)
    #    negative_assoc = sum(pmi[term][tx] for tx in negative_vocab)
    #    semantic_orientation[term] = positive_assoc - negative_assoc
    #
    #semantic_sorted = sorted(semantic_orientation.items(), key=operator.itemgetter(1), reverse=True)
    #top_pos = semantic_sorted[:100]
    #top_neg = semantic_sorted[-100:]
    #print_list( top_pos, "Top positive word" )
    #print_list( top_neg, "Top negative word" )
    #
    #print
    ## print "Team Cavaliers: %f" % semantic_orientation["#Cavs"]
    ## print "Team Raptors: %f" % semantic_orientation["#Raptors"]
    ## print "Team Thunder: %f" % semantic_orientation["Thunder"]
    ## print "Team Warriors: %f" % semantic_orientation["Warriors"]   
    #
    ## Print the first 5 most frequent words
    ## Count terms only once, equivalent to Document Frequency
    ## terms_single = set(terms_all)
    #com_max = []
    ## For each term, look for the most common co-occurrent terms
    #for t1 in com:
    #    t1_max_terms = sorted(com[t1].items(), key=operator.itemgetter(1), reverse=True)[:100]
    #    for t2, t2_count in t1_max_terms:
    #        com_max.append( ((t1, t2),t2_count) )
    ## Get the most frequent co-occurrences
    #terms_max = sorted(com_max, key=operator.itemgetter(1), reverse=True)
    #print_listin( terms_max[:100], "Top co-word")
    #
    
    
    
    
    
    
    #for search_word in search_word_dict:
    #    print_list( search_word_dict[search_word].most_common(100), "Co-occurrence for %s" % search_word)
    #    print
    
    #print_listin( count_bigram.most_common(100), "Top co-word-together")
    #
    #print_list(  count_all.most_common(100), "Top all Words")
    #print
    #print_list(  count_user.most_common(100), "Top Users")
    #print
    #print_list(  count_hash.most_common(100), "Top Tags")
    #print
    #print_list(  count_only.most_common(100), "Top pure words")
    #print
    print "total: ", n_nodelete + n_delete
    print "n_docs: ", n_docs
    print "n_notOK: ", n_notOK
    print "n_retweet: ", n_retweet
    print "n_delete: ", n_delete
    print "n_nodelete: " , n_nodelete
    print "n_coupon: ", n_coupon
    
    ##f_test = open("coupon_for_review_everycase.txt", "w")
    ##for a in coupon_model_all:
    ##    f_test.write("\n\n\n\n***********"+str(a)+"***********("+str(len(coupon_model_all[a]))+")\n")
    ###    print a
    ###    print
    ##    for b in coupon_model_all[a]:
    ##	f_test.write("****"+b.encode("utf-8"))
    ##	f_test.write("\n\n")
    ###	print b
    ###	print
    ##f_test.close()
