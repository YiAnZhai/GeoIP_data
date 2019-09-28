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
 
    r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
    r'(?:[\w_]+)', # other words
    r'(?:\S)' # anything else
]
    
tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
 
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

punctuation = list(string.punctuation)
stop = stopwords.words('english') + punctuation + ['rt', 'via']
stop = []
search_word_list = sys.argv[1].split(",") # pass a term as a command-line argument
search_word_dict = {search_word_list[i]:Counter() for i in range(len(search_word_list))}
com = defaultdict(lambda : defaultdict(int))

fname = '/home/moma/Documents/python_documents/twitter/stream_data/sample/sample-20160602-1307->20160602-1531.txt'
#dirname = '/disk1/data/twitter/data_twitter_sample/'
dirname = "/disk1/data/twitter/data_twitter_coupontag/"
n_docs = 0
n_noeng = 0
n_retweet = 0
n_delete = 0
n_nodelete = 0
count_all = Counter()
count_hash = Counter()
count_user = Counter()
count_only = Counter()
count_bigram = Counter()
for child_file in os.listdir(dirname)[:]:
    print "file", child_file
    fname = '%s%s' % (dirname, child_file)
#    fname = '/disk1/data/twitter/data_twitter_sample_history/sample/sample-20160531-0107->20160531-0331.txt'
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
                    for search_word in search_word_dict:
#			if search_word in terms_all:
			if search_word in tweet['text'].lower():
                            search_word_dict[search_word].update(terms_all)

                    terms_bigram = bigrams(terms_all)
                    # Build co-occurrence matrix
                    # for i in range(len(terms_all)-1):            
                    #     for j in range(i+1, len(terms_all)):
                    #         w1, w2 = sorted([terms_all[i], terms_all[j]])                
                    #         if w1 != w2:
                    #             com[w1][w2] += 1

                    # Update the counter
                    count_all.update(terms_all)
                    count_hash.update(terms_hash)
                    count_user.update(terms_user)
                    count_only.update(terms_only)
                    count_bigram.update(terms_bigram)
                else:
                    n_noeng += 1
            except Exception, e:
                pass

# p_t = {}
# p_t_com = defaultdict( lambda : defaultdict(int) )
 
# for term, n in count_all.items():
#     p_t[term] = float(n) / n_docs
#     for t2 in com[term]:
#         p_t_com[term][t2] = float(com[term][t2]) / n_docs
# positive_vocab = [
#     # 'coupon', 'discount', 'couponcode', 'code', #, ':)', ':-)',
#     'good', 'nice', 'great', 'awesome', 'outstanding',
#     'fantastic', 'terrific', ':)', ':-)', 'like', 'love',
#     'amazing', 'god',
#     # shall we also include game-specific terms?
#     # 'triumph', 'triumphal', 'triumphant', 'victory', etc.
# ]
# negative_vocab = [
#     # ':(', ':-(',
#     'bad', 'terrible', 'crap', 'useless', 'hate', ':(', ':-(',
#     # 'defeat', etc.
# ]

# pmi = defaultdict( lambda : defaultdict(int) )
# for t1 in p_t:
#     for t2 in com[t1]:
#         # print p_t[t1],  p_t[t2]
#         denom = p_t[t1] * p_t[t2]
#         pmi[t1][t2] = math.log(p_t_com[t1][t2] / denom, 2)
 
# semantic_orientation = {}
# for term, n in p_t.items():
#     # print n
#     positive_assoc = sum(pmi[term][tx] for tx in positive_vocab)
#     negative_assoc = sum(pmi[term][tx] for tx in negative_vocab)
#     semantic_orientation[term] = positive_assoc - negative_assoc

# semantic_sorted = sorted(semantic_orientation.items(), key=operator.itemgetter(1), reverse=True)
# top_pos = semantic_sorted[:100]
# top_neg = semantic_sorted[-100:]
# print_list( top_pos, "Top positive word" )
# print_list( top_neg, "Top negative word" )

# print
# # print "Team Cavaliers: %f" % semantic_orientation["#Cavs"]
# # print "Team Raptors: %f" % semantic_orientation["#Raptors"]
# # print "Team Thunder: %f" % semantic_orientation["Thunder"]
# # print "Team Warriors: %f" % semantic_orientation["Warriors"]   

# # Print the first 5 most frequent words
# # Count terms only once, equivalent to Document Frequency
# # terms_single = set(terms_all)
# com_max = []
# # For each term, look for the most common co-occurrent terms
# for t1 in com:
#     t1_max_terms = sorted(com[t1].items(), key=operator.itemgetter(1), reverse=True)[:100]
#     for t2, t2_count in t1_max_terms:
#         com_max.append( ((t1, t2),t2_count) )
# # Get the most frequent co-occurrences
# terms_max = sorted(com_max, key=operator.itemgetter(1), reverse=True)
# print_list( terms_max[:100], "Top co-word")

print_list( count_bigram.most_common(100), "Top co-word-together")
for search_word in search_word_dict:
    print_list( search_word_dict[search_word].most_common(100), "Co-occurrence for %s" % search_word)
    print

print_list( count_bigram.most_common(100), "Top co-word-together")

print_list(  count_all.most_common(100), "Top all Words")
print
print_list(  count_user.most_common(100), "Top Users")
print
print_list(  count_hash.most_common(100), "Top Tags")
print
print_list(  count_only.most_common(100), "Top pure words")
print
print "total: ", n_nodelete + n_delete
print "n_docs: ", n_docs
print "n_noeng: ", n_noeng
print "n_retweet: ", n_retweet
print "n_delete: ", n_delete
print "n_nodelete: " , n_nodelete
