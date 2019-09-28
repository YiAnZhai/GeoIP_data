# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from coupon.items import AmazonCouponItem
import xmltodict
import json
import re
import time
import collections
import urllib
import hmac
import base64
import hashlib
import MySQLdb as mdb
import datetime

class AmazonSpider(scrapy.Spider):
    name = "amazon_coupon"
    allowed_domains = ["amazon.com"]
    start_urls = (
#        'http://www.shareasale.com/',
    )
    custom_settings = {
	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline':100,},
	'MYSQL_HOST':'192.168.8.240',
	'MYSQL_DBNAME':'haipeng',
	'MYSQL_USER':'root',
	'MYSQL_PASSWD':'moma',
	'DOWNLOAD_DELAY':1,
    }

    def __init__(self):
	userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2236.0 Safari/537.36"
	#    baseurl = "http://webservices.amazon.com/onca/xml?AWSAccessKeyId=AKIAJTTW2NK2N2U544LQ&AssociateTag=saveornever-20&Keywords=coupon&Operation=ItemSearch&ResponseGroup=ItemAttributes%2COfferFull%2COfferListings%2COffers%2CSalesRank&SearchIndex=Appliances&Service=AWSECommerceService&Sort=price&Timestamp=2015-12-25T07%3A22%3A17.000Z&Signature=a%2B6zAWIddky1R7b0MCys0nVAuv%2Fnuhwe0bGiMqsmP%2F8%3D"
	category_list = ["Appliances","ArtsAndCrafts","Automotive","Baby","Beauty","Blended","Books","Collectibles","Electronics","Fashion","FashionBaby","FashionBoys", "FashionGirls","FashionMen","FashionWomen","GiftCards","Grocery","HealthPersonalCare","HomeGarden","Industrial","KindleStore","LawnAndGarden","Luggage","MP3Downloads","Magazines","Merchants","MobileApps","Movies","Music","MusicalInstruments","OfficeProducts","PCHardware","PetSupplies","Software","SportingGoods","Tools","Toys","UnboxVideo","VideoGames","Wine","Wireless"]
	con = mdb.connect(host = '192.168.8.240', user = 'root', passwd = 'moma', db = 'haipeng')
	cur = con.cursor()
	sql_asins = 'select asin from amazon_coupon'
	cur.execute(sql_asins)
	existed_asins = cur.fetchall()
	existed_asins = [item[0] for item in existed_asins]

	cur.close()
	con.close()
 
    def get_amazon_url(self, page_num, cat):
        aws_access_key_id = "AKIAJTTW2NK2N2U544LQ"
        aws_secret_key = "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"
        endpoint = "webservices.amazon.com"
        uri = "/onca/xml"
        params_dict = {'Service':'AWSECommerceService', 'Operation':'ItemSearch', 'AWSAccessKeyId':'AKIAJTTW2NK2N2U544LQ', 'AssociateTag':'saveornever-20', 'SearchIndex':cat, 'Keywords':'coupon', 'Sort':'price', 'ResponseGroup':'ItemAttributes, Offers, SalesRank, EditorialReview', 'ItemPage':str(page_num)}
        if 'Timestamp' not in params_dict:
                params_dict['Timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))
        od = collections.OrderedDict(sorted(params_dict.items()))
        params_str = list()
        for item in od:
                params_str.append(urllib.quote(item)+"="+urllib.quote(od[item]))
        canonical_query_string = '&'.join(params_str)

        string_to_sign = "GET\n" + endpoint + "\n" + uri + "\n" + canonical_query_string
        signature = base64.b64encode(hmac.new(aws_secret_key, string_to_sign, hashlib.sha256).digest())
        request_url = "http://" + endpoint + uri + '?' + canonical_query_string + '&Signature=' + urllib.quote(signature)
	return request_url

    def start_requests(self):
	for category in self.category_list[:]:
		url = self.get_amazon_url(1, category)
#		headers = {}
		request = scrapy.Request(url, callback = self.parse)
		request.meta['cat'] = category
		yield request
#	f = open("/home/moma/Documents/amazon.txt", "r")
#	content = f.read()
#	f.close()
#	url_list = content.split('\n')[:-1]
#	print len(url_list), url_list[0], url_list[-1]
#	headers = {'User-agent':self.userAgent}
#	for url in url_list[:]:
#		yield scrapy.Request(url, callback = self.parse, headers = headers)

    def parse(self, response):
	print response.url, '------------------'
#	return
	coupons_xml = xmltodict.parse(response.body)
	coupons_str = json.dumps(coupons_xml)
	coupons_dict = json.loads(coupons_str)

#	print type(coupons_dict['ItemSearchResponse']['Items']['Item'])
	item_list = list()
	if 'Items' in coupons_dict['ItemSearchResponse']:
		for xx in coupons_dict['ItemSearchResponse']['Items']:
			print xx, 'hahahhhhhhh'
		if 'Item' in coupons_dict['ItemSearchResponse']['Items']:
			item_list = coupons_dict['ItemSearchResponse']['Items']['Item']
		if 'cat' in response.meta:
			print 'yessssssssssssssss'
			if 'TotalPages' in coupons_dict['ItemSearchResponse']['Items']:
				totalPages = int(coupons_dict['ItemSearchResponse']['Items']['TotalPages'])
				print totalPages, '-----------------'
				for i in range(2, min(11, totalPages+1)):
					url = self.get_amazon_url(i, response.meta['cat'])
					yield scrapy.Request(url, callback = self.parse)

	for item in item_list:
		link = ''
		title = ''
		last_price = ''
		price = ''
		sales_rank = 0
		saved_price = ''
		saved_percent = ''
		coupon_cannon = ''
		product_description = ''
		cat = ''
		brand = ''
		merchant = ''
		asin = ''
		
		try:
			asin = item['ASIN']
		except Exception, e:
			asin = ""
			print 'error0', e
		if asin in self.existed_asins:
			print asin, 'existed in db'
			continue
		try:
			link = item['DetailPageURL']
		except Exception, e:
			link = ""
			print 'error1', e
		try:
			title = item['ItemAttributes']['Title']
		except Exception, e:
			title = ""
			print 'error2', e
		try:
			last_price = item['ItemAttributes']['ListPrice']['FormattedPrice']
		except Exception, e:
			last_price = ""
			print 'error3', e
		try:
			price = item['Offers']['Offer']['OfferListing']['Price']['FormattedPrice']
		except Exception, e:
			price = ""
			print 'error4', e
		try:
			sales_rank = int(item['SalesRank'])
		except Exception, e:
			sales_rank = 0
			print 'error5', e
		try:
			saved_price = item['Offers']['Offer']['OfferListing']['AmountSaved']['FormattedPrice']
		except Exception, e:
			saved_price = ""
			print 'error6', e
		try:
			saved_percent = item['Offers']['Offer']['OfferListing']['PercentageSaved']
		except Exception, e:
			saved_percent = ""
			print 'error7', e
		try:
			feature_list = item['ItemAttributes']['Feature']
			for feature in feature_list:
				if 'Coupon Cannon' in feature:
					coupon_cannon_list = re.findall('Coupon Cannon -(.*)', feature)
					if coupon_cannon_list:
						coupon_cannon = coupon_cannon_list[0].strip()
					break
		except Exception, e:
			coupon_cannon = ""
			print 'error8', e
		try:
			product_description = item['EditorialReviews']['EditorialReview']['Content']
		except Exception, e:
#			print item['editorialReviews']['EditorialReview']
			product_description = ""
			print 'error9', e
		try:
			cat = item['ItemAttributes']['Binding']
		except Exception, e:
			cat = ''
			print 'error10', e
		try:
			brand = item['ItemAttributes']['Brand']
		except Exception, e:
			brand = ""
			print 'error11', e
		try: 
			merchant = item['Offers']['Offer']['Merchant']['Name']
		except Exception, e:
			merchant = ""
			print 'error12', e

		amazonitem = AmazonCouponItem()
		amazonitem['asin'] = asin
		amazonitem['link'] = link
		amazonitem['title'] = title
		amazonitem['last_price'] = last_price
		amazonitem['price'] = price
		amazonitem['saved_price'] = saved_price
		amazonitem['saved_percent'] = saved_percent
		amazonitem['coupon_cannon'] = coupon_cannon
		amazonitem['product_description'] = product_description
		amazonitem['sales_rank'] = sales_rank
		amazonitem['cat'] = cat
		amazonitem['brand'] = brand
		amazonitem['merchant'] = merchant
		yield amazonitem

#    def close(reason):
#	con = mdb.connect(host = '192.168.8.240', user = 'root', passwd = 'moma', db = 'haipeng')
#        cur = con.cursor()
#        sql = 'select count(*) from amazon_coupon where add_date like %s'
#        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#        cur.execute(sql, (current_date+"%", ))
#        res = cur.fetchone()
#        if res:
#                sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
#                cur.execute(sql_insert, ('amazon_coupon', str(res[0]), datetime.datetime.now()))
#                con.commit()
#        cur.close()
#        con.close()

#		print 'link', link
#		print 'title', title
#		print 'last_price', last_price
#		print 'price', price
#		print 'saved_price', saved_price
#		print 'saved_percent', saved_percent
#		print 'coupon_cannon', coupon_cannon
##		print 'product_description', product_description	
#		print 'sales_rank', sales_rank
#		print 'cat', cat
#		print 'brand', brand
#		print 'merchant', merchant
#	f = open('hello.html', 'w')
#	f.write(coupons_str)
#	f.close()
##	print response.body

