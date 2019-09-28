# -*- coding: utf-8 -*-
import scrapy
from coupon import settings
import sys
sys.path.append(settings.TOOL_PATH)
import utils
import xmltodict
import json
from coupon.items import AdvertiserItem
from scrapy.http import FormRequest
import re

class AddNewAdvertiserSpider(scrapy.Spider):
    name = "add_new_advertiser"
#    allowed_domains = ["abc.com"]
    start_urls = (
#        'http://www.abc.com/',
    )

    custom_settings = {
	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,},
	}

    def __init__(self):
	self.cj_baseurl = "https://advertiser-lookup.api.cj.com/v3/advertiser-lookup?advertiser-ids="
	self.sas_baseurl = "https://account.shareasale.com/affiliateAjax.cfc?returnFormat=plain&method=displayMerchantInfo&storeId=0&_cf_nodebug=true&_cf_nocache=true&_cf_rc=1&merchantId=%s&_cf_containerId=MER_%s_body"

        self.developerKey = '00bcb8357a9be9409e11d51eb3de6a0c5de1eb7ba5fd3d852d4a19a086c32270e9b5c1038f13bc500b3f2eae54074f5afb56d2b8f5b61b25174a1a15c66656d4c1/3ca5a2220ddaaeb65d9ab4ac2f0cbb7eac2ca5e6d8f41d4949a7e4ab8a28164aa753cbbd221d6bd20e93b5a808d7940e339e710c28689871c2d7d81b634ad1f1'   
#	self.developerKey = '008e65525d82416c3a2ffe2b16de9358d2f226978504c679de783f0b39bfeff5b7497fd5812a755605d0f90e10100726d5059587e988c02493f7fc308305642ffd/25abd4807c8746549681cd707d6dc9dff4c414edd1f798222cf9b0c4946cad3a6c4dd7966cd75570b32ebc57ef848e7d13a718adb3f2b2860224119ee784cdd9'
	self.userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2236.0 Safari/537.36"

    def start_requests(self):
	url = "https://account.shareasale.com/a-login.cfm"
        formdata = {'step2':'True','redirect':'a-main.cfm','username':'cathyca','nuisance':'thanks','gadgetry':'azimuth',\
                    'drafter':'millionth','jq1':'overawe','password':'MOMAcnbeta#1102','glad-hand':'dispirit',\
                    'glad-hand':'dispirit'}
        request = FormRequest(url, callback = self.parse, formdata = formdata)
        yield request

	
    def parse(self, response):
	print response.url
	host = settings.MYSQL_HOST
	user = settings.MYSQL_USER
	passwd = settings.MYSQL_PASSWD
	db = settings.MYSQL_DBNAME
	
	sql = 'SELECT A.source, A.advertiserid FROM coupon_api_today A LEFT JOIN cj_sas_linkshare_brand B ON A.advertiserid = B.advertiserid AND A.source = B.source WHERE B.advertiserid IS NULL'

	con, cur = utils.connect_db(host, user, passwd, db)
	data_list = utils.select_db(cur, sql)
	print len(data_list), data_list[:2]

	headers = {'authorization':self.developerKey, 'User-agent':self.userAgent}
	count = 0

	cj_set = set()
	sas_set = set()
	for data in data_list[:]:
		advertiserid = data[1]
		if data[0] == 'cj':
			if advertiserid in cj_set:
				continue
			cj_set.add(advertiserid)
			url = self.cj_baseurl + str(advertiserid)
			print count
			print url
			print
			count += 1
			req = scrapy.Request(url, callback = self.parse_cj, headers = headers)
			yield req

		elif data[0] == 'shareasale':
			if advertiserid in sas_set:
				continue
			sas_set.add(advertiserid)
			url = self.sas_baseurl % (advertiserid, advertiserid)
			print count
			print url
			print
			count += 1
			req = scrapy.Request(url, callback = self.parse_sas)
			req.meta['advertiserid'] = advertiserid
			yield req

    def parse_cj(self, response):
	advertisers_xml = xmltodict.parse(response.body)
        advertisers_str = json.dumps(advertisers_xml)
        advertisers_dict = json.loads(advertisers_str)
#       print advertisers_dict  
        advertiser = advertisers_dict['cj-api']['advertisers']
	if 'advertiser' in advertiser:
		advertiser = advertiser['advertiser']
		advertiserid = ''
		program_url = ''
		if 'advertiser-id' in advertiser:
			advertiserid = advertiser['advertiser-id']
	#                if int(advertiser_id) in self.existed_advertisers:
	#                        print 'existed in db ---------'
	#                        continue
			if 'program-url' in advertiser:
				program_url = advertiser['program-url']
		
		print advertiserid
		print program_url
		
		program_url = re.split(r'[,; ]', program_url)[0]	
		if advertiserid:
			item = AdvertiserItem()
			item['source'] = 'cj'
			item['advertiserid'] = advertiserid
			item['program_url'] = program_url
			item['domain'] = utils.get_domain_from_url(program_url)
#			print item
			yield item
#
    def parse_sas(self, response):
		advertiserid = response.meta['advertiserid']	
		program_url = response.css("#dspHead a::text").extract_first(default = '').strip()
		if program_url == 'CLOSE X':
			url = 'https://account.shareasale.com/a-viewmerchant.cfm?merchantID=%s' % advertiserid
			req = scrapy.Request(url, callback = self.parse_sas_2)
			req.meta['advertiserid'] = advertiserid
			yield req
		else:		
			program_url = re.split(r'[,; ]', program_url)[0]	
			if '@' in program_url:
				program_url = program_url.rsplit('@', 1)[-1]
			if advertiserid:
				item = AdvertiserItem()
				item['source'] = 'shareasale'
				item['advertiserid'] = advertiserid
				item['program_url'] = program_url
				item['domain'] = utils.get_domain_from_url(program_url)
	#			print item
				yield item

    def parse_sas_2(self, response):
	program_url = response.xpath("//tbody/tr[1]/td/a[2]/text()").extract_first(default = "").strip()
	advertiserid = response.meta['advertiserid']
	if program_url:
		program_url = re.split(r'[,; ]', program_url)[0]	
		if '@' in program_url:
			program_url = program_url.rsplit('@', 1)[-1]
		item = AdvertiserItem()
		item['source'] = 'shareasale'
		item['advertiserid'] = advertiserid
		item['program_url'] = program_url
		item['domain'] = utils.get_domain_from_url(program_url)
		yield item		
	
    def parse_ls(self, response):
	f = open('test.html', 'w')
	f.write(response.body)
	f.close()
	print response.url
	print response.body
