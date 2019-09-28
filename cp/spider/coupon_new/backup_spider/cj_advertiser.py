# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from coupon.items import CjAdvertiserItem
import xmltodict
import json
import MySQLdb as mdb
import datetime

class ShareasaleSpider(scrapy.Spider):
    name = "cj_advertiser"
    allowed_domains = ["cj.com"]
    start_urls = (
#        'http://www.shareasale.com/',
    )
    custom_settings = {
	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline':100,},
	'MYSQL_HOST':'192.168.8.240',
	'MYSQL_DBNAME':'haipeng',
	'MYSQL_USER':'root',
	'MYSQL_PASSWD':'moma',
    }
    advertisersPerPage = 100
    website_id = 7860417
    developerKey = '008e65525d82416c3a2ffe2b16de9358d2f226978504c679de783f0b39bfeff5b7497fd5812a755605d0f90e10100726d5059587e988c02493f7fc308305642ffd/25abd4807c8746549681cd707d6dc9dff4c414edd1f798222cf9b0c4946cad3a6c4dd7966cd75570b32ebc57ef848e7d13a718adb3f2b2860224119ee784cdd9'   
    userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2236.0 Safari/537.36"
    baseurl = 'https://advertiser-lookup.api.cj.com/v3/advertiser-lookup?records-per-page=%d&advertiser-ids='
    con = mdb.connect(host = '192.168.8.240', user = 'root', passwd = 'moma', db = 'haipeng')
    cur = con.cursor()
    sql = 'select advertiser_id from cj_advertiser'
    cur.execute(sql)
    existed_advertisers = cur.fetchall()
    existed_advertisers = [item[0] for item in existed_advertisers]
    cur.close()
    con.close()

    def start_requests(self):
	url = self.baseurl % self.advertisersPerPage
	headers = {'authorization':self.developerKey, 'User-agent':self.userAgent}
        request = scrapy.Request(url, callback = self.parse, headers = headers)
	yield request

    def parse(self, response):
	total_advertisers = response.xpath("//advertisers/@total-matched").extract_first(default = '')
	print total_advertisers
	if total_advertisers:
		total_advertisers = int(total_advertisers)
	if total_advertisers%self.advertisersPerPage == 0:
		numPages = total_advertisers/self.advertisersPerPage
	else:
		numPages = total_advertisers/self.advertisersPerPage + 1
	print 'Total advertisers: %d, advertisers per page: %d, number of pages: %d' % (total_advertisers, self.advertisersPerPage, numPages)
	
	for i in range(1, numPages+1):
		url = self.baseurl % self.advertisersPerPage + '&page-number=%d' % i
		headers = {'authorization':self.developerKey, 'User-agent':self.userAgent}
		yield scrapy.Request(url, callback = self.parse_advertiser, headers = headers)

    def parse_advertiser(self, response):
	advertisers_xml = xmltodict.parse(response.body)
	advertisers_str = json.dumps(advertisers_xml)
	advertisers_dict = json.loads(advertisers_str)
#	print advertisers_dict	
	advertisers = advertisers_dict['cj-api']['advertisers']['advertiser']
				
	for advertiser in advertisers:
		advertiser_id = ''
		account_status = ''
		seven_day_epc = ''
		three_month_epc = ''
		language = ''
		advertiser_name = ''
		program_url = ''
		mobile_tracking_certified = ''
		network_rank = ''
		category = ''
		performance_incentives = ''
		primary_category = ''
		if 'advertiser-id' in advertiser:
			advertiser_id = advertiser['advertiser-id']
			if int(advertiser_id) in self.existed_advertisers:
				print 'existed in db ---------'
				continue
		if 'program-url' in advertiser:
			program_url = advertiser['program-url']
		if 'advertiser-name' in advertiser:
			advertiser_name = advertiser['advertiser-name']
		if advertiser_name:
			advertiser_name = advertiser_name
		if 'account-status' in advertiser:
			account_status = advertiser['account-status']
		if 'seven-day-epc' in advertiser:
			seven_day_epc = advertiser['seven-day-epc']
		if 'three-month-epc' in advertiser:
			three_month_epc = advertiser['three-month-epc']
		if 'language' in advertiser:
			language = advertiser['language']
		if 'mobile-tracking-certified' in advertiser:
			mobile_tracking_certified = advertiser['mobile-tracking-certified']
		if 'network-rank' in advertiser:
			network_rank = advertiser['network-rank']
		if 'primary-category' in advertiser:
			primary_category = advertiser['primary-category']
			if 'parent' in primary_category:
				if primary_category['parent']:
					category += primary_category['parent']
			if 'child' in primary_category:
				if category != '':
					category += ';'
				if primary_category['child']:
					category += primary_category['child']
		if 'performance-incentives' in advertiser:	
			performance_incentives = advertiser['performance-incentives']
			if performance_incentives:
				performance_incentives = performance_incentives 
		
		item = CjAdvertiserItem()
		item['advertiser_id'] = advertiser_id
		item['advertiser_name'] = advertiser_name
		item['program_url'] = program_url
		item['category'] = category
    		item['account_status'] = account_status
		item['seven_day_epc'] = seven_day_epc
		item['three_month_epc'] = three_month_epc
		item['language'] = language
		item['mobile_tracking_certified'] = mobile_tracking_certified
		item['network_rank'] = network_rank
   		item['performance_incentives'] = performance_incentives
		yield item

#    def close(reason):
#	con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'coupon')
#        cur = con.cursor()
#        sql = 'select count(*) from cj_advertiser where add_date like %s'
#        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#        cur.execute(sql, (current_date+"%", ))
#        res = cur.fetchone()
#        if res:
#                sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
#                cur.execute(sql_insert, ('cj_advertiser', str(res[0]), datetime.datetime.now()))
#                con.commit()
#        cur.close()
#        con.close()
