# -*- coding: utf-8 -*-

import scrapy
from scrapy.http import FormRequest
from coupon.items import CouponItem
from coupon import settings
import xmltodict
import json
import MySQLdb as mdb
import datetime
import datetime
import re
from hashlib import md5
#import utils

class CjSpider(scrapy.Spider):
#     name = "cj_coupon"
#     allowed_domains = ["cj.com"]
#     start_urls = (
# #        'http://www.shareasale.com/',
#     )
#     custom_settings = {
# 	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,
# 			'coupon.pipelines.DataProcessingPipeline':50,},
# #	"MYSQL_HOST" : '192.168.8.222',
# #	"MYSQL_DBNAME" : 'coupon_datacenter',
# #	"MYSQL_USER" : 'root',
# #	"MYSQL_PASSWD" : 'moma',
#     }
#     recordsPerPage = 100
# #    website_id = 7860417
#     website_id = 8067612
#     #developerKey = '008e65525d82416c3a2ffe2b16de9358d2f226978504c679de783f0b39bfeff5b7497fd5812a755605d0f90e10100726d5059587e988c02493f7fc308305642ffd/25abd4807c8746549681cd707d6dc9dff4c414edd1f798222cf9b0c4946cad3a6c4dd7966cd75570b32ebc57ef848e7d13a718adb3f2b2860224119ee784cdd9'   
#     developerKey = '00bcb8357a9be9409e11d51eb3de6a0c5de1eb7ba5fd3d852d4a19a086c32270e9b5c1038f13bc500b3f2eae54074f5afb56d2b8f5b61b25174a1a15c66656d4c1/3ca5a2220ddaaeb65d9ab4ac2f0cbb7eac2ca5e6d8f41d4949a7e4ab8a28164aa753cbbd221d6bd20e93b5a808d7940e339e710c28689871c2d7d81b634ad1f1'   
#     userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2236.0 Safari/537.36"
#     types = ['coupon', 'product', 'sale/discount', 'free shipping']

#     #baseurl="https://linksearch.api.cj.com/v2/link-search?website-id=%s&records-per-page=%d&promotion-type=%s" % (website_id, recordsPerPage)
#     baseurl="https://linksearch.api.cj.com/v2/link-search?website-id=%s&records-per-page=%d&promotion-type=%s"
#     baseurl_list = [baseurl%(website_id, recordsPerPage, item) for item in types] 

#     con = mdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, passwd=settings.MYSQL_PASSWD, db=settings.MYSQL_DBNAME, charset='utf8')
#     cur = con.cursor()
#     sql = 'select linkid from linkid where source = "cj"'
#     cur.execute(sql)
#     existed_coupons = cur.fetchall()
#     existed_coupons = [item[0] for item in existed_coupons]
#     ##########  affiliate unique
#     joined_sql = 'select linkid from coupon_api_affiliate_status where source = "cj" and affiliate_status="joined"'
#     cur.execute(joined_sql)
#     joined_existed_coupons = cur.fetchall()
#     joined_existed_coupons = [item[0] for item in joined_existed_coupons]
#     cur.close()
#     con.close()
#     today = datetime.datetime.now().strftime('%Y-%m-%d')

    def start_requests(self):
	for item in self.baseurl_list:
		url = item
		headers = {'authorization':self.developerKey, 'User-agent':self.userAgent}
		request = scrapy.Request(url, callback = self.parse, headers = headers)
		request.meta['url'] = item
		yield request

    def parse(self, response):
	coupons_xml = xmltodict.parse(response.body)
	coupons_str = json.dumps(coupons_xml)
	coupons_dict = json.loads(coupons_str)

	totalRecords = int(coupons_dict['cj-api']['links']['@total-matched'])
	coupons = coupons_dict['cj-api']['links']['link']
	if totalRecords%self.recordsPerPage == 0:
	    numPages = totalRecords / self.recordsPerPage
	else:
	    numPages = totalRecords / self.recordsPerPage + 1		
	print "Total coupons: %d, coupons per page: %d, Number of pages: %d" % (totalRecords, self.recordsPerPage, numPages)

	current_url = response.meta['url']
	if 'free shipping' in current_url:
		freeshipping = 1
	else:
		freeshipping = 0
	for i in range(1, numPages+1):
		print "Page number: %d" % i
		url = current_url  + ("&page-number=%d" % i)
		headers = {'authorization':self.developerKey, 'User-agent':self.userAgent}
		req = scrapy.Request(url, callback = self.parse_page, headers = headers)
		req.meta['freeshipping'] = freeshipping
		yield req

    def parse_page(self, response):
	coupons_xml = xmltodict.parse(response.body)
	coupons_str = json.dumps(coupons_xml)
	coupons_dict = json.loads(coupons_str)
#	print advertisers_dict	
	coupons = coupons_dict['cj-api']['links']['link']

	freeshipping = response.meta['freeshipping']
	for coupon in coupons[:]:
		advertiser_id = ''
		advertiser_name = ''
		category = ''
		click_commission = ''
		creative_height = '0'
		creative_width = '0'
		language = ''
		lead_commission = ''
		description = ''
		destination = ''
		link_id = ''
		link_name = ''
		link_type = ''
		performance_incentive = ''
		promotion_end_date = ''
		promotion_start_date = ''
		promotion_type = ''
		coupon_code = ''
		sale_commission = ''
		seven_day_epc = ''
		three_month_epc = ''
		affiliate_status =''
		is_new = 1

		if 'relationship-status' in coupon:
			affiliate_status = coupon['relationship-status']
		if 'link-id' in coupon:
			link_id = coupon['link-id']
			if str(link_id) in self.joined_existed_coupons:
				continue
			print link_id, str(link_id) in self.existed_coupons, 'hhhhhhhhhhhh'
			if str(link_id) in self.existed_coupons:
				print 'existed in db'
				if affiliate_status=='joined':
					print "#####################affiliate coupon"
					is_new = 0
				else:
					continue
		if 'advertiser-id' in coupon:
			advertiser_id = coupon['advertiser-id']
		if 'advertiser-name' in coupon:
			advertiser_name = coupon['advertiser-name']
			if advertiser_name:
				advertiser_name = advertiser_name.encode('utf-8')
			else:
				advertiser_name = ""
		if 'category' in coupon:
			category = coupon['category']
			if category:
				category = category.encode('utf-8')
			else:
				category = ""
		if 'click-commission' in coupon:
			click_commission = coupon['click-commission']
		if 'creative-height' in coupon:
			creative_height = coupon['creative-height']
		if 'creative-width' in coupon:
			creative_width = coupon['creative-width']
		if 'language' in coupon:
			language = coupon['language']
			if language != 'en':
				continue
		if 'lead-commission' in coupon:
			lead_commission = coupon['lead-commission']
		if 'description' in coupon:
			description = coupon['description']
			pattern1 = creative_width+'x'+creative_height
			pattern2 = creative_width+'X'+creative_height
			pattern3 = creative_width+' x '+creative_height
			pattern4 = creative_width+' X '+creative_height
			pattern5 = creative_width+'\*'+creative_height

			pattern = '%s|%s|%s|%s|%s' % (pattern1, pattern2, pattern3, pattern4, pattern5)
			title = re.sub(pattern, '', description)
			title = re.sub('\(\)', '', title).strip()
			if title.endswith('_'):
				title = title[:-1].strip()
			if title.endswith('-'):
				title = title[:-1].strip()
			if title.startswith('-'):
				title = title[1:].strip()
			if title.startswith('_'):
				title = title[1:].strip()
			title = re.sub('__', '_', title)
			description = title	
			
		if 'link-type' in coupon:
			link_type = coupon['link-type']
		if 'performance-incentive' in coupon:
			performance_incentive = coupon['performance-incentive']
		if 'clickUrl' in coupon:
			destination = coupon['clickUrl']
		else:
			if 'destination' in coupon:
				destination = coupon['destination']
		if 'link-name' in coupon:
			link_name = coupon['link-name']
			pattern1 = creative_width+'x'+creative_height
			pattern2 = creative_width+'X'+creative_height
			pattern3 = creative_width+' x '+creative_height
			pattern4 = creative_width+' X '+creative_height
			pattern5 = creative_width+'\*'+creative_height

			pattern = '%s|%s|%s|%s|%s' % (pattern1, pattern2, pattern3, pattern4, pattern5)
			title = re.sub(pattern, '', link_name)
			title = re.sub('\(\)', '', title).strip()
			if title.endswith('_'):
				title = title[:-1].strip()
			if title.endswith('-'):
				title = title[:-1].strip()
			if title.startswith('-'):
				title = title[1:].strip()
			if title.startswith('_'):
				title = title[1:].strip()
			title = re.sub('__', '_', title)
			link_name = title	
			
		if 'link-type' in coupon:
			link_type = coupon['link-type']
		if 'performance-incentive' in coupon:
			performance_incentive = coupon['performance-incentive']
		if 'promotion-end-date' in coupon:
			promotion_end_date = coupon['promotion-end-date']
			if promotion_end_date:
				if promotion_end_date.endswith('00:00:00.0'):
					promotion_end_date = promotion_end_date[:10]
				else:
					promotion_end_date = promotion_end_date[:16]
			if promotion_end_date < self.today:
				continue
		if 'promotion-start-date' in coupon:
			promotion_start_date = coupon['promotion-start-date']
			if promotion_start_date:
				if promotion_start_date.endswith('00:00:00.0'):
					promotion_start_date = promotion_start_date[:10]
				else:
					promotion_start_date = promotion_start_date[:16]
		if 'promotion-type' in coupon:
			promotion_type = coupon['promotion-type']
		if 'coupon-code' in coupon:
			coupon_code = coupon['coupon-code']
		if 'sale-commission' in coupon:
			sale_commission = coupon['sale-commission']
		if 'seven-day-epc' in coupon:
			seven_day_epc = coupon['seven-day-epc']

		if 'three-month-epc' in coupon:
			three_month_epc = coupon['three-month-epc']

		if promotion_end_date and promotion_start_date:
			if promotion_end_date < promotion_start_date:
				continue		

		item = CouponItem()
		item['source'] = 'cj'
		item['advertiserid'] = advertiser_id
		item['advertisername'] = advertiser_name
		item['title'] = link_name
		item['description'] = description
		item['code'] = coupon_code
		item['end_date'] = promotion_end_date
		item['start_date'] = promotion_start_date
		item['url'] = destination
		item['category'] = category
		item['restriction'] = ''
		item['keywords'] = ''
		item['htmlofdeal'] = ''
		item['promotiontypes'] = ''
		item['linkid'] = link_id
		if freeshipping == 1:
			item['freeshipping'] = '1'
		item['affiliate_status'] = affiliate_status
		item['is_new'] = is_new
			
		yield item
		#item = CjCouponItem()
		#item['advertiser_id'] = advertiser_id
		#item['advertiser_name'] = advertiser_name
		#item['category'] = category
		#item['click_commission'] = click_commission
		#item['creative_height'] = creative_height
		#item['creative_width'] = creative_width
		#item['language'] = language
		#item['lead_commission'] = lead_commission
		#item['description'] = description
		#item['destination'] = destination
		#item['link_id'] = link_id
		#item['link_name'] = link_name
		#item['link_type'] = link_type
		#item['performance_incentive'] = performance_incentive
		#item['promotion_end_date'] = promotion_end_date
		#item['promotion_start_date'] = promotion_start_date
		#item['promotion_type'] = promotion_type
		#item['couponcode'] = coupon_code
		#item['sale_commission'] = sale_commission
		#item['seven_day_epc'] = seven_day_epc
		#item['three_month_epc'] = three_month_epc
		#yield item	

#    def get_guid(self, md5_string):
#	return md5(md5_string).hexdigest()

#    def close(reason):
#	#cur = con.cursor()
#	#sql = 'select count(*) from cj_coupon where add_date like %s'
#	#current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#	#cur.execute(sql, (current_date+"%", ))
#	#res = cur.fetchone()
#	#if res:
#	#	sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
#	#	cur.execute(sql_insert, ('cj_coupon', str(res[0]), datetime.datetime.now()))
#	#	con.commit()
#	#cur.close()
#	#con.close()
#
#	print 'helloooooooooooooo'
#	con = mdb.connect(host = '192.168.8.250', user = 'root', passwd = 'moma', db = 'haipeng')
#	cur = con.cursor()
#
#	current_date = datetime.datetime.now().strftime('%Y-%m-%d')
##	sql = """INSERT INTO coupon_api (source , advertiserid, brand, title, description, `code`, end_date, start_date, url, category, adddate) SELECT 'cj', cj_coupon.advertiser_id, cj_sas_linkshare_brand.brand, cj_coupon.link_name, cj_coupon.description, cj_coupon.coupon_code, cj_coupon.promotion_end_date, cj_coupon.promotion_start_date, cj_coupon.destination, cj_coupon.category,  cj_coupon.add_date FROM cj_coupon INNER JOIN cj_sas_linkshare_brand ON ( cj_coupon.advertiser_id = cj_sas_linkshare_brand.advertiserid ) WHERE cj_sas_linkshare_brand.source = 'cj' AND cj_coupon.advertiser_name is not null and cj_coupon.advertiser_name <> '' AND cj_coupon.advertiser_name <> '*' AND cj_coupon.add_date LIKE %s"""
#	sql = """SELECT 'cj', cj_coupon.advertiser_id, cj_sas_linkshare_brand.brand, cj_coupon.link_name, cj_coupon.description, cj_coupon.coupon_code, cj_coupon.promotion_end_date, cj_coupon.promotion_start_date, cj_coupon.destination, cj_coupon.category,  cj_coupon.add_date FROM cj_coupon INNER JOIN cj_sas_linkshare_brand ON ( cj_coupon.advertiser_id = cj_sas_linkshare_brand.advertiserid ) WHERE cj_sas_linkshare_brand.source = 'cj' AND cj_coupon.advertiser_name is not null and cj_coupon.advertiser_name <> '' AND cj_coupon.advertiser_name <> '*' AND cj_coupon.add_date LIKE %s"""
#	cur.execute(sql, (current_date+"%", ))
#	coupons = cur.fetchall()
#	
#	sql_check_exist = """select exists (select 1 from coupon_api where guid = %s)"""
#	sql_insert = """insert into coupon_api (source, advertiserid, brand, title, description, `code`, end_date, start_date, url, category, adddate, guid) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#
#	guid_set = set()
#	data_list = list()
#	for coupon in coupons:
#		md5_string = coupon[3]+coupon[4]+coupon[5]+coupon[8]
#		guid = self.get_guid(md5_string)
#		if guid in guid_set:
#			continue
#		guid_set.add(guid)
#		coupon = list(coupon)
#		coupon.append(guid)
#	
#		cur.execute(sql_check_exist, guid)
#		if not cur.fetchone()[0]:
#			cur.execute(sql_insert, coupon)
#			con.commit()
#
#	sql_unknown_brand = """INSERT INTO unknown_advertiser (source , advertiserid, advertisername, couponnum)
#SELECT 'cj' as source, advertiser_id as advertiserid, advertiser_name as advertisername, COUNT(*) as couponnum FROM cj_coupon
#WHERE cj_coupon.add_date LIKE %s AND cj_coupon.advertiser_id NOT in (SELECT DISTINCT cj_sas_linkshare_brand.advertiserid FROM cj_sas_linkshare_brand WHERE cj_sas_linkshare_brand.source = 'cj') GROUP BY advertiser_id, advertiser_name"""
#	cur.execute(sql_unknown_brand, (current_date+"%", ))
#	con.commit()
#	cur.close()
#	co.close()
