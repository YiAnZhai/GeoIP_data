# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from coupon.items import CouponItem
import xmltodict
import urllib2
import json
import MySQLdb as mdb
import datetime
import re
from coupon import settings

class ShareasaleSpider(scrapy.Spider):
# 	name = "shareasale_coupon"
# 	allowed_domains = ["shareasale.com"]
# 	start_urls = (
# #        'http://www.shareasale.com/',
# 	)
# 	custom_settings = {
# 	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,
# 			'coupon.pipelines.DataProcessingPipeline':50,},
# #	"MYSQL_HOST" : '192.168.8.222',
# #	"MYSQL_DBNAME" : 'coupon_datacenter',
# #	"MYSQL_USER" : 'root',
# #	"MYSQL_PASSWD" : 'moma',
# #	'COOKIES_ENABLED':True,
# 	}

# 	couponurl = 'http://shareasale.com/dealdatabase2.xml'
# 	con = mdb.connect(host=settings.MYSQL_HOST, user=settings.MYSQL_USER, passwd=settings.MYSQL_PASSWD, db=settings.MYSQL_DBNAME, charset='utf8')
# 	cur = con.cursor()
# 	sql = 'select linkid from linkid where source = "shareasale"'
# 	cur.execute(sql)
# 	existed_coupons = cur.fetchall()
# 	existed_coupons = [item[0] for item in existed_coupons]
# #    print len(existed_coupons), existed_coupons[:5], '----------------------------'
# 	joined_sql = 'select linkid from coupon_api_affiliate_status where source = "shareasale" and affiliate_status="joined"'
# 	cur.execute(joined_sql)
# 	joined_existed_coupons = cur.fetchall()
# 	joined_existed_coupons = [item[0] for item in joined_existed_coupons]
# 	cur.close()
# 	con.close()
# 	today = datetime.datetime.now().strftime('%Y-%m-%d')


	def start_requests(self):
		url = "https://account.shareasale.com/a-login.cfm"
		formdata = {'step2':'True','redirect':'a-main.cfm','username':'cathyca','nuisance':'thanks','gadgetry':'azimuth',\
					'drafter':'millionth','jq1':'overawe','password':'MOMAcnbeta#1102','glad-hand':'dispirit',\
					'glad-hand':'dispirit'}
		request = FormRequest(url, callback = self.parse, formdata = formdata)
		yield request

	def parse(self, response):
		url = self.couponurl
		print url, '--------------------'
	#	return
		yield scrapy.Request(url, callback = self.parse_page)

	def parse_page(self, response):
		coupons_xml = xmltodict.parse(response.body)
		coupons_str = json.dumps(coupons_xml)
		coupons_dict = json.loads(coupons_str)
		coupons = coupons_dict['rss']['channel']['item']
		print len(coupons), coupons[0]

		for coupon in coupons[:]:
			merchantid = ''
			merchantname = ''
			title = ''
			description = ''
			trackingurl = ''
			dealstartdate = ''
			dealenddate = ''
			dealpublishdate = ''
			couponcode = ''
			keywords = ''
			category = ''
			restrictions = ''
			commissionpercentage = ''
			dealtitle = ''
			imagebig = ''
			imagesmall = ''
			htmlofdeal = ''
			link = ''
			enddate = ''
			startdate = ''
			linkid = ''
			affiliate_status=''
			is_new = 1
			if 'affiliate_status' in coupon:
				affiliate_status = coupon['affiliate_status']
			if 'link' in coupon:
				link = coupon['link']
				linkid = link[link.rfind('=')+1:]
				if str(linkid) in self.joined_existed_coupons:
					continue
				if str(linkid) in self.existed_coupons:
					print 'existed in db ---------------'
					if affiliate_status == 'joined':
						print "#####################affiliate coupon"
						is_new = 0
					else:
						continue
			if 'merchantID' in coupon:
				merchantid = coupon['merchantID']
			if 'merchantname' in coupon:
				merchantname = coupon['merchantname']
			if 'title' in coupon:
				title = coupon['title']
			if 'description' in coupon:
				description = coupon['description']
			if 'trackingurl' in coupon:
				trackingurl = coupon['trackingurl']
			if 'dealstartdate' in coupon:
				dealstartdate = coupon['dealstartdate']
				startdate_list = dealstartdate.split('/')
				if len(startdate_list) < 3:
					continue
				startdate = startdate_list[2] + '-' + startdate_list[0] + '-' + startdate_list[1]
				print 'dealstartdate', dealstartdate
			if 'dealenddate' in coupon:
				dealenddate = coupon['dealenddate']
				print 'dealenddate', dealenddate
				enddate_list = dealenddate.split('/')
				if len(enddate_list) < 3:
					continue
				enddate = enddate_list[2] + '-' + enddate_list[0] + '-' + enddate_list[1]
				if enddate < self.today:
					continue
				if startdate:
					if enddate < startdate:
						continue
			if 'dealpublishdate' in coupon:
				dealpublishdate = coupon['dealpublishdate']
			if 'couponcode' in coupon:
				couponcode = coupon['couponcode']
			if 'keywords' in coupon:
				keywords = coupon['keywords']
			if 'category' in coupon:
				category = coupon['category']
			if 'restrictions' in coupon:
				restrictions = coupon['restrictions']
			if 'commissionpercentage' in coupon:
				commissionpercentage = coupon['commissionpercentage']
			if 'dealtitle' in coupon:
				dealtitle = coupon['dealtitle']
			if 'imagebig' in coupon:
				imagebig = coupon['imagebig']
			if 'imagesmall' in coupon:
				imagesmall = coupon['imagesmall']
			if 'htmlofdeal' in coupon:
				htmlofdeal = coupon['htmlofdeal']
			if link:
				print '--------', link+'&ajax=1', '---------'
				#request = scrapy.Request(link+'&ajax=1', callback = self.get_landingpage)
				request = scrapy.Request(link, callback = self.get_landingpage)
				request.meta['merchantid'] = merchantid
				request.meta['merchantname'] = merchantname
				request.meta['title'] = title
				request.meta['description'] = description
				request.meta['trackingurl'] = trackingurl
				request.meta['dealstartdate'] = startdate
				request.meta['dealenddate'] = enddate
				request.meta['dealpublishdate'] = dealpublishdate
				request.meta['couponcode'] = couponcode
				request.meta['keywords'] = keywords
				request.meta['category'] = category
				request.meta['restrictions'] = restrictions
				request.meta['commissionpercentage'] = commissionpercentage
				request.meta['dealtitle'] = dealtitle
				request.meta['imagebig'] = imagebig
				request.meta['imagesmall'] = imagesmall
				request.meta['htmlofdeal'] = htmlofdeal
				request.meta['link'] = link
				request.meta['affiliate_status'] = affiliate_status
				request.meta['is_new'] = is_new
				yield request

	def get_landingpage(self, response):
		print 'xxxxxxxxxx', response.url, 'xxxxxxxxx'
		item = CouponItem()
		item['source'] = 'shareasale'
		if response.meta['merchantid']:
			item['advertiserid'] = response.meta['merchantid']
		else:
			item['advertiserid'] = ''
		if response.meta['merchantname']:
			item['advertisername'] = response.meta['merchantname'].strip()
		else:
			item['advertisername'] = ''
		if response.meta['title']:
			merchant_info = re.findall("Merchant \d+", response.meta['title'])
			if merchant_info:
				pattern = "Merchant \d+ - %s -" % item['advertisername']
				item['title'] = re.sub(pattern, "", response.meta['title']).strip()
			else:
				item['title'] = response.meta['title'].strip()
		else:
			item['title'] = ''
		if response.meta['description']:
			item['description'] = response.meta['description'].strip()
		else:
			item['description'] = ''
		if response.meta['couponcode']:
			item['code'] = response.meta['couponcode']
		else:
			item['code'] = ''
		if response.meta['dealenddate']:
			item['end_date'] = response.meta['dealenddate'].strip()
		else:
			item['end_date'] = ''
		if response.meta['dealstartdate']:
			item['start_date'] = response.meta['dealstartdate'].strip()
		else:
			item['start_date'] = ''
		landingpage = response.xpath("//tr[contains(td[1]/text(), 'Landing Page')]/td[2]/text()").extract_first(default = "").strip()
	#	print response.xpath("//tr[contains(td[1]/b/text(), 'Your Tracking URL')]/td[2]/b/text()").extract_first(default = "").strip()
		tracking_url = response.xpath("//tr[contains(td[1]/b/text(), 'Your Tracking URL')]/td[2]/b/text()").extract_first(default = "").strip()
		if tracking_url:
			item['affiliate_status'] = 'joined'
		else:
			item['affiliate_status'] = ''

		if 'shareasale.com' in tracking_url:
			item['url'] = tracking_url
		else:
			if landingpage:
				item['url'] = landingpage
			else:
				item['url'] = ''
	#			item['url'] = response.meta['link'].strip()

		if response.meta['category']:
			item['category'] = response.meta['category'].strip()
		else:
			item['category'] = ''
		if response.meta['restrictions']:
			item['restriction'] = response.meta['restrictions'].strip()
		else:
			item['restriction'] = ''
		if response.meta['keywords']:
			item['keywords'] = response.meta['keywords'].strip()
		else:
			item['keywords'] = ''
		if response.meta['htmlofdeal']:
			item['htmlofdeal'] = response.meta['htmlofdeal'].strip()
		else:
			item['htmlofdeal'] = ''
		item['promotiontypes'] = ''
		if response.meta['link']:
			link = response.meta['link']
			item['linkid'] =  link[link.rfind('=')+1:]
		else:
			item['linkid'] = ''
		item['freeshipping'] = '0'
		item['is_new'] = response.meta['is_new']
	#	print item
		yield item
#	item_list = response.xpath("//item")
#	for sascoupon in item_list[:1]:
#		item = SasCouponItem()
#		item['merchantid'] = sascoupon.xpath("merchantID/text()").extract_first(default = '').strip()
#		item['merchantname'] = sascoupon.xpath("merchantname/text()").extract_first(default = '').strip()
#		item['title'] = sascoupon.xpath("title/text()").extract_first(default = '').strip()
#		item['description'] = sascoupon.xpath("description/text()").extract_first(default = '').strip()
#		item['dealstartdate'] = sascoupon.xpath("dealstartdate/text()").extract_first(default = '').strip()
#		item['dealenddate'] = sascoupon.xpath("dealenddate/text()").extract_first(default = '').strip()
#		item['dealpublishdate'] = sascoupon.xpath("dealpublishdate/text()").extract_first(default = '').strip()
#		item['couponcode'] = sascoupon.xpath("couponcode/text()").extract_first(default = '').strip()
#		item['keywords'] = sascoupon.xpath("keywords/text()").extract_first(default = '').strip()
#		item['category'] = sascoupon.xpath("category/text()").extract_first(default = '').strip()
#		item['restrictions'] = sascoupon.xpath("restrictions/text()").extract_first(default = '').strip()
#		item['commissionpercentage'] = sascoupon.xpath("commissionpercentage/text()").extract_first(default = '').strip()
#		item['dealtitle'] = sascoupon.xpath("dealtitle/text()").extract_first(default = '').strip()
#		item['imagebig'] = sascoupon.xpath("imagebig/text()").extract_first(default = '').strip()
#		item['imagesmall'] = sascoupon.xpath("imagesmall/text()").extract_first(default = '').strip()
#		item['htmlofdeal'] = sascoupon.xpath("htmlofdeal/text()").extract_first(default = '').strip()
#		item['link'] = sascoupon.xpath("link/text()").extract_first(default = '').strip()
		##yield item

#    def close(reason):
#	#con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'coupon')
#        #cur = con.cursor()
#        #sql = 'select count(*) from shareasale_coupon where add_date like %s'
#        #current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#        #cur.execute(sql, (current_date+"%", ))
#        #res = cur.fetchone()
#        #if res:
#        #        sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
#        #        cur.execute(sql_insert, ('shareasale_coupon', str(res[0]), datetime.datetime.now()))
#        #        con.commit()
#        #cur.close()
#        #con.close()
#
#
#	con = mdb.connect(host = '192.168.8.250', user = 'root', passwd = 'moma', db = 'haipeng')
#        cur = con.cursor()
#
#        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#	sql = """INSERT INTO coupon_api (source , advertiserid, brand, title, description, `code`, end_date, start_date, url, category, restriction, keywords, htmlofdeal, adddate) SELECT 'shareasale', shareasale_coupon.merchantid, cj_sas_linkshare_brand.brand, shareasale_coupon.dealtitle, shareasale_coupon.description,
#shareasale_coupon.couponcode, shareasale_coupon.dealenddate, shareasale_coupon.dealstartdate, shareasale_coupon.landingpage, shareasale_coupon.category, shareasale_coupon.restrictions, shareasale_coupon.keywords, shareasale_coupon.htmlofdeal, shareasale_coupon.add_date FROM shareasale_coupon INNER JOIN cj_sas_linkshare_brand ON ( shareasale_coupon.merchantid = cj_sas_linkshare_brand.advertiserid )
#WHERE cj_sas_linkshare_brand.source = 'shareasale' AND shareasale_coupon.merchantname is not null AND shareasale_coupon.merchantname <> '' AND shareasale_coupon.merchantname <> '*' AND shareasale_coupon.add_date LIKE %s"""
#        cur.execute(sql, (current_date+"%", ))
#	con.commit()
#
#	sql_unknown_brand = """INSERT INTO unknown_advertiser (source , advertiserid, advertisername, couponnum)
#SELECT 'shareasale', shareasale_coupon.merchantid as advertiserid, shareasale_coupon.merchantname as advertisername, COUNT(*) as couponnum
#FROM shareasale_coupon WHERE shareasale_coupon.add_date LIKE %s AND shareasale_coupon.merchantid NOT in (SELECT DISTINCT cj_sas_linkshare_brand.advertiserid FROM cj_sas_linkshare_brand WHERE cj_sas_linkshare_brand.source = 'shareasale') GROUP BY merchantid, merchantname"""
#	cur.execute(sql_unknown_brand, (current_date+"%", ))
#	con.commit()
#        cur.close()
#        con.close()
