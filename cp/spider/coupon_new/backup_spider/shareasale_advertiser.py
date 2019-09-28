# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import FormRequest
from coupon.items import SasAdvertiserItem
import MySQLdb as mdb
import datetime

class ShareasaleSpider(scrapy.Spider):
    name = "shareasale_advertiser"
    allowed_domains = ["shareasale.com"]
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

    merchanturl='https://account.shareasale.com/a-datafeeds.cfm'
    con = mdb.connect(host = '192.168.8.240', user = 'root', passwd = 'moma', db = 'haipeng')
    cur = con.cursor()
    sql = 'select advertiser_id from shareasale_advertiser'
    cur.execute(sql)
    existed_advertisers = cur.fetchall()
    existed_advertisers = [item[0] for item in existed_advertisers]
#    print len(existed_advertisers), type(existed_advertisers[0])
    cur.close()
    con.close()

    def start_requests(self):
	url = "https://account.shareasale.com/a-login.cfm"
        formdata = {'step2':'True','redirect':'a-main.cfm','username':'cathyca','nuisance':'thanks','gadgetry':'azimuth',\
                    'drafter':'millionth','jq1':'overawe','password':'MOMAcnbeta#1102','glad-hand':'dispirit',\
                    'glad-hand':'dispirit'}
        request = FormRequest(url, callback = self.parse, formdata = formdata)
	yield request

    def parse(self, response):
	url = self.merchanturl
#	return
	print url, '--------------------'
	yield scrapy.Request(url, callback = self.parse_page)
    
    def parse_page(self, response):
	advertiser_list = response.css("table#rptAA tr.dat")
	print len(advertiser_list), advertiser_list[1]
	for advertiser_sel in advertiser_list[:]:
		merchantid = advertiser_sel.css("a.bodylink::attr(href)").re_first('.*?merchantID=(\d+)')
		if not merchantid:
			continue
		merchantid = merchantid.strip()
#		print merchantid, type(merchantid)
		if int(merchantid) in self.existed_advertisers:
			print 'existed in db --------------------'
			continue
		merchantname = advertiser_sel.css("a.bodylink::text").extract_first(default = '').strip()
		print 'yes',  merchantid, merchantname
		url = 'https://account.shareasale.com/affiliateAjax.cfc?returnFormat=plain&method=displayMerchantInfo&storeId=0&_cf_nodebug=true&_cf_nocache=true&_cf_rc=1&merchantId=%s&_cf_containerId=MER_%s_body' % (merchantid, merchantid)
		request = scrapy.Request(url, callback = self.parse_detail)
		request.meta['merchantid'] = merchantid
		request.meta['merchantname'] = merchantname
		yield request

	# next 100 results
	next_page_url = response.xpath("//a[contains(text(), 'Next 100 Results')]/@onclick").re_first(".*?\('(.*?)'\)")
	if next_page_url:
		next_page_url = response.urljoin(next_page_url)
		yield scrapy.Request(next_page_url, callback = self.parse_page)
		print next_page_url, '---------------------------------------------------'

    def parse_detail(self, response):
	merchantid = response.meta['merchantid']
	merchantname = response.meta['merchantname']	
	hp = response.css("#dspHead a::text").extract_first(default = '').strip()
	category = response.xpath("//tr[contains(th/text(), 'Category')]/td/text()").extract_first(default = '').strip()
	commission = response.xpath("//tr[contains(th/text(), 'Commission')]/td/text()").extract_first(default = '').strip()
	seven_day_epc = response.xpath("//tr[contains(th/text(), 'EPC')]/td[1]/text()").extract_first(default = '').strip()
	one_month_epc = response.xpath("//tr[contains(th/text(), 'EPC')]/td[2]/text()").extract_first(default = '').strip()
	item = SasAdvertiserItem()
	item['merchantid'] = merchantid
	item['brand'] = merchantname
	item['hp'] = hp
	item['category'] = category
	item['commission'] = commission
	item['seven_day_epc'] = seven_day_epc
	item['one_month_epc'] = one_month_epc
	yield item

#    def close(reason):
#	con = mdb.connect(host = '192.168.8.147', user = 'root', passwd = '123', db = 'coupon')
#        cur = con.cursor()
#        sql = 'select count(*) from shareasale_advertiser where add_date like %s'
#        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
#        cur.execute(sql, (current_date+"%", ))
#        res = cur.fetchone()
#        if res:
#                sql_insert = 'insert into new_coupon (type, new_num, add_date) values (%s, %s, %s)'
#                cur.execute(sql_insert, ('shareasale_advertiser', str(res[0]), datetime.datetime.now()))
#                con.commit()
#        cur.close()
#        con.close()
