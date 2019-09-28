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
    name = "add_new_advertiser_ls"
#    allowed_domains = ["abc.com"]
    start_urls = (
#        'http://www.abc.com/',
    )

    custom_settings = {
	'ITEM_PIPELINES':{'coupon.pipelines.UniformMySQLStorePipeline2':100,},
	}

    def __init__(self):
	self.ls_baseurl = "http://cli.linksynergy.com/cli/publisher/programs/adv_info.php?mid=%s&eid=nw5GPrlpYZw&lsnoid=NONE"

    def start_requests(self):
	url = "https://login.linkshare.com/sso/login"
        request = scrapy.Request(url, callback = self.parse)
        yield request

    def parse(self, response):
	lt = response.xpath("//input[contains(@name, 'lt')]/@value").extract_first()
	execution = response.xpath("//input[contains(@name, 'execution')]/@value").extract_first()
	eventid = response.xpath("//input[contains(@name, '_eventId')]/@value").extract_first()
	healthcheck = response.xpath("//input[contains(@name, 'HEALTHCHECK')]/@value").extract_first()
	print lt, execution, eventid, healthcheck

	headers = {"Host": "login.linkshare.com",
"User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Accept-Language": "en-US,en;q=0.5",
"Accept-Encoding": "gzip, deflate",
"Referer": "https://login.linkshare.com/sso/login",
"Connection": "keep-alive"}
	formdata = {"lt":lt,
"execution":execution,
"_eventId":eventid,
"HEALTHCHECK":healthcheck,
"username":"OliviaM",
"password":"aff@RLS414",
"login":"Log In"}
	url = "https://login.linkshare.com/sso/login"
        request = FormRequest(url, callback = self.parse_login, formdata = formdata, headers = headers)
        yield request
	
    def parse_login(self, response):
	f = open('test.html', 'w')
	f.write(response.body)
	f.close()

	host = settings.MYSQL_HOST
	user = settings.MYSQL_USER
	passwd = settings.MYSQL_PASSWD
	db = settings.MYSQL_DBNAME
	
	sql = 'SELECT A.source, A.advertiserid FROM coupon_api_today A LEFT JOIN cj_sas_linkshare_brand B ON A.advertiserid = B.advertiserid AND A.source = B.source WHERE B.advertiserid IS NULL'

	print response.request.headers, 'aaaaaaaaaaaaaaaaaaaaa'
	print response.headers, 'bbbbbbbbbbbbbbbbbb'
#	cookie = response.meta['']	
	con, cur = utils.connect_db(host, user, passwd, db)
	data_list = utils.select_db(cur, sql)
	print len(data_list), data_list[:2]
	count = 0
	headers = {"Host": "cli.linksynergy.com",
"User-Agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:41.0) Gecko/20100101 Firefox/41.0",
"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
"Accept-Language": "en-US,en;q=0.5",
"Accept-Encoding": "gzip, deflate",
"Connection": "keep-alive"}
	ls_set = set()

	for data in data_list[:]:
		advertiserid = data[1]
		if data[0] == 'linkshare':
			if advertiserid in ls_set:
				continue		
			ls_set.add(advertiserid)
			url = self.ls_baseurl % (advertiserid, )
			print count
			print url
			print
			count += 1
			req = scrapy.Request(url, callback = self.parse_ls, headers = headers, dont_filter = True)
			req.meta['advertiserid'] = advertiserid
			yield req
	utils.close_db(con, cur)

    def parse_ls(self, response):
	program_url = response.css("a#website::text").extract_first(default = "").strip()
	program_url = re.split(r'[,; ]', program_url)[0]
	item = AdvertiserItem()
	item['source'] = 'linkshare'
	item['advertiserid'] = response.meta['advertiserid']
	item['program_url'] = program_url
	item['domain'] = utils.get_domain_from_url(program_url)
	yield item
