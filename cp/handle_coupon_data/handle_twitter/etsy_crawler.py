#!/usr/bin/python
# -*- coding: UTF-8 -*-
########crawl etsy 信息 ############
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import requests
import time
import threading
import MySQLdb
import MySQLdb.cursors
import threadpool
import codecs
import re
import config
from scrapy.selector import Selector


class Crawler(object):
    headers = {
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2638.0 Safari/537.36',
        "Accept" : "text/html",
        "Cookie":"__utmt=1; __utma=206883212.174790556.1465977299.1465977299.1465977299.1; __utmb=206883212.1.10.1465977299; __utmc=206883212; __utmz=206883212.1465977299.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __gads=ID=6563fee18e9f7b7c:T=1465977300:S=ALNI_MY_pNRXAI9Qry9b7kD6pCUVsAClVw; blogads_book_excerpt_put_ttd_tracking_tags=1",
    }

    def __init__(self):
        self.DB_HOST = config.DB_HOST
        self.DB_NAME = config.DB_NAME
        self.DB_USER = config.DB_USER
        self.DB_PSWD = config.DB_PSWD
        self.CHARSET = config.CHARSET
        self.etsy_table = config.twitter_etsy_table
        self.couponneedhandled_list_table = config.couponneedhandled_list_table
        self.crawl_domain = "https://www.etsy.com/"

    def connect_db(self):
        con = MySQLdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset='utf8mb4')
        cur = con.cursor()
        return con, cur

    def close_db(self,con):
        con.close()

    def prepare_data(self):
        '''读取待爬取的数据'''
        con,cur = self.connect_db()
        sql_select_domains = """SELECT  a.black_url, a.guid from %s as a \
        left join %s as b on a.guid=b.guid where a.black_reason='etsy.com' \
        and b.guid is null GROUP BY a.GUID""" \
        % (self.couponneedhandled_list_table, self.etsy_table)

        cur.execute( sql_select_domains )
        data = cur.fetchall()
        self.close_db(con)
        urllist = []
        for i in data:
            #####处理shop的url
            if(re.search('\/shop\/',i[0])):
                temp = i[0].replace('?','/').split('/')
                shopindex = temp.index('shop')
                domainList = []
                for i2 in xrange(0,shopindex+2):
                    domainList.append(temp[i2])
                url = '/'.join(domainList)
                urllist.append([url, i[1]])
            else:
                urllist.append( [i[0], i[1]] )
        self.urllist = urllist
        print '#################url count='+str(len(self.urllist))

    def query(self, query, params):
        '''数据库操作'''
        try:
            con,cur = self.connect_db()
            cur.execute(query, params)
            cur.execute("commit")
            self.close_db(con)
        except Exception, e:
            print "sql error", e

    def fetchPage(self,url_tuple):
        '''爬虫'''
        url = url_tuple[0]
        guid = url_tuple[1]
        if not url.startswith('http'):
            url = 'http://'+url
        print "current url "+ url
        item = {}
        item['guid'] = guid
        item['origin_url'] = url
        item['history'] = 'None'
        item['status_code'] = '000'
        item['time'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        item['shop_name'] = 'None'
        item['shop_description'] = 'None'
        item['facebook'] = 'None'
        item['twitter'] = 'None'
        item['website'] = 'None'
        item['relate'] = 'None'
        item['relate_url'] = 'None'

        #######爬取首页文件
        try:
            response = requests.get(url,headers=Crawler.headers,timeout=60)
            item['history'] = response.url.encode('utf-8','ignore')
            item['status_code']=response.status_code
            #转为utf-8编码存入数据库中
            body = self.parse_body(response)
            ############above blongsto common######################
            # print body
            if len(body) > 0:
                threadLock.acquire
                resp = Selector(text=body)
                #############解析shop
                if re.search('\/shop\/',response.url):
                    item.update(self.parse_shop(resp))
                    ########get first product
                    goodslist = resp.css('div[class*="block-grid-item listing-card"]>a::attr(href)').extract()
                    if len(goodslist)>0:
                        product_url = goodslist[0]
                        try:
                            response_product = requests.get(product_url,headers=Crawler.headers,timeout=60)
                            item['relate_url'] = response_product.url.encode('utf-8','ignore')
                            body = self.parse_body(response_product)
                            resp_inner = Selector(text=body)
                            item['relate'] = self.parse_list(resp_inner)
                        except Exception as e:
                            print e
                    else:
                        sidebar = resp.css('div[class=pt-xs-3]>a::attr(href)').extract()
                        if len(sidebar)>0:
                            temp = response.url.split('/')
                            midurl = temp[0]+'/'+temp[1]+'/'+temp[2]+sidebar[0]
                            # print midurl
                            response_product = requests.get(midurl,headers=Crawler.headers,timeout=60)
                            body = self.parse_body(response_product)
                            resp_inner = Selector(text=body)
                            goodslist = resp_inner.css('div[class*="block-grid-item listing-card"]>a::attr(href)').extract()
                            # print goodslist
                            if len(goodslist)>0:
                                product_url = goodslist[0]
                                response_product = requests.get(product_url,headers=Crawler.headers,timeout=60)
                                item['relate_url'] = response_product.url.encode('utf-8','ignore')
                                body = self.parse_body(response_product)
                                resp_inner = Selector(text=body)
                                item.update(self.parse_shop(resp_inner))
                                item['relate'] = self.parse_list(resp_inner)
                            else:
                                item['relate'] = 'None'
                        else:
                            item['relate'] = 'None'
                    # print 'shop page----------------->'+str(item)
                    print '######shop#########'+item['relate']
                elif re.search('\/listing\/',response.url):
                    shopname = resp.css('div[class="shop-name"] a[itemprop]::attr(href)').extract()
                    ####如果有店铺存在则查找,否则跳过
                    if len(shopname)>0:
                        shopurl = shopname[0]
                        try:
                            response_product = requests.get(shopurl,headers=Crawler.headers,timeout=60)
                            item['relate_url'] = response_product.url.encode('utf-8','ignore')
                            body = self.parse_body(response_product)
                            resp_inner = Selector(text=body)
                            item.update(self.parse_shop(resp_inner))
                        except Exception as e:
                            print e
                        item['relate'] = self.parse_list(resp)
                        print '######shop#########'+item['relate']
                elif re.search('\/people\/',response.url):
                    item['shop_description'] = self.parse_people(resp)
                threadLock.release
                # print item
        except requests.exceptions.ConnectionError, e:
            item['history'] = e
            print e
        except Exception, e:
            item['history'] = e
            print e
        return item

    def parse_body(self,response):
        '''对response body解码'''
        if response.encoding == 'ISO-8859-1':
            encodings = requests.utils.get_encodings_from_content(response.content)
            if encodings:
                response.encoding = encodings[0]
            else:
                response.encoding = response.apparent_encoding
        body = response.content
        if len(body)>0:
            body = response.content.decode(response.encoding, 'replace').strip().encode('utf-8', 'replace')
        else:
            body = ""
        return body

    def parse_shop(self,response):
        '''解析shop类型的url response'''
        # response = Selector(text=body)
        item = {}
        ######处理店铺信息
        name_temp = response.css('div[class*="shop-name-and-title-container"] > h1::text').extract()
        if len(name_temp)>0:
            item['shop_name'] = name_temp[0].strip().encode('utf-8','ignore')
        desc_temp = response.css('div[class*="shop-name-and-title-container"] > h1+span::text').extract()
        if len(desc_temp)>0:
            item['shop_description'] = desc_temp[0].strip().encode('utf-8','ignore')
        shop_around_href = response.xpath('//div[contains(./h5/text(),"Around the web")]/following-sibling::div/div/div/a/@href').extract()
        shop_around_show = response.xpath('//div[contains(./h5/text(),"Around the web")]/following-sibling::div/div/div/a/span/text()').extract()
        # print shop_around_show
        for index,who in enumerate(shop_around_show):
            if re.search('facebook',who.lower()):
                if not item.get('facebook'):
                    item['facebook'] = shop_around_href[index].strip()
            if re.search('twitter',who.lower()):
                if not item.get('twitter'):
                    item['twitter'] = shop_around_href[index].strip()
            if re.search('website',who.lower()):
                if not item.get('website'):
                    item['website'] = shop_around_href[index].strip()
        return item

    def parse_list(self,response):
        '''解析listing类型的response'''
        # response = Selector(text=body)
        list_item = response.xpath('//h2[contains(./text(),"Related to this item")]/following-sibling::ul/li/a/text()').extract()
        if len(list_item)>0:
            return ','.join(list_item)
        else:
            return 'None'

    def parse_people(self,response):
        '''解析people类型的response'''
        profile = response.css('div[id=bio]').extract()
        if len(profile)>0:
            data = profile[0]
        else:
            data = 'None'
        return data

    def manager_thread(self):
        '''线程分配'''
        num = 80
        pool = threadpool.ThreadPool(num)
        requestlist = threadpool.makeRequests(self.fetchPage,self.urllist,self.insertDB)
        [pool.putRequest(req) for req in requestlist]
        pool.wait()
        pool.dismissWorkers(num)
        if pool.dismissWorkers:
            pool.joinAllDismissedWorkers()

    def insertDB(self,request,item):
        '''回调函数,存储数据库'''
        # print item
        threadLock.acquire()
        self.query("""insert into """ + self.etsy_table + """ (guid, origin_url, time, status_code, ext1, ext2, ext3, ext4, ext5, ext6, history, ext7)
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        ( item['guid'], item['origin_url'], item['time'], item['status_code'], item['shop_name'], item['shop_description'],
            item['facebook'], item['twitter'], item['website'], item['relate'], item['history'], item['relate_url']))
        del item
        threadLock.release()

    def start(self):
        '''入口方法，加入预处理操作'''
        print 'prepare data'
        self.prepare_data()
        self.manager_thread()

if __name__ == '__main__':
    print 'crawler is running...'
    start = time.time()
    threadLock = threading.Lock()
    Crawler().start()
    end = time.time()
    print end-start
##############  usage:准备好数据库信息,直接执行   ##############
