#!/usr/bin/python
# -*- coding: UTF-8 -*-
######## crawl instagram ############

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
from hashlib import md5
import json

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
        self.instagram_table = config.twitter_instagram_table
        self.couponneedhandled_list_table = config.couponneedhandled_list_table
        self.crawl_domain = "https://www.instagram.com/"

    def connect_db(self):
        con = MySQLdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset='utf8mb4')
        cur = con.cursor()
        return con, cur

    def close_db(self,con):
        con.close()

    def prepare_data(self):
        '''准备待爬取链接'''
        con,cur = self.connect_db()
        sql_select_domains = """SELECT a.black_url, a.guid from %s as a left join %s as b \
        on a.guid=b.guid where a.black_reason='instagram.com' and b.guid is null GROUP BY a.guid
        """ % (self.couponneedhandled_list_table, self.instagram_table)

        cur.execute( sql_select_domains )
        data = cur.fetchall()
        self.close_db(con)
        urllist = [[i[0], i[1]] for i in data]
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
        print url
        item = {}
        item['guid'] = guid
        item['origin_url'] = url.encode('utf-8','ignore')
        if not url.startswith('http'):
            url = 'http://'+url
        item['time'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        item['status_code'] = "000"
        item['current_ip'] = ''
        item['useurl'] = "000"
        item['res'] = 'None'
        item['source'] = 'None'
        item['username'] = 'None'
        item['user_url'] = 'None'
        item['user_src'] = 'None'
        item['user_urls'] = 'None'
        item['user_description'] = 'None'
        #######爬取首页文件
        try:
            response = requests.get(url,headers=Crawler.headers,timeout=60)
            item['useurl'] = response.url.encode('utf-8','ignore')
            item['status_code']=response.status_code
            item['history']=str(response.history)
            #转为utf-8编码存入数据库中
            body = self.parse_body(response)
            if len(body) > 0:
                threadLock.acquire
                rt = self.parse(body)
                item['res'] = rt['matchs']
                item['source'] = rt['source']
                item['username'] = rt['username']
                item['user_url'] = self.crawl_domain+rt['username']
                try:
                    http_user = requests.get(item['user_url'],headers=Crawler.headers,timeout=60)
                    body = self.parse_body(http_user)
                    if len(body)>0:
                        user_res = self.parse_userinfo(body)
                        item['user_src'] = user_res['source']
                        item['user_urls'] = user_res['urls']
                        item['user_description'] = user_res['description']
                except Exception as e:
                    print "ddddddddddddddddddddddd",e
                threadLock.release
        except requests.exceptions.ConnectionError, e:
            item['history'] = e
            print "aaaaaaaaaaaaaaaaaaaaaa",e
        except Exception, e:
            item['history'] = e
            print "bbbbbbbbbbbbbbbbbbbbbb",e
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

    def parse(self,body):
        '''解析返回信息'''
        resp = Selector(text=body)
        data = {}
        data['source'] = 'None'
        data['matchs'] = 'None'
        data['username'] = 'None'
        #找到所需js代码
        json_string=resp.xpath('//body/script[not(@src)]/text()').extract()
        if len(json_string)>0:
            json_string=json_string[0]
            data['source'] = json_string
            item = ''
            try:
                item = json.loads(json_string[json_string.find("{"):json_string.find("}",-1)])['entry_data']['PostPage'][0]['graphql']['shortcode_media']
                data['matchs'] = item['edge_media_to_caption']['edges'][0]['node']['text']
            except Exception as e:
                pass
            try:
                data['username'] = item['owner']['username']
            except Exception as e:
                pass
        return data

    def parse_userinfo(self,body):
        '''解析用户信息'''
        resp = Selector(text=body)
        data = {}
        data['source'] = 'None'
        data['urls'] = 'None'
        data['description'] = 'None'
        json_string=resp.xpath('//body/script[not(@src)]/text()').extract()
        if len(json_string)>0:
            json_string=json_string[0]
            data['source'] = json_string
            try:
                item = json.loads(json_string[json_string.find("{"):json_string.find("}",-1)])['entry_data']['ProfilePage'][0]
                data['urls'] = item['user']['external_url']
            except Exception as e:
                pass
            try:
                data['description'] = item['user']['biography']
            except Exception as e:
                pass
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
        '''插入数据库'''
        # print item
        threadLock.acquire()

        self.query("""INSERT into """+self.instagram_table+"""(guid, `time`, origin_url, \
            history, status_code,ext1,ext2,ext3,ext4,ext5,ext6,ext7,ext8,ext8_dest,ext8_guid,\
            ext9) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,( item['guid'], item['time'], item['origin_url'], item['history'], item['status_code'],
            item['current_ip'], item['useurl'], item['res'], "", item['username'],
            item['user_url'], "", item['user_urls'],"", md5(item['user_urls']).hexdigest(), item['user_description']) )
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
################ usage:准备好数据库信息直接运行  ##################
