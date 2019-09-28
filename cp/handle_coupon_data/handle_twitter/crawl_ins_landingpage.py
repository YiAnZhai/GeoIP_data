#!/usr/bin/python
# -*- coding: UTF-8 -*-
############# 获取user页面 destination url   #######################
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
from scrapy.selector import Selector
import config

class Crawler(object):

    headers = {
        'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2638.0 Safari/537.36',
        "Accept" : "text/html",
        "Cookie":"__utmt=1; __utma=206883212.174790556.1465977299.1465977299.1465977299.1; __utmb=206883212.1.10.1465977299; __utmc=206883212; __utmz=206883212.1465977299.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __gads=ID=6563fee18e9f7b7c:T=1465977300:S=ALNI_MY_pNRXAI9Qry9b7kD6pCUVsAClVw; blogads_book_excerpt_put_ttd_tracking_tags=1",
    }

    def __init__(self):
        '''初始化配置'''
        self.DB_HOST = config.DB_HOST
        self.DB_NAME = config.DB_NAME
        self.DB_USER = config.DB_USER
        self.DB_PSWD = config.DB_PSWD
        self.CHARSET = config.CHARSET
        self.instagram_table = config.twitter_instagram_table

    def connect_db(self):
        con = MySQLdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset='utf8mb4')
        cur = con.cursor()
        return con, cur

    def close_db(self,con):
        con.close()

    def prepare_data(self):
        '''查找用户url'''
        con,cur = self.connect_db()
        cur.execute("""SELECT ext8_guid, ext8 from %s \
            where guid is not null and ext8!='None' and ext8_dest = "" group by ext8_guid
        """ % self.instagram_table)
        data = cur.fetchall()
        self.close_db(con)
        urllist = [[i[0],i[1]] for i in data]
        self.urllist = urllist
        print '#################url count='+str(len(self.urllist))

    def query(self, query, params):
        '''插入数据库'''
        try:
            con,cur = self.connect_db()
            cur.execute(query, params)
            cur.execute("commit")
            self.close_db(con)
        except Exception, e:
            print "sql error", e

    def fetchPage(self,url_tuple):
        guid = url_tuple[0]
        url = url_tuple[1]
        '''爬虫'''
        print "current url "+ url
        item = {}
        item['ext8_guid'] = guid
        item['ext8_dest'] = 'None'
        if not url.startswith("http"):
            url = "http://"+url
        #######爬取首页文件
        try:
            response = requests.get(url,headers=Crawler.headers,timeout=60)
            item['ext8_dest'] = response.url.encode('utf-8','ignore')
        except requests.exceptions.ConnectionError, e:
            print e
        except Exception, e:
            print e
        return item

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
        '''更新数据库'''
        print item['ext8_dest']
        threadLock.acquire()
        self.query("""
            update  """ + self.instagram_table + """  set ext8_dest= %s where ext8_guid= %s
        """,( item['ext8_dest'], item['ext8_guid']))
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
