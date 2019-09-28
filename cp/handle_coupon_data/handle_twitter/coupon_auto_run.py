# -*- coding: utf-8 -*-
import os
import sys
from datetime import datetime as dt
from check_info_autorunstart import Today_table


from common_functions import time_period_spend, debug_error

global_log_table = "status_process"
global_log_fileds = ["status","process","date","period","time_spend"]
global_today = dt.now().strftime("%Y-%m-%d")
global_log_time_format = "%Y-%m-%d %H:%M:%S"


def update_log_table():
    sql_update = "UPDATE %s SET `%s`=0 WHERE `%s`=1" % (global_log_table, "is_fresh", "is_fresh")
    global_auto_run_msl.execute_sql( sql_update )


def run_cmd( cmd ):
    flag = 1
    t1 = dt.now()
    global_auto_run_msl.log_status( global_log_table, global_log_fileds, ["Success-Begin", cmd, global_today, t1.strftime(global_log_time_format), ""] )
    flag = os.system( cmd )
    t2 = dt.now()
    time_period, time_spend = time_period_spend(t1, t2, global_log_time_format)
    if 0 != flag:
        debug_error( cmd )
        global_auto_run_msl.log_status( global_log_table, global_log_fileds, ["**Faild**", cmd, global_today, time_period, time_spend] )
        return False
    global_auto_run_msl.log_status( global_log_table, global_log_fileds, ["Success", cmd, global_today, time_period, time_spend] )
    return True


def process_run():
    spider_path = "/home/moma/Documents/Project_process/mycoupon/spider/coupon_new/coupon/coupon/spiders"
    ############################################################################################
    ############################################################################################
    #################################### write your code here###################################
    # export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games
    cmd_list = [
        ##### get landing url from twitter
        "cd %s;scrapy crawl getredirecturl -a api='twitter' -L WARNING" % (spider_path, ), 
        #####update getredurl black or not, add pending back to info_handle, need continue handled domain to needhandled_table
        "python update_brandingetredurl.py", 

        #######handle etsy and instagram
        # python instagram_crawler.py
        # python etsy_crawler.py
        # python crawl_ins_landingpage.py
        # #######extract etsy and instagram
        # python update_handleneeded_brand.py

        #####get domain title and description
        #####the order twitter and etsy_instagram is important
        "cd %s;scrapy crawl getredirecturl_fordomain -a src='twitter' -L WARNING" % (spider_path, ), 

        ##### update domain_get_redurl domain porn or not, init dup_base
        "python update_domain_porn.py", 
        ### python join_red_url.py
        ### python get_clean_url.py
        ##### add data that not black for dup remove
        "python judge_destinationurl_black.py", 
        # 添加新流程 Yifan
        # python handler_linkis.py
        # python handler_constantcontact.py
        # python handler_campagin.py
        # 下面会重置 handled
        # "python judge_destinationurl_needhandled.py", 

        "python delete_negativeword.py", 

        "python dup_remove_innertwitter.py", 
        ### python dup_remove_withapi.py
        "python init_today_table.py"    ####do it in dup_remove_innertwitter.py
    ]

    cmd_list = [
        "cd %s;scrapy crawl getredirecturl -a api='twitter' -L WARNING" % (spider_path, ), 
        "python update_brandingetredurl.py", 
        "cd %s;scrapy crawl getredirecturl_fordomain -a src='twitter' -L WARNING" % (spider_path, ), 
        "python update_domain_porn.py", 
        "python judge_destinationurl_black.py", 
        "python delete_negativeword.py", 
        "python dup_remove_innertwitter.py", 
        "python init_today_table.py"    ####do it in dup_remove_innertwitter.py
    ]
############################################################################################
############################################################################################
    for cmd in cmd_list:
        flag = run_cmd( cmd )
        if not flag:
            return False
    return True



if "__main__" == __name__:
    global_auto_run_msl = Today_table()
    ###############update log table
    update_log_table()


    t1 = dt.now()
    process_status = process_run()
    t2 = dt.now()

    time_period, time_spend = time_period_spend(t1, t2, global_log_time_format)
    status = ""

    if process_status:
        status = "Success-total task"
        print "\n\n%s\n\n" % (status, )
    else:
        status = "Faild-total task"
        debug_error( "%s" % (status, ) )

    global_auto_run_msl.log_status( global_log_table, global_log_fileds, [status, "All", global_today, time_period, time_spend] )