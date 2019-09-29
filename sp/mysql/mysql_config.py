# -*- coding: utf-8 -*-

MYSQL_HOST = "localhost"
MYSQL_DB_NAME = "instagram_meme_datacenter"
MYSQL_USER = "root"
MYSQL_PASSWD = "moma"
CHART_SET = "utf8mb4"
MYSQL_PORT = 3306



############### log table
config_common_log_processstatus_table = "status_process"
config_common_log_processstatus_fields_detail = [
    ['status', 'varchar(20) DEFAULT NULL', ],
    ['process', 'text DEFAULT NULL', ],
    ['date', 'varchar(20) DEFAULT NULL', ],
    ['period', 'varchar(50) DEFAULT NULL', ],
    ['time_spend', 'varchar(20) DEFAULT NULL', ],
]
config_common_log_processstatus_fields = [ f[0] for f in config_common_log_processstatus_fields_detail ]
config_common_log_time_format = "%Y-%m-%d %H:%M:%S"



############### urls table need to be crawled
config_urls_need_crawl_fields_detail = [
    ['guid', 'varchar(32) DEFAULT NULL', ],  ####### necessary *
    ['url', 'text DEFAULT NULL', ],           ####### necessary *
]
configurls_need_crawl_fields = [ f[0] for f in config_urls_need_crawl_fields_detail ]