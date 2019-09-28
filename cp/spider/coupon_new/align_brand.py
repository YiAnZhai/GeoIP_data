# -*- coding: utf-8 -*-
import datetime
import MySQLdb as mdb
import re
from hashlib import md5

def connect_db(host, user, passwd, db):
	con = mdb.connect(host = host, user = user, passwd = passwd, db = db)
	cur = con.cursor()
	return con, cur

def close_db(con, cur):
	cur.close()
	con.close()

def align_brand(host, user, passwd, db):
	con, cur = connect_db(host, user, passwd, db)
	current_date = datetime.datetime.now().strftime("%Y-%m-%d")
	sql = """UPDATE coupon_api_today INNER JOIN cj_sas_linkshare_brand 
	ON coupon_api_today.advertiserid = cj_sas_linkshare_brand.advertiserid
	AND coupon_api_today.source = cj_sas_linkshare_brand.source
	SET coupon_api_today.brand = cj_sas_linkshare_brand.brand
	WHERE coupon_api_today.advertisername is not null 
	and coupon_api_today.advertisername <> '' 
	and coupon_api_today.advertisername <> '*'"""
#	and coupon_api_today.adddate like %s"""
	
	res = cur.execute(sql)
#	res = cur.execute(sql, (current_date+"%", ))
#	res = cur.execute(sql, ("2016-01-25%", ))
	print 'update %s brand' % str(res)
	con.commit()
	close_db(con, cur)

def remove_duplicate_intra_api(host, user, passwd, db):
	con, cur = connect_db(host, user, passwd, db)
	sql = "select id, brand, title, description, `code` from coupon_api_today"
	sql_update = "update coupon_api_today set guid = %s where id = %s"
	cur.execute(sql)
	res = cur.fetchall()
	
	md5_list = list()		
	for one_res in res:
		#compute_guid，根据指定的规则计算md5值
		guid = compute_guid(one_res)
		md5_list.append((guid, one_res[0]))
	
	# insert the md5 value into database
	cur.executemany(sql_update, md5_list)
	con.commit()
	
	# 删除今日coupon数据中guid相同的数据
	sql_delete_self = """DELETE FROM coupon_api_today WHERE guid in (SELECT a.guid FROM (SELECT guid FROM coupon_api_today GROUP BY guid HAVING COUNT(*) > 1) a) AND id NOT in (SELECT b.minid FROM (SELECT min(id) as minid FROM coupon_api_today GROUP BY guid HAVING COUNT(*) > 1) b)"""
	res = cur.execute(sql_delete_self)
	con.commit()
	print '删除了 %s 自身重复的coupon' % str(res)

	# 删除今日coupon中，guid已存在于之前coupon的数据
	sql_delete_with_base = """DELETE FROM coupon_api_today WHERE guid in (SELECT DISTINCT guid FROM coupon_api)"""
	res = cur.execute(sql_delete_with_base)
	con.commit()
	print '删除了 %s 与线上重复的coupon' % str(res)

	close_db(con, cur)

def compute_guid(data):
	data = list(data)
	if data[2]:
		data[2] = re.sub("\W", "", data[2])
	if data[3]:
		data[3] = re.sub("\W", "", data[3])
	one_res_list = [item.lower() for item in data[1:] if item]
	mess = ''.join(one_res_list)
	guid = md5(mess).hexdigest()
	return guid	

def insert_new_coupon_into_base(host, user, passwd, db):
	con, cur = connect_db(host, user, passwd, db)
	sql = """INSERT INTO coupon_api SELECT * FROM coupon_api_today"""
	res = cur.execute(sql)
	con.commit()
	print '插入了 %s 条新的coupon' % str(res)
	close_db(con, cur)

def delete_today_coupon(host, user, passwd, db):	
	con, cur = connect_db(host, user, passwd, db)
	sql = """DELETE FROM coupon_api_today"""
	res = cur.execute(sql)
	con.commit()
	print '把today coupon表清空，删除了 %s 条数据' % str(res)
	close_db(con, cur)

if __name__ == '__main__':
	host = '192.168.8.147'
	user = 'root'
	passwd = '123'
	db = 'scrapy'
#	align_brand(host, user, passwd, db)
	remove_duplicate_intra_api(host, user, passwd, db)
#	insert_new_coupon_into_base(host, user, passwd, db)
#	delete_today_coupon(host, user, passwd, db)	
