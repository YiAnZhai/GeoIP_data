import MySQLdb as mdb
from hashlib import md5
import re
import operator
import config_couponapi

DB_HOST = config_couponapi.DB_HOST
DB_NAME = config_couponapi.DB_NAME
DB_USER = config_couponapi.DB_USER
DB_PSWD = config_couponapi.DB_PSWD
CHARSET = config_couponapi.CHARSET
api_fresh_copy_table = config_couponapi.api_fresh_copy_table


def uniform_url(url):
	url = url.lower().split('//', 1)[-1].strip('/')
	if url.startswith('www.'):
		url = url[4:]
	return url

con = mdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PSWD, db=DB_NAME, charset=CHARSET)
cur = con.cursor()

sql = "select id, landing_page from %s" % (api_fresh_copy_table)
cur.execute(sql)
res = cur.fetchall()

sql_update = 'update %s set url = %s where id = %s' % (api_fresh_copy_table, "%s", "%s")
md5_list = list()
print len(res), res[:2]
count = 0
key_dict = {}


param_set = set(['utm_', 'affiliate', 'commissionjunction', 'shareasale', 'linkshare'])
partial_key_set = set(['siteid', 'cm_mmc', 'ref', 'source', 'src'])
exact_key_set = set(['mid', 'rid', 'caid', 'adtrack', 'lsid', 'ls_affid', 'afid', 'cid', 'pubid', 'publisher_id', 'linid', 'ranmid', 'raneaid', 'aid', 'affid'])
value_set = set(['cj'])

def check_param(param):
	flag = False
	for item in param_set:
		if item in parameter.lower():
			flag = True
			break
	return flag

def check_partial_key(lower_key):
	flag = False
	for item in partial_key_set:
		if item in lower_key:
			flag = True
			break
	return flag

def check_exact_key(lower_key):
	flag = False
	for item in exact_key_set:
		if item == lower_key:
			flag = True
			break 
	return flag

def check_exact_value(lower_value):
	flag = False
	for item in value_set:
		if item == lower_value:
			flag = True
			break
	return flag

for one_res in res[:]:
#	print one_res, '--------------'
	final_url = one_res[1]
	if not final_url:
		count += 1
		continue
	tmp_list = final_url.split('?', 1)
#	print tmp_list 
	if len(tmp_list) == 1:
		clean_url = uniform_url(tmp_list[0])
#		print clean_url, tmp_list[0]
		md5_list.append((clean_url, one_res[0]))
		continue
	url_base = tmp_list[0]
	parameter_list = tmp_list[1].split('&')
	preserve_parameter_list = list()
	for parameter in parameter_list:
		flag = 0
		# filter utm_
		if check_param(parameter):
			continue

		key_val = parameter.split('=')
		if len(key_val) == 1:
			continue
#			preserve_parameter_list.append(parameter)
		else:
			lower_key = key_val[0].lower()
			if check_partial_key(lower_key):
				continue
			
			if check_exact_key(lower_key):
				continue
			
			lower_value = key_val[1].lower()
			if check_exact_value(lower_value):
				continue

			preserve_parameter_list.append(parameter)
#			if key_val[0] in key_dict:
#				key_dict[key_val[0]] += 1
#			else:
#				key_dict[key_val[0]] = 1
	if preserve_parameter_list:
		clean_url = url_base + '?' + '&'.join(preserve_parameter_list)
	else:
		clean_url = url_base
#	print final_url
#	print clean_url	

	clean_url = uniform_url(clean_url)
	md5_list.append((clean_url, one_res[0]))

print len(md5_list)
print count
cur.executemany(sql_update, md5_list)
con.commit()
#print len(res), res[:10]
cur.close()
con.close()
