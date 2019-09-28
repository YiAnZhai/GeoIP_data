#-*- coding:UTF-8 -*-
import sys
reload(sys);
sys.setdefaultencoding('utf-8')
import csv
import xlrd
import xlwt
import MySQLdb as mdb
import commands, os, re
import time

def read_csv(csv_path):
	csvfile = file(csv_path, 'rb')
	reader = csv.reader(csvfile)
	# headers = next(reader)
	data_list = list()
	count=0
	for line in reader:
		data_list.append(line)
	headers = data_list[0]
	print headers
	csvfile.close()
	data_list = data_list[1:]
	return data_list, headers

def save_csv(csv_path, data, header):
	print "hehehehe"
	with open(csv_path, 'w') as csvfile:
#		fieldnames=['rank', 'website']
		fieldnames = header
		writer=csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for row in data:
			data_dict = {}
			for count in range(0, len(fieldnames)):
				data_dict[fieldnames[count]] = row[count]
			writer.writerow(data_dict)
#			writer.writerow({'rank':row[0], 'website':row[1]})

def save_csv_append(csv_path, data, header):
	print "hehehehe"
	with open(csv_path, 'a') as csvfile:
#		fieldnames=['rank', 'website']
		fieldnames = header
		writer=csv.DictWriter(csvfile, fieldnames=fieldnames)
		writer.writeheader()
		for row in data:
			data_dict = {}
			for count in range(0, len(fieldnames)):
				data_dict[fieldnames[count]] = row[count]
			writer.writerow(data_dict)
#			writer.writerow({'rank':row[0], 'website':row[1]})

def read_excel(excel_path):
	book=xlrd.open_workbook(excel_path)
	try:
		sh_brandhp=book.sheet_by_index(0)
	except:
		print 'no sheet named Sheet1'
		return None
	brandhp_nrows=sh_brandhp.nrows
	brandhp_ncols=sh_brandhp.ncols
	print 'nrows %d, ncols %d' % (brandhp_nrows, brandhp_ncols)

	data_list=[]
	headers = []
	for j in range(0, brandhp_ncols):
		headers.append(sh_brandhp.cell(0, j).value)
	for i in range(1, brandhp_nrows):
		data=[]
		for j in range(0, brandhp_ncols):
			data.append(sh_brandhp.cell(i, j).value)
		data_list.append(data)
	print 'headers: -------------', headers
	return data_list, headers

def save_excel_new(data_list, attribute_list, excel_path):
	# savedir=os.path.dirname(excel_path)
	# if not os.path.exists(savedir):
	#     os.mkdir(savedir)
	book = xlwt.Workbook()
	sheet = book.add_sheet("data")
	loop = 1
	index=0
	for attribute in attribute_list:
		sheet.write(0, index, attribute)
		index+=1

	for data in data_list:
		for index in range(0, len(attribute_list)):
			sheet.write(loop, index, data[index])
		loop+=1
	book.save(excel_path)

def connect_db(host, user, passwd, db):
        con = mdb.connect(host = host, user = user, passwd = passwd, db = db, charset='utf8')
        cur = con.cursor()
        return con, cur

def close_db(con, cur):
        cur.close()
        con.close()

def insert_db(con, cur, sql, data):
	try:
		cur.execute(sql, data)
		con.commit()
	except Exception, e:
		print e	

def select_db(cur, sql):
	try:
		cur.execute(sql)
		res = cur.fetchall()
	except Exception, e:
		print e
	return res

def update_db(con, cur, sql, data, many = 0):
	try:
		if many == 0:
			cur.execute(sql, data)
		elif many == 1:
			cur.executemany(sql, data)	
		con.commit()
	except Exception, e:
		print e

def insertmany_db(con, cur, sql, data):
	try:
		cur.executemany(sql, data)
		con.commit()
	except Exception, e:
		print e	

def broke_proxy():
	check_network_status_cmd = 'nmcli con status'
	if 'Ubuntu 15' in commands.getstatusoutput('cat /etc/issue')[1]:
                check_network_status_cmd = 'nmcli connection show --active'
	try:
                result = commands.getstatusoutput(check_network_status_cmd)
                print result, 'resssssssssssssssssss'
#               res = re.findall('(51vpn.*\d+) ', result[1])
                res = re.findall('(51vpn.+\d{1,3}\.\d{1,3}\.\d{1,3}) ', result[1])
                if res:
                        current_proxy = res[0]
                        cmd = 'nmcli con down id %s' % current_proxy
                        os.system(cmd)
                else:
                        print 'errorrrrrrrrrrrrrrrrrr'
        except Exception, e:
                print e
#                        self.logger.error('broke proxy error')

def check_network():
        count = 0
	check_network_status_cmd = 'nmcli con status'
	if 'Ubuntu 15' in commands.getstatusoutput('cat /etc/issue')[1]:
                check_network_status_cmd = 'nmcli connection show --active'

        while True:
                result = commands.getstatusoutput(check_network_status_cmd)
#                                print result, 'resssssssssssssssssss'
                res11 = re.findall('(51vpn.+\d{1,3}\.\d{1,3}\.\d{1,3}) ', result[1])
                if not res11:
                        print 'proxy is not ok', count
                        broke_proxy()
                        time.sleep(20)
                else:
                        print 'proxy is ok'
                        break
                count += 1

def get_domain_from_url(url):
	url = url.strip().split('//', 1)[-1].split('/', 1)[0].split('?', 1)[0].lower()
	if url.startswith('www.'):
		url = url[4:]
        return url
