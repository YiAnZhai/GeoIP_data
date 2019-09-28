import MySQLdb as mdb
import config_couponapi

class Today_table():

	def __init__(self):
		self.DB_HOST = config_couponapi.DB_HOST
		self.DB_NAME = config_couponapi.DB_NAME
		self.DB_USER = config_couponapi.DB_USER
		self.DB_PSWD = config_couponapi.DB_PSWD
		self.CHARSET = config_couponapi.CHARSET

		self.today_table = config_couponapi.coupon_apifresh_table
		self.today_table_all = config_couponapi.coupon_apifresh_all_table
		self.today_table_fields = config_couponapi.api_fresh_fields
		self.today_copy_table = config_couponapi.api_fresh_copy_table			
		self.today_copy_table_all = config_couponapi.api_fresh_copy_all_table
		self.today_copy_table_fields = config_couponapi.api_fresh_copy_fields

	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def init_today_table(self):
		sql_transfer = "INSERT INTO %s (%s) SELECT %s from %s" % (self.today_table_all, self.today_table_fields, self.today_table_fields, self.today_table)
		self.coupon_cur.execute(sql_transfer)

		sql_delete = "delete from %s" % (self.today_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()


	def init_today_copy_table(self):
		sql_transfer = "INSERT INTO %s (%s) SELECT %s from %s" % (self.today_copy_table_all, self.today_copy_table_fields, self.today_copy_table_fields, self.today_copy_table)
		self.coupon_cur.execute(sql_transfer)

		sql_delete = "delete from %s" % (self.today_copy_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()

if "__main__" == __name__:
	table = Today_table()
	table.connect_db()
	table.init_today_table()
	table.init_today_copy_table()
	table.close_db()
