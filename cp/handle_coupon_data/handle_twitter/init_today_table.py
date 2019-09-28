import MySQLdb as mdb
import config

class Today_table():

	def __init__(self):
		self.DB_HOST = config.DB_HOST
		self.DB_NAME = config.DB_NAME
		self.DB_USER = config.DB_USER
		self.DB_PSWD = config.DB_PSWD
		self.CHARSET = config.CHARSET


		# self.today_table = "coupon_twitter_today"
		# self.today_table_all = "coupon_twitter_today_all"
		# self.today_table_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, url"
		# self.today_fresh_table = "coupon_twitter_today_copy"
		# self.today_fresh_table_all = "coupon_twitter_today_copy_all"
		# self.today_fresh_table_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, redirect_url, landing_page, url_status, url"

		# self.inner_twitter_table = "coupon_twitter_innertwitter"			
		# self.inner_twitter_table_all = "coupon_twitter_innertwitter_all"
		# self.inner_twitter_table_fields = "source, advertiserid, brand, title, description, code, end_date, start_date, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, guid, advertisername, linkid, redirect_url, landing_page, url_status, url"

		self.today_fresh_table = config.dup_base_table
		self.today_fresh_table_all = config.dup_baseall_table
		self.today_fresh_table_fields = config.twitter_today_copy_toall

		self.inner_twitter_table = config.dup_innertwitter_table			
		self.inner_twitter_table_all = config.dup_innertwitterall_table
		self.inner_twitter_table_fields = config.twitter_innercoupon_toall


	def connect_db(self):
		self.coupon_con = mdb.connect(host=self.DB_HOST, user=self.DB_USER, passwd=self.DB_PSWD, db=self.DB_NAME, charset=self.CHARSET)
		self.coupon_cur = self.coupon_con.cursor()

	def close_db(self):
		self.coupon_cur.close()
		self.coupon_con.close()

	def auto_increment(self, table_name):
		sql_change_autoincrement = "ALTER TABLE %s AUTO_INCREMENT = 1" % (table_name, )
		re_auto = self.coupon_cur.execute(sql_change_autoincrement)
		self.coupon_con.commit()
		return re_auto

	def init_today_table(self):
		sql_transfer = "INSERT INTO %s (%s) SELECT %s from %s" \
		% (self.today_table_all, self.today_table_fields, 
			self.today_table_fields, self.today_table)
		self.coupon_cur.execute(sql_transfer)

		sql_delete = "delete from %s" % (self.today_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()
		self.auto_increment(self.today_table_all)


	def init_today_copy_table(self):
		sql_transfer = "INSERT INTO %s (%s) SELECT %s from %s" \
		% (self.today_fresh_table_all, self.today_fresh_table_fields, 
			self.today_fresh_table_fields, self.today_fresh_table)
		self.coupon_cur.execute(sql_transfer)
		self.auto_increment(self.today_fresh_table_all)

		#why not truncate
		sql_delete = "delete from %s" % (self.today_fresh_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()


	def init_inner_twitter_table(self):
		sql_transfer = "INSERT INTO %s (%s) SELECT %s from %s" \
		% (self.inner_twitter_table_all, self.inner_twitter_table_fields, 
			self.inner_twitter_table_fields, self.inner_twitter_table)
		print sql_transfer
		self.coupon_cur.execute(sql_transfer)
		self.auto_increment(self.inner_twitter_table_all)

		sql_delete = "delete from %s" % (self.inner_twitter_table)
		self.coupon_cur.execute(sql_delete)
		self.coupon_con.commit()

if "__main__" == __name__:
	table = Today_table()
	table.connect_db()
	# table.init_today_table()
	table.init_today_copy_table()
	# table.init_inner_twitter_table()
	table.close_db()
	print
	print "init today table, done!!"