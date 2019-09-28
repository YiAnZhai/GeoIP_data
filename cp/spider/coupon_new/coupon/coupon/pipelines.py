import sys
reload(sys)
sys.setdefaultencoding('utf8')
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
from twisted.enterprise import adbapi
from items import SasAdvertiserItem, SasCouponItem, CjAdvertiserItem, CjCouponItem, AmazonCouponItem, SasCouponItem2, \
RakutenmarketingItem, LinkshareItem, URLItem, CouponTTItem, AdvertiserItem, CouponItem, RedurlItem, RedurlItem_domain
from hashlib import md5
import re
import utils
from scrapy.exceptions import DropItem
from scrapy import log

class DataProcessingPipeline(object):
	def __init__(self):
		self.id_seen = set()
		print 'yesssssssss222222222222222'

	def process_item(self, item, spider):
		print 'noooooooooooooo'
		if item['start_date']:
			item['start_date'] = utils.transform_time_format(item['start_date'])
		if item['end_date']:
			##### ongoing
			if item['end_date'] == 'ongoing':
				item['end_date'] = item['end_date'];
			else:
				item['end_date'] = utils.transform_time_format(item['end_date'])
		if item['linkid']:
			if item['linkid'] in self.id_seen:
				raise DropItem('Duplicate item found: %s' % item)
			else:
				self.id_seen.add(item['linkid'])

		if 'freeshipping' not in item:
			if utils.check_freeshipping((item['title'], item['description'], item['keywords'], item['htmlofdeal'])):
				item['freeshipping'] = '1'
				print 'yesssssssssss'
			else:
				item['freeshipping'] = '0'
				print 'noooooooooo'

		item['discount'] = utils.check_discount((item['description'], item['title']))
		item['code'] = utils.process_couponcode(item['code'])

		print item, '---------------------'
		return item

class UniformMySQLStorePipeline2(object):
	def __init__(self, dbpool):
		self.dbpool = dbpool

	@classmethod
	def from_settings(cls, settings):
		dbargs = dict(
			host=settings['MYSQL_HOST'],
			db=settings['MYSQL_DBNAME'],
			user=settings['MYSQL_USER'],
			passwd=settings['MYSQL_PASSWD'],
			charset='utf8',
			use_unicode=True,
		)
		dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
		return cls(dbpool)

	def process_item(self, item, spider):
		# run db query in the thread pool
		if isinstance(item, CouponItem) or isinstance(item, CouponTTItem):
			d = self.dbpool.runInteraction(self._do_upsert_coupon, item, spider)
		elif isinstance(item, AdvertiserItem):
			print 'an advertiser itemmmmmmmmmmmmmmmm'
			d = self.dbpool.runInteraction(self._do_upsert_advertiser, item, spider)
		elif isinstance(item, RedurlItem):
			d = self.dbpool.runInteraction(self._do_upsert_redurl, item, spider)
		elif isinstance(item, RedurlItem_domain):
			d = self.dbpool.runInteraction(self._do_upsert_redurl_middle, item, spider)


		d.addErrback(self._handle_error, item, spider)
		# at the end return the item in case of success or failure
		d.addBoth(lambda _: item)
		# return the deferred instead the item. This makes the engine to
		# process next item (according to CONCURRENT_ITEMS setting) after this
		# operation (deferred) has finished.
		return d

	def _get_redurl_guid(self, item):
		return md5(item['origin_url']).hexdigest()

	def _get_guid(self, string_to_encode):
		return md5(string_to_encode).hexdigest()

	def _do_upsert_redurl(self, conn, item, spider):
		print 'yesssssssssssssss, getredurl'
		# guid = self._get_redurl_guid(item)
		guid = item["guid"]
		conn.execute("""SELECT EXISTS(SELECT 1 FROM get_red_url WHERE guid = %s)""", (guid, ))
		ret = conn.fetchone()[0]
		#print 'yesssssssssssss1'
		if not ret:
			sql = """INSERT INTO get_red_url ( guid, middle_urls, origin_url, origin_red_url, red_url, url_status, error_info, src, spider_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
	 #       print 'yesssssssss2'
			try:
				conn.execute(sql, (guid, item["middle_urls"], item["origin_url"], item["origin_red_url"], item["red_url"], item["url_status"], item["error_info"], item["src"], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
			except Exception, e:
				print 'error', e
			spider.log("Item stored in db: %r" % (item))

	def _do_upsert_redurl_middle(self, conn, item, spider):
		print 'yesssssssssssssss, getredurl'
		# guid = self._get_redurl_guid(item)
		guid = item["guid"]
		conn.execute("""SELECT EXISTS(SELECT 1 FROM get_domain_titledescription WHERE guid = %s)""", (guid, ))
		ret = conn.fetchone()[0]
		#print 'yesssssssssssss1'
		if not ret:
			sql = """INSERT INTO get_domain_titledescription ( domain, middle_urls, origin_url, red_url, url_status, title, description, error_info, src, spider_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
	 #       print 'yesssssssss2'
			try:
				conn.execute(sql, (guid, item["middle_urls"], item["origin_url"], item["red_url"], item["url_status"], item["title"], item["description"], item["error_info"], item["src"], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
			except Exception, e:
				print 'error', e
			spider.log("Item stored in db: %r" % (item))


	def _do_upsert_advertiser(self, conn, item, spider):
		sql = 'insert into cj_sas_linkshare_brand (advertiserid, source, website, domain, add_date) values (%s, %s, %s, %s, %s)'
		try:
			conn.execute(sql, (item['advertiserid'], item['source'], item['program_url'], item['domain'], datetime.datetime.now()))
		except Exception, e:
			print 'error', e
			f = open('error.log', 'a')
			f.write(str(e)+'\n')
			f.close()

	def _do_upsert_coupon(self, conn, item, spider):
		"""Perform an insert or update."""
#		attr_list = ['title', 'description', 'code', 'url']
#		str_list = [item[attr].lower() for attr in attr_list if item[attr]]
#		mess = ''.join(str_list)
#		guid = self._get_guid(mess)
		sql = 'insert into coupon_api_today(linkid, source, advertiserid, title, description, `code`, end_date, start_date, url, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, advertisername) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
		if isinstance(item, CouponTTItem):
			sql = 'insert into coupon_twitter_today(linkid, source, advertiserid, title, description, `code`, end_date, start_date, url, category, restriction, keywords, htmlofdeal, promotiontypes, adddate, freeshipping, discount, advertisername) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
		try:
			conn.execute(sql, (item['linkid'], item['source'], item['advertiserid'], item['title'], item['description'], item['code'], item['end_date'], item['start_date'], item['url'], item['category'], item['restriction'], item['keywords'], item['htmlofdeal'], item['promotiontypes'], datetime.datetime.now(), item['freeshipping'], item['discount'], item['advertisername']))
		except Exception, e:
			print 'errorrrrrrrrrr22', e
			f = open('error.log', 'a')
			f.write(str(e)+'\n')
			f.close()
		sql_linkid = "insert into linkid (linkid, source, adddate) values (%s, %s, %s)"
		try:
			conn.execute(sql_linkid, (item['linkid'], item['source'], datetime.datetime.now()))
		except Exception, e:
			print 'errorrrrrr33', e
			f = open('error.log', 'a')
			f.write(str(e)+'\n')
			f.close()
		####### add affiliate flag for api
		if isinstance(item, CouponItem) and item['affiliate_status']!='' and item['affiliate_status']!='notjoined':
			print "###################  affiliate coupon   #####################"
			sql_linkid = "insert into coupon_api_affiliate_status (linkid, source, affiliate_status, is_new, add_date) values (%s, %s, %s, %s, %s)"
			try:
				conn.execute(sql_linkid, (item['linkid'], item['source'], item['affiliate_status'], item['is_new'], datetime.datetime.now()))
			except Exception, e:
				print 'errorrrrrr44', e
				f = open('error.log', 'a')
				f.write(str(e)+'\n')
				f.close()

	def _handle_error(self, failure, item, spider):
		"""Handle occurred on db interaction.""" # do nothing, just log
		print failure
		log.err(failure)
		f = open('error.log', 'a')
		f.write(str(failure)+'\n')
		f.close()

class UniformMySQLStorePipeline(object):
	def __init__(self, dbpool):
		self.dbpool = dbpool
		print 'hello, inittttttttttt'

		@classmethod
		def from_settings(cls, settings):
			dbargs = dict(
				host=settings['MYSQL_HOST'],
				db=settings['MYSQL_DBNAME'],
				user=settings['MYSQL_USER'],
				passwd=settings['MYSQL_PASSWD'],
				charset='utf8',
				use_unicode=True,
			)
			dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
			return cls(dbpool)
	def process_item(self, item, spider):
		# run db query in the thread pool
		d = self.dbpool.runInteraction(self._do_upsert, item, spider)
		d.addErrback(self._handle_error, item, spider)
		# at the end return the item in case of success or failure
		d.addBoth(lambda _: item)
		# return the deferred instead the item. This makes the engine to
		# process next item (according to CONCURRENT_ITEMS setting) after this
		# operation (deferred) has finished.
		return d

	def _get_guid(self, item):
		"""Generates an unique identifier for a given item."""
		# hash based solely in the url field
		return md5(item['img_name']).hexdigest()

	def _get_cjcoupon_guid(self, item):
		mes = item['link_name'] + item['destination'] + item['category'] + item['promotion_end_date'] + item['promotion_start_date'] + item['advertiser_name'] + item['description'] + item['couponcode']
		return md5(mes).hexdigest()

	def _get_sasadv_guid(self, item):
		return md5(item['merchantid']).hexdigest()

	def _get_sascou_guid(self, item):
		return md5(item['link']).hexdigest()

	def _get_rakutenmarketing_guid(self, item):
		return md5(item['clickurl']).hexdigest()

	def _do_upsert(self, conn, item, spider):
		"""Perform an insert or update."""
		  # guid = self._get_guid(item)
		  # conn.execute("""SELECT EXISTS(
		  #         SELECT 1 FROM googleimg2 WHERE guid = %s
		  # )""", (guid, ))
		  # ret = conn.fetchone()[0]
		  # if not ret:
		  #  print item, 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', isinstance(item, RakutenmarketingItem)
		if isinstance(item, URLItem):
			sql = "update coupon_api_today_copy set final_url = %s, url_status = %s where id = %s"
			try:
				conn.execute(sql, (item['destination'], item['urlstatus'], item['couponid']))
			except Exception, e:
				print 'error', e

		if isinstance(item, LinkshareItem):
			sql = "update coupon_api_copy set url = %s where id = %s"
			try:
				conn.execute(sql, (item['destination'], item['couponid']))
			except Exception, e:
				print 'error', e

		if isinstance(item, RakutenmarketingItem):
			print 'yesssssssssssssss, rakutenmarketingItem'
			guid = self._get_rakutenmarketing_guid(item)
			conn.execute("""SELECT EXISTS(SELECT 1 FROM spider_coupon_rakutenmarketing_api WHERE guid = %s)""", (guid, ))
			ret = conn.fetchone()[0]
			print 'yesssssssssssss1'
			if not ret:
				sql = """INSERT INTO spider_coupon_rakutenmarketing_api (guid, cat ,promotiontypes ,offerdescription ,offerstartdate ,offerenddate ,couponcode, couponrestriction, clickurl ,impressionpixel ,advertiserid ,advertisername ,network, spider_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
				print 'yesssssssss2'
				try:
					conn.execute(sql, (guid, item["cat"], item["promotiontypes"], item["offerdescription"], item["offerstartdate"], item["offerenddate"], item["couponcode"], item["couponrestriction"], item["clickurl"], item["impressionpixel"], item["advertiserid"], item["advertisername"], item["network"], datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
				except Exception, e:
					print 'error', e
				spider.log("Item stored in db: %r" % (item))

		elif isinstance(item, SasAdvertiserItem):
			try:
				guid = self._get_sasadv_guid(item)
				print guid, 'eeeeeeeeeeeeeeeeee'
				conn.execute("""SELECT EXISTS(
						SELECT 1 FROM shareasale_advertiser WHERE guid = %s
				)""", (guid, ))
				ret = conn.fetchone()[0]
				if not ret:
					# print item, 'yunnnnnnnnnnnnnnnnnn'
					sql = """insert into shareasale_advertiser (advertiser_id, advertiser_name, program_url, category, one_month_epc, seven_day_epc, add_date, commission, guid, update_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
					conn.execute(sql, (item['merchantid'], item['brand'], item['hp'], item['category'], item['one_month_epc'], item['seven_day_epc'], datetime.datetime.now(), item['commission'], guid, datetime.datetime.now()))
				else:
					sql = """update shareasale_advertiser set update_date = %s where advertiser_id = %s"""
				conn.execute(sql, (datetime.datetime.now(), item['merchantid']))

			except Exception, e:
				print 'error', e

		elif isinstance(item, SasCouponItem2):
			try:
				sql = """update shareasale_coupon set landingpage = %s where id = %s"""
				conn.execute(sql, (item['landingpage'], item['couponid']))
			except Exception, e:
				print 'error', e

		elif isinstance(item, SasCouponItem):
			try:
				guid = self._get_sascou_guid(item)
				print guid, 'eeeeeeeeeeeeeeeeee'
				conn.execute("""SELECT EXISTS(
						SELECT 1 FROM shareasale_coupon WHERE guid = %s
				)""", (guid, ))
				ret = conn.fetchone()[0]
				if not ret:
					# print item, 'yunnnnnnnnnnnnnnnnnn'
					sql = """insert into shareasale_coupon (landingpage, merchantid, merchantname, title, description, dealstartdate, dealenddate, dealpublishdate, couponcode, keywords, category, restrictions, commissionpercentage, dealtitle, imagebig, imagesmall, htmlofdeal, link, guid, add_date, update_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
					conn.execute(sql, (item['landingpage'], item['merchantid'], item['merchantname'], item['title'], item['description'], item['dealstartdate'], item['dealenddate'], item['dealpublishdate'], item['couponcode'], item['keywords'], item['category'], item['restrictions'], item['commissionpercentage'], item['dealtitle'], item['imagebig'], item['imagesmall'], item['htmlofdeal'], item['link'], guid, datetime.datetime.now(), datetime.datetime.now()))
				else:
					sql = """update shareasale_coupon set update_date = %s where link = %s"""
				conn.execute(sql, (datetime.datetime.now(), item['link']))
			except Exception, e:
				print 'error', e

		elif isinstance(item, CjAdvertiserItem):
	#		print 'yessssssssssssssssssssssss'
			try:
				conn.execute("""SELECT EXISTS(
						SELECT 1 FROM cj_advertiser WHERE advertiser_id = %s
				)""", (item['advertiser_id'], ))
				ret = conn.fetchone()[0]
				if not ret:
					# print item, 'yunnnnnnnnnnnnnnnnnn'
					sql = """insert into cj_advertiser (advertiser_id, advertiser_name, program_url, category, account_status, seven_day_epc, three_month_epc, language, mobile_tracking_certified, network_rank, performance_incentives, add_date, update_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
					conn.execute(sql, (item['advertiser_id'], item['advertiser_name'], item['program_url'], item['category'], item['account_status'], item['seven_day_epc'], item['three_month_epc'], item['language'], item['mobile_tracking_certified'], item['network_rank'], item['performance_incentives'], datetime.datetime.now(), datetime.datetime.now()))
				else:
					sql = """update cj_advertiser set update_date = %s where advertiser_id = %s"""
					conn.execute(sql, (datetime.datetime.now(), item['advertiser_id']))
			except Exception, e:
				print 'error', e
		elif isinstance(item, CjCouponItem):
			try:
				guid = self._get_cjcoupon_guid(item)
				conn.execute("""SELECT EXISTS(
						SELECT 1 FROM cj_coupon WHERE guid = %s
				)""", (guid, ))
#                                conn.execute("""SELECT EXISTS(
#                                       SELECT 1 FROM cj_coupon WHERE link_id = %s
#                              )""", (item['link_id'], ))
				print 'herehereherehere', guid
				ret = conn.fetchone()[0]
				if not ret:
					print item[guid], 'yunnnnnnnnnnnnnnnnnn'
					sql = """insert into cj_coupon (guid, advertiser_id, advertiser_name, category, click_commission, creative_height, creative_width, language, lead_commission, description, destination, link_id, link_name, link_type, performance_incentive, promotion_end_date, promotion_start_date, promotion_type, coupon_code, sale_commission, seven_day_epc, three_month_epc, add_date, update_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
					conn.execute(sql, (guid, item['advertiser_id'], item['advertiser_name'], item['category'], item['click_commission'], item['creative_height'], item['creative_width'], item['language'], item['lead_commission'], item['description'], item['destination'], item['link_id'], item['link_name'], item['link_type'], item['performance_incentive'], item['promotion_end_date'], item['promotion_start_date'], item['promotion_type'], item['couponcode'], item['sale_commission'], item['seven_day_epc'], item['three_month_epc'], datetime.datetime.now(), datetime.datetime.now()))
				else:
					sql = """update cj_coupon set update_date = %s where link_id = %s"""
					conn.execute(sql, (datetime.datetime.now(), item['link_id']))
			except Exception, e:
				print 'error', e

		elif isinstance(item, AmazonCouponItem):
#			print 'yessssssssssssssssssss'
			try:
				conn.execute("""SELECT EXISTS(SELECT 1 FROM amazon_coupon WHERE asin = %s)""", (item['asin']))
				ret = conn.fetchone()[0]
				if not ret:
					sql = """insert into amazon_coupon (asin, link, title, last_price, price, saved_price, saved_percent, coupon_cannon, product_description, sales_rank, cat, brand, merchant, add_date, update_date) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
				conn.execute(sql, (item['asin'], item['link'], item['title'], item['last_price'], item['price'], item['saved_price'], item['saved_percent'], item['coupon_cannon'], item['product_description'], item['sales_rank'], item['cat'], item['brand'], item['merchant'], datetime.datetime.now(), datetime.datetime.now()))
			except Exception, e:
				print 'error', e

	def _handle_error(self, failure, item, spider):
		"""Handle occurred on db interaction.""" # do nothing, just log
		log.err(failure)
