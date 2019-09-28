# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field
from scrapy import Item

class RedurlItem(scrapy.Item):
    guid = scrapy.Field()
    origin_url = scrapy.Field()
    origin_red_url = scrapy.Field()
    red_url = scrapy.Field()
    url_status = scrapy.Field()
    error_info = scrapy.Field()
    src = scrapy.Field()
    middle_urls = scrapy.Field()

class RedurlItem_domain(scrapy.Item):
    guid = scrapy.Field()
    origin_url = scrapy.Field()
    red_url = scrapy.Field()
    url_status = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    error_info = scrapy.Field()
    src = scrapy.Field()
    middle_urls = scrapy.Field()


class CouponItem(Item):
    source = Field()
    advertiserid = Field()
    advertisername = Field()
    title = Field()
    description = Field()
    trackingurl = Field()
    code = Field()
    end_date = Field()
    start_date = Field()
    url = Field()
    category = Field()
    restriction = Field()
    keywords = Field()
    htmlofdeal = Field()
    promotiontypes = Field()
    linkid = Field()
    freeshipping = Field()
    discount = Field()
    affiliate_status = Field()
    is_new  = Field()

class AdvertiserItem(Item):
    source = Field()
    advertiserid = Field()
    program_url = Field()
    domain = Field()

class CouponTTItem(Item):
    source = Field()
    advertiserid = Field()
    advertisername = Field()
    title = Field()
    description = Field()
    code = Field()
    end_date = Field()
    start_date = Field()
    url = Field()
    category = Field()
    restriction = Field()
    keywords = Field()
    htmlofdeal = Field()
    promotiontypes = Field()
    linkid = Field()
    freeshipping = Field()
    discount = Field()

class RakutenmarketingItem(Item):
    cat = Field()
    promotiontypes = Field()
    offerdescription = Field()
    offerstartdate = Field()
    offerenddate = Field()
    couponcode = Field()
    couponrestriction = Field()
    clickurl = Field()
    impressionpixel = Field()
    advertiserid = Field()
    advertisername = Field()
    network = Field()

class LinkshareItem(Item):
    couponid = Field()
    destination = Field()

class URLItem(Item):
    couponid = Field()
    destination = Field()
    urlstatus = Field()

class AmazonCouponItem(Item):
    asin = Field()
    link = Field()
    title = Field()
    last_price = Field()
    price = Field()
    saved_price = Field()
    saved_percent = Field()
    coupon_cannon = Field()
    product_description = Field()
    sales_rank = Field()
    cat = Field()
    brand = Field()
    merchant = Field()

class CjCouponItem(Item):
    advertiser_id = Field()
    advertiser_name = Field()
    category = Field()
    click_commission = Field()
    creative_height = Field()
    creative_width = Field()
    language = Field()
    lead_commission = Field()
    description = Field()
    destination = Field()
    link_id = Field()
    link_name = Field()
    link_type = Field()
    performance_incentive = Field()
    promotion_end_date = Field()
    promotion_start_date = Field()
    promotion_type = Field()
    couponcode = Field()
    sale_commission = Field()
    seven_day_epc = Field()
    three_month_epc = Field()

class CjAdvertiserItem(Item):
    advertiser_id = Field()
    advertiser_name = Field()
    program_url = Field()
    category = Field()
    account_status = Field()
    seven_day_epc = Field()
    three_month_epc = Field()
    language = Field()
    mobile_tracking_certified = Field()
    network_rank = Field()
    performance_incentives = Field()

class SasCouponItem2(Item):
    couponid = Field()
    landingpage = Field()

class SasCouponItem(Item):
    merchantid = Field()
    merchantname = Field()
    title = Field()
    description = Field()
    dealstartdate = Field()
    dealenddate = Field()
    dealpublishdate = Field()
    couponcode = Field()
    keywords = Field()
    category = Field()
    restrictions = Field()
    commissionpercentage = Field()
    dealtitle = Field()
    imagebig = Field()
    imagesmall = Field()
    htmlofdeal = Field()
    link = Field()
    landingpage = Field()

class SasAdvertiserItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    merchantid = Field()
    brand = Field()
    hp = Field()
    category = Field()
    commission = Field()
    seven_day_epc = Field()
    one_month_epc = Field()
