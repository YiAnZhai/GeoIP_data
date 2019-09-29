# !/usr/bin/env python
# -*- coding:utf-8 -*-
from os.path import *
import io, os, sys
import re
from lxml import etree
import gzip, glob, time
import urllib2

sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), "./mysql"))
from mysql import Mysqldb


def generate_xml(filename, url_list, lastmod_time):
    """Generate a new xml file use url_list"""
    root = etree.Element('urlset',
                         xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    for each in url_list:
        url = etree.Element('url')
        loc = etree.Element('loc')
        lastmod = etree.Element('lastmod')
        changefreq = etree.Element('changefreq')
        loc.text = each
        lastmod.text = lastmod_time
        changefreq.text = "daily"
        url.append(loc);url.append(lastmod);url.append(changefreq)
        root.append(url)
 
    header = u'<?xml version="1.0" encoding="UTF-8"?>\n'
    s = etree.tostring(root, encoding='utf-8', pretty_print=True)
    with io.open(filename, 'w', encoding='utf-8') as f:
        print type(header), type(s)
        # f.write(unicode(header+s))
        f.write(header)
        f.write(s.decode("utf8"))
 

# def generate_xml(filename, url_list):
#     with gzip.open(filename,"w") as f:
#         f.write("""<?xml version="1.0" encoding="utf-8"?>
# <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n""")
#         for i in url_list:
#             f.write("""<url><loc>%s</loc></url>\n"""%i)
#         f.write("""</urlset>""")

 
######### def update_xml(filename, url_list):
#########     """Add new url_list to origin xml file."""
#########     f = open(filename, 'r')
#########     lines = [i.strip() for i in f.readlines()]
#########     f.close()
######## 
#########     old_url_list = []
#########     for each_line in lines:
#########         d = re.findall('<loc>(http:\/\/.+)<\/loc>', each_line)
#########         old_url_list += d
#########     url_list += old_url_list
######## 
#########     generate_xml(filename, url_list)
######## 
######## 
######### def append_xml(filename, url_list):
#########     with gzip.open(filename, 'r') as f:
#########         for each_line in f:
#########             d = re.findall('<loc>(http:\/\/.+)<\/loc>', each_line)
#########             url_list.extend(d)
######## 
#########         generate_xml(filename, set(url_list))
########
########
######### def generatr_xml_index(filename, sitemap_list, lastmod_list):
#########     """Generate sitemap index xml file."""
#########     root = etree.Element('sitemapindex',
#########                          xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
#########     for each_sitemap, each_lastmod in zip(sitemap_list, lastmod_list):
#########         sitemap = etree.Element('sitemap')
#########         loc = etree.Element('loc')
#########         loc.text = each_sitemap
#########         lastmod = etree.Element('lastmod')
#########         lastmod.text = each_lastmod
#########         sitemap.append(loc)
#########         sitemap.append(lastmod)
#########         root.append(sitemap)
######## 
#########     header = u'<?xml version="1.0" encoding="UTF-8"?>\n'
#########     s = etree.tostring(root, encoding='utf-8', pretty_print=True)
#########     with io.open(filename, 'w', encoding='utf-8') as f:
#########         f.write(unicode(header+s))
 
 
def modify_time(filename=None):
    if filename:
        time_stamp = os.path.getmtime(filename)
    else:
        time_stamp = time.time()
    t = time.localtime(time_stamp)
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', t)

 
def new_xml(filename, url_list):
    num_per_map = 2000
    lastmod = modify_time()
    for i in range((len(url_list)-1)/num_per_map+1):
        generate_xml(filename+"-%s"%i+"-urls.xml", url_list[i*num_per_map:(i+1)*num_per_map], lastmod)
    root = dirname(filename)
 
    with open(join(root, INDEX_SITEMAP),"w") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n')
        for i in glob.glob(join(root,"*urls.xml")):
            # i = i[len(CONFIG.SITEMAP_PATH):]
            lastmod = modify_time(i)
            path = join(ROOT_PATH_SITEMAP, basename(i))
            f.write("  <sitemap>\n    <loc>%s</loc>\n    "%path)
            f.write("<lastmod>%s</lastmod>\n  </sitemap>\n"%lastmod)
        f.write('</sitemapindex>')

def get_tag_url_list():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma", db="instagram_data_analysis", charset="utf8mb4")
    urls = msl.execute_sql("SELECT tag from instag_extend_tag where tag is not null")
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]

def get_user_url_list():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma", db="instagram_data_analysis", charset="utf8mb4")
    urls = msl.execute_sql("SELECT concat(user_name,'/',user_id) from instagram_user_verified_info_following where is_private=0  order by num_followers desc limit 1000000")
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_eco():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma", db="instagram_data_analysis", charset="utf8mb4")
    # sql = "SELECT concat(username,'/',userid) from insuser_info limit 1000000"
    sql = "SELECT concat(user_name,'/',user_id) from instagram_user_verified_info_following where is_private=0 order by rand() limit 1000000"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_insmeta_after():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma1601cool", db="instagram_user_extend", charset="utf8mb4")
    # sql = "SELECT concat(username,'/',userid) from insuser_info limit 1000000"
    sql = "SELECT user_slug from ins_user_relation_extend_uniq where is_private = 0 and follower_count > 1000"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_insmeme_after():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma1601cool", db="instagram_user_extend", charset="utf8mb4")
    # sql = "SELECT concat(username,'/',userid) from insuser_info limit 1000000"
    sql = "SELECT user_slug from ins_user_relation_extend_uniq where is_private = 0 and follower_count > 100"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_twiview():
    msl = Mysqldb(host="54.39.133.196", user="root", port=3306,\
                passwd="moma1601cool", db="twitter_user_extend", charset="utf8mb4")
    # sql = "SELECT screen_name from twitter_user where protected=0 and  statuses_count > 10 and followers_count > 10"
    sql = "SELECT screen_name from twitter_user_porn_all_gr1000_nodup where protected=0 and  statuses_count > 10 and followers_count > 10"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_twianon():
    msl = Mysqldb(host="54.39.133.196", user="root", port=3306,\
                passwd="moma1601cool", db="twitter_user_extend", charset="utf8mb4")
    # sql = "SELECT screen_name from twitter_user where protected=0 and  statuses_count > 10 and followers_count > 10"
    sql = "SELECT screen_name from twitter_user_twianon_extend where protected=0 and followers_count > 9500"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_insdear():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma", db="instagram_data_analysis", charset="utf8mb4")
    urls = msl.execute_sql("""SELECT concat(user_name,'/',user_id) from instagram_user_verified_info_following where is_private=0 and num_followers > 100
        and num_medias > 100 order by num_followers desc""")
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]


def get_user_url_list_instagzone():
    msl = Mysqldb(host="localhost", user="root", port=3306,\
                passwd="moma1601cool3.14!q1", db="instagram_user_extend", charset="utf8mb4")
    # sql = "SELECT concat(username,'/',userid) from insuser_info limit 1000000"
    sql = "SELECT user_slug from ins_user_relation_extend_uniq where is_private = 0 and follower_count > 3000"
    urls = msl.execute_sql( sql )
    return [join(URL_PRE,urllib2.quote(u[0].encode("utf8"))) for u in urls]

# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.instagimg.com/sitemap-cjdx-12hfihgnting-tags"
#     INDEX_SITEMAP = "sitemap-tags-index.xml"
#     URL_PRE = "http://www.instagimg.com/tag"
#     urls = get_tag_url_list()
#     new_xml('/disk1/data/webpage/instag_sitemap_tag/tags', urls)
#     # generatr_xml_index('index.xml', urls, mods)


# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.insgain.com/sitemap-dfg-123figh-user"
#     INDEX_SITEMAP = "sitemap-user-index.xml"
#     URL_PRE = "http://www.insgain.com/user"
#     urls = get_user_url_list()
#     new_xml('/disk1/data/webpage/instag_sitemap_user/users', urls)



# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.insmeta.com/sitemap-cjdx-ins"
#     INDEX_SITEMAP = "sitemap-u-index.xml"
#     URL_PRE = "http://www.insmeta.com/user"
#     urls = get_user_url_list()
#     new_xml('/disk1/data/webpage/insmeta_sitemap_user/u', urls)


# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.insmeta.com/sitemap-cjdx-ins"
#     INDEX_SITEMAP = "sitemap-u-index.xml"
#     URL_PRE = "http://www.insmeta.com/user"
#     urls = get_user_url_list_eco()
#     new_xml('/disk1/data/webpage/insmeta_sitemap_urls/eco', urls)


# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.insmeme.com/sitemap-ins-urls"
#     INDEX_SITEMAP = "sitemap-index.xml"
#     URL_PRE = "http://www.insmeme.com/user"
#     urls = get_user_url_list_eco()
#     new_xml('/disk1/data/webpage/insmeme_sitemap_urls/stmp', urls)


# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.twiview.com/sitemap-twiview-urls"
#     INDEX_SITEMAP = "sitemap-index.xml"
#     URL_PRE = "http://www.twiview.com/twiuser"
#     urls = get_user_url_list_twiview()
#     new_xml('/disk1/data/webpage/twiview_sitemap_urls/sitemap', urls)


# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.twianon.com/sitemap-anonmap"
#     INDEX_SITEMAP = "sitemap-index.xml"
#     URL_PRE = "http://www.twianon.com/twiuser"
#     urls = get_user_url_list_twiview()
#     new_xml('/disk1/data/webpage/twianon_sitemap_urls/sitemap', urls)


###########################################################################################
# if __name__ == '__main__':
#     ROOT_PATH_SITEMAP = "http://www.insdear.com/sitemap-urls"
#     INDEX_SITEMAP = "sitemap-index.xml"
#     URL_PRE = "http://www.insdear.com/user"
#     urls = get_user_url_list_insdear()
#     new_xml('/disk1/data/webpage/sitemap_insdear/sitemap', urls)


if __name__ == '__main__':
    ROOT_PATH_SITEMAP = "http://www.instagzone.com/sitemap-urls"
    INDEX_SITEMAP = "sitemap-index.xml"
    URL_PRE = "http://www.instagzone.com/user"
    urls = get_user_url_list_instagzone()
    new_xml('/var/www/sitemap_instagzone/sitemap', urls)