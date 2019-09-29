# -*- coding: utf-8 -*-
import MySQLdb as mdb
import sys
import os
import __main__
######################self define modules
from mysql_config import *



class Mysqldb():

    def __init__(self,host=MYSQL_HOST, user=MYSQL_USER, port=MYSQL_PORT,\
            passwd=MYSQL_PASSWD, db=MYSQL_DB_NAME, charset=CHART_SET ):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.db=db
        self.charset=charset
        self.port=port


    def connect_to_db(self):
        # self.mysql_con = mdb.connect(host=MYSQL_HOST, user=MYSQL_USER, \
        #     passwd=MYSQL_PASSWD, db=MYSQL_DB_NAME, charset=CHART_SET)
        self.mysql_con = mdb.connect(host=self.host, port=self.port, user=self.user, \
            passwd=self.passwd, db=self.db, charset=self.charset)
        self.mysql_cur = self.mysql_con.cursor()
        self.mysql_cur.execute("SET NAMES %s" % (self.charset, ))

    def close_db(self):
        self.mysql_cur.close()
        self.mysql_con.close()

    def execute_sql(self, sql):
        self.connect_to_db()
        self.mysql_cur.execute( sql )
        self.mysql_con.commit()
        results = self.mysql_cur.fetchall()
        self.close_db()
        return results


    def select_data(self, table, fields, where):
        self.connect_to_db()
        sql_select = """SELECT `%s` from `%s` %s""" \
            % ( "`,`".join(fields), table, where )

        self.mysql_cur.execute( sql_select )
        results = self.mysql_cur.fetchall()
        self.close_db()
        return results


    def insert_data(self, table, fields, data, many=False, n=100000):
        # assert()
        if not data:
            return
        self.connect_to_db()
        sql_insert = """INSERT INTO `%s` (`%s`) values (%s)""" \
            % ( table, "`,`".join(fields), ",".join(["%s"]*len(fields)) )
        if many:
            index = 0
            while index < len(data):
                self.mysql_cur.executemany( sql_insert, data[ index:index+n ] )
                index = index + n
        else:
            self.mysql_cur.execute( sql_insert, data )

        self.mysql_con.commit()
        self.close_db()


    def update_data(self, table, fields, data, locate_key, many=False):
        # assert()
        self.connect_to_db()
        set_string = ",".join( ["`%s` = %s" % (f,"%s") for f in fields[:-1]] )
        sql_update = """UPDATE `%s` SET %s where `%s` = %s""" \
            % ( table, set_string, locate_key, "%s" )
        if many:
            self.mysql_cur.executemany( sql_update, data )
        else:
            self.mysql_cur.execute( sql_update, data )

        self.mysql_con.commit()
        self.close_db()


    def auto_increment_back(self, table_name, begin_num=1):
        sql_change_autoincrement = "ALTER TABLE `%s` AUTO_INCREMENT = %s" % (table_name, begin_num, )
        self.connect_to_db()
        self.mysql_cur.execute( sql_change_autoincrement )
        self.mysql_con.commit()
        self.close_db()


    def backup_data(self, from_table, to_table, except_field=["id"], fields=[]):
        if fields:
            fields_str = "`, `".join(fields)
        else:
            f_fields = [a[0] for a in self.show_fields(from_table) if a[0] not in except_field]
            t_fields = [b[0] for b in self.show_fields(to_table) if b[0] not in except_field]
            if set(f_fields) == set(t_fields):
                fields_str = "`, `".join(f_fields)
            else:
                print "error: table fields not the same"
                print "from: %s-> to: %s" % (from_table, to_table)
                print "!"*200
                raise Exception("error: table fields not the same when backup")

        sql_backup = """INSERT into `%s` (`%s`) select `%s` from `%s`""" % \
            ( to_table, fields_str, fields_str, from_table )

        self.connect_to_db()
        self.mysql_cur.execute( sql_backup )
        self.mysql_con.commit()
        self.close_db()
        #######back the auto increment value
        self.auto_increment_back( to_table )


    def backup_table(self, from_table, to_table):
        sql_backup = """CREATE TABLE `%s` like `%s`""" % (to_table, from_table)

        self.connect_to_db()
        self.mysql_cur.execute( sql_backup )
        self.mysql_con.commit()
        self.close_db()



    def show_fields(self, table):
        sql_show = """SHOW fields from %s""" % (table, )

        self.connect_to_db()
        self.mysql_cur.execute( sql_show )
        self.mysql_con.commit()
        results = self.mysql_cur.fetchall()
        self.close_db()

        return results




if "__main__" == __name__:
    ad = Mysqldb()
    ad.format_data()