# -*- coding:utf-8 -*-
# e.g. 
# python delete_files.py /home/moma/aaaaaaaa 0

import os
import re
import shutil
import sys
from datetime import datetime as dt
import datetime


def delete_file( file_in ):
    print file_in
    try:
        if os.path.isdir( file_in ):
            shutil.rmtree( file_in )
        elif os.path.isfile( file_in ):
            os.remove( file_in )
    except Exception, e:
        print "delete error"
        print "!" * 200
        print file_in

def get_expire_time( ):
    days_num = datetime.timedelta(max_expire_time)
    d = (dt.now()-days_num)
    return int( d.strftime( "%Y%m%d" ) )


def judge_illegal( file_in ):
    time_min = get_expire_time()
    pattern = re.compile(r"(20[\d]{2})[_-]?([012][\d])[_-]?([0123][\d])")
    date_re_all = pattern.findall( file_in )
    for date in date_re_all:
        if 0 < int(date[1]) < 13 and 0 < int(date[2]) < 32:
            if int("".join(date)) < time_min:
                return True
            break
    return False

def gci( src_dir, recursion ):
    """this is a statement"""
    parents = os.listdir(src_dir)
    for parent in parents:
        child = os.path.join(src_dir,parent)
        if judge_illegal( parent ):
            print child
            delete_file( child )
        elif recursion:
            if os.path.isdir(child):
                gci( child, recursion )


if "__main__" == __name__:
    # src_dir = sys.argv[1] #dir1
    # recursion = int(sys.argv[2]) #what if the dir has dirinside for output result

    src_dirs_base = ["/disk1/data/twitter", "/disk1/data/webpage"]
    recursion = 1 #what if the dir has dirinside for output result

    
    max_expire_time = 30
    for src_dir in src_dirs_base:
        gci( src_dir, recursion )
