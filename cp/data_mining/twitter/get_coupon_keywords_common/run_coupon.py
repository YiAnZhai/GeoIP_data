# -*- coding: utf-8 -*-
#Import the necessary methods from tweepy library
import time
import subprocess
import os
import signal
from datetime import datetime as dt
import datetime

def exec_cmd(cmd, times):
	
	sub = subprocess.Popen(cmd, shell = True, preexec_fn = os.setsid)
	time.sleep(times)

	ret1 = subprocess.Popen.poll(sub)
	if ret1 == 0:
		pass
	elif ret1 is None:
		sub.terminate()
		sub.wait()
		# sub.kill()
		try:
			os.killpg(sub.pid, signal.SIGTERM)
		except OSError as e:
			pass
	else:
		print sub.pid,'term', ret1, ret1

	output = sub.communicate()
	print output


file_store_path = "/disk1/data/twitter/data_twitter_coupon_commonkeywords"
while True:
	time_start = dt.now()
	times_period = datetime.timedelta(days=0.1)
	time_end = time_start + times_period
	cmd = "python get_twitter_stream_coupon.py > '%s/coupon-%s->%s.txt'" % (file_store_path, time_start.strftime("%Y%m%d-%H%M"), time_end.strftime("%Y%m%d-%H%M"))
	exec_cmd(cmd, times_period.total_seconds())
	time.sleep(2)
