from hashlib import md5
import re

def get_guid(string_md5):
	guid = md5(string_md5.lower()).hexdigest()

def transform_time_format(coupontime):
	time_list = coupontime.split()
	year_month_day = time_list[0].split('-')
	uniform_time = year_month_day[1] + '/' + year_month_day[2] + '/' + year_month_day[0]
	if len(time_list) > 1:
		uniform_time = uniform_time + ' ' + time_list[1]
	return uniform_time
	

def check_discount(data):
	for item in data:
		if not item:
			continue
		off_list = []
		off_list = re.findall('(\d+%) off', item.lower())
		if off_list:
			return off_list[0]
		off_list = re.findall('(\$\d+) off', item.lower())
		if off_list:
			return off_list[0]

def check_freeshipping(data):
        for item in data:
                if not item:
                        continue
                if 'free ship' in item.lower():
                        return True
        return False

def process_couponcode(couponcode):
	if couponcode:
		couponcode = re.sub('<.*?>', '', couponcode)
		couponcode_lower = couponcode.lower()
		if 'http' in couponcode or 'www' in couponcode or '.com' in couponcode:
			couponcode = ''
		elif 'n/a' in couponcode_lower or len(couponcode) == 1:
			couponcode = ''
		elif 'no ' in couponcode_lower and 'code' in couponcode_lower:
			couponcode = ''
		elif 'not ' in couponcode_lower and 'code' in couponcode_lower:
			couponcode = ''
		elif 'various code' in couponcode_lower:
			couponcode = ''
		if len(re.findall(':', couponcode)) == 1:
			couponcode = couponcode.split(':')[-1].strip()
        return couponcode

