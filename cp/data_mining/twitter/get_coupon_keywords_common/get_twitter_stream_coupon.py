# -*- coding: utf-8 -*-
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time



#develop_wang
access_token = "719861643849658368-F2IzjKIz2kxJjAXiiR0Q5W9Fz1stQFP"
access_token_secret = "jQLVbPVcM82ZHcjMeCm1ZTinhQ1hksJ2x8lwUT8ZFdXcY"
consumer_key = "l98SnRS4holpxpCN82xluj9EB"
consumer_secret = "UAiN9WMiv80JEuxPkuEX7zVzdezHhfK6tfWAs32dpPyTqyoeuJ"

#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        # print status
        raise Exception(status)
        pass

if __name__ == '__main__':
    while True:
        try:

            l = StdOutListener()
            auth = OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            stream = Stream(auth, l)
#            #This handles Twitter authetification and the connection to Twitter Streaming API
#            #This line filter Twitter Streams to capture data by the keywords: 'coupon', 'discount', coupon_code

#	    off_percent_list = ['%s%s off' % (i, "%") for i in range(1, 100)] + ["$%s off" % (i) for i in range(100)]
#	    track_list = off_percent_list + ['free off','discount off']
# 	    track_list = ['promo code','coupon code','deals code','discount code', 'code free', 'code off']
            track_list = ['code','off','coupon','coupons','codes','promo','promocode','discount','discounts','couponcode','sale','sales','deal','deals']
	    stream.filter(track=track_list, languages=["en"])
#	    stream.filter(track=track_list)

        except Exception, e:
            time.sleep(90)
            pass
            # print "error: ", e
