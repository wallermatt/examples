examples
========

Collection of (hopefully) interesting programs 


check_test_all.py
=================
Looks for arbitrage opportunities in the UK energy spot market.

It uses urllib2 to access the market's API. Then matches up buy and sell orders looking for profits. If there any it sends an SMS notification using Twilio. Finally it saves all order data to an SQLITE database for future analysis.
