import urllib2
import time
import sqlite3
import sys
import pdb
from twilio.rest import TwilioRestClient


CONN = sqlite3.connect('apx_data.db') # connect to databaseb

NUMBER_OF_CHECKS = 10000  # how many times the program will load API
DELAY            = 10     # in seconds - limit on polling API

# set number to send SMS notification to
MY_NUMBER       = 'XXXXXX'


# sample test data 
# instrument code, market segment flag, buy/sell flag, price, volume, order id, version number, begintime, endtime
html_test = """2H131009-6A,S_1HB,B,87,50,9447604,0,2013-10-09 19:00:00,2013-10-09 20:00:00
HH131009-37,S_HH,S,42,26,9447872,2,2013-10-09 19:00:00,2013-10-09 19:30:00
HH131009-37,S_HH,S,42.5,8.6,9447880,1,2013-10-09 19:30:00,2013-10-09 20:00:00
2H131009-6A,S_1HB,S,62,50,9447604,0,2013-10-09 19:00:00,2013-10-09 20:00:00
HH131009-37,S_HH,S,42.5,8.6,9447880,1,2013-10-09 17:00:00,2013-10-09 17:30:00
2H131009-6A,S_1HB,B,100,50,9447604,0,2013-10-09 19:30:00,2013-10-09 21:00:00
HH131010-16,S_HH,B,45,100,9448730,0,2013-10-10 06:30:00,2013-10-10 07:00:00
2H131010-2A,S_2HB,S,29,20,9448812,1,2013-10-10 02:00:00,2013-10-10 04:00:00"""

# sample test data
html_test2 = """2H131010-1B,S_2HB,S,29.95,100,9448670,6,2013-10-10 00:00:00,2013-10-10 02:00:00
2H131010-2A,S_2HB,S,31,40,9448430,0,2013-10-10 02:00:00,2013-10-10 04:00:00
2H131010-2A,S_2HB,B,27,100,9448895,0,2013-10-10 02:00:00,2013-10-10 04:00:00
2H131010-2A,S_2HB,S,29,20,9448812,1,2013-10-10 02:00:00,2013-10-10 04:00:00
2H131010-3A,S_2HB,B,56.35,85,9448916,0,2013-10-10 06:00:00,2013-10-10 08:00:00
2H131010-3A,S_2HB,S,65,50,9448656,0,2013-10-10 06:00:00,2013-10-10 08:00:00"""

# sample test data
html_test3="""2H131010-2B,S_2HB,B,40,15,9448794,0,2013-10-10 04:00:00,2013-10-10 06:00:00
2H131010-2A,S_2HB,S,31,40,9448430,0,2013-10-10 02:00:00,2013-10-10 04:00:00
2H131010-2A,S_2HB,S,29,20,9448812,1,2013-10-10 02:00:00,2013-10-10 04:00:00"""

def insert_order(record, goes, load_time):
    field_str = ''
    for pos in range(len(record)):
        field_str += '"' + str(record[pos]) + '",'
    if goes == 0:
        field_str += '"Open-at-Start",'
    else:
        field_str += '"New",'   
    field_str += '"' + str(load_time) + '",'
    field_str += '""'
    c = CONN.cursor()
    c.execute("INSERT INTO orders VALUES (" + field_str + ")")
    CONN.commit()

def close_order(order, goes, load_time):
    sql = "UPDATE orders SET close = '" + str(load_time) + "' WHERE order_id = '" + str(order[5]) + "' AND ver_num = '" + str(order[6]) + "'"
    c = CONN.cursor()
    c.execute(sql)
    CONN.commit()

# to save space, doesn't record each load from API, instead it assigns open and closed times to every offer    
def compare_loads(previous, current, goes, load_time):
    for order in current:
        if order not in previous:
            insert_order(order, goes, load_time)
    if goes != 0:
        for order in previous:
            if order not in current:
                close_order(order, goes, load_time)
            
def convert_datetime_to_seconds(datetime):
    return time.mktime(time.strptime(datetime, "%Y-%m-%d %H:%M:%S"))

# calculates all possible matching combinations 
def build_coverage(start, end, matching_sales, sell_orders, path, combinations):
    for sale in matching_sales:
        if sale in path:
            continue
        new_path = path[:]
        if sell_orders[sale][9] == start:
            new_path.append(sale)
            if sell_orders[sale][10] == end:
                combinations.append(new_path)
            else:
                combinations += build_coverage(sell_orders[sale][10], end, matching_sales, sell_orders, new_path, [])
    return combinations

# calculates potential profits for each valid combination    
def calculate_profits(coverage, buy_price, buy_volume, sell_orders):
    profits = []
    for combination in coverage:
        sale_volume = calculate_sale_volume(combination, buy_volume, sell_orders)
        sell_price = calculate_sell_price(combination, sell_orders)
        sell_cost = sale_volume * sell_price
        buy_payment = buy_price * sale_volume
        profit = buy_payment - sell_cost
        if profit > 0.0:
            profits.append((profit, combination))
    return profits

# calculates total number of units in transaction                
def calculate_sale_volume(combination, buy_volume, sell_orders):
    highest_volume = sell_orders[combination[0]][4]
    for sale in combination:
        if float(sell_orders[sale][4]) < highest_volume:
            highest_volume = float(sell_orders[sale][4])
    if highest_volume > buy_volume:
        highest_volume = buy_volume
    return highest_volume

# calculates total sale price in transaction
def calculate_sell_price(combination, sell_orders):
    sell_price = 0.0
    for sale in combination:
        sell_price += float(sell_orders[sale][3])
    return sell_price

# compare all sales orders with each buy order                     
def check_buy(buy, sell_orders):
    opportunity = False
    matching_sales = []
    # compare time periods
    for rowno in range(len(sell_orders)):
        if float(sell_orders[rowno][9]) >= float(buy[9]) and float(sell_orders[rowno][10]) <= float(buy[10]):
            matching_sales.append(rowno)
    # if time periods overlap - i.e. there's a possibility of a profit
    if matching_sales:
       coverage = build_coverage(buy[9], buy[10], matching_sales, sell_orders, [], [])
       profits = calculate_profits(coverage, float(buy[3]), float(buy[4]), sell_orders)
       # if profits display details and add them to database
       if profits:
           opportunity = True
           insert_time = time.time()
           c = CONN.cursor()
           print
           print buy
           db_buy = str(buy[0]) + ',' + str(buy[1]) + ',' + str(buy[2]) + ',' + str(buy[3]) + ',' + str(buy[4]) + ',' + str(buy[5]) + ',' + str(buy[6]) + ',' + str(buy[7]) + ',' + str(buy[8])  
           field_str = "'"+str(insert_time)+"','"+str(db_buy)+"'"
           c.execute("INSERT INTO opportunities VALUES (" + field_str + ")")
           for row in profits:
               print row[0]
               field_str =  "'"+str(insert_time)+"','"+str(row[0])+"'"
               c.execute("INSERT INTO opportunities VALUES (" + field_str + ")")
               for combination in row[1]:
                   print sell_orders[combination]
                   db_sell = str(sell_orders[combination][0]) + ',' + str(sell_orders[combination][1]) + ',' + str(sell_orders[combination][2]) + ',' + str(sell_orders[combination][3]) + ',' + str(sell_orders[combination][4]) + ',' + str(sell_orders[combination][5]) + ',' + str(sell_orders[combination][6]) + ',' + str(sell_orders[combination][7]) + ',' + str(sell_orders[combination][8]) 
                   field_str =  "'"+str(insert_time)+"','"+str(db_sell)+"'"
                   c.execute("INSERT INTO opportunities VALUES (" + field_str + ")")
       CONN.commit()
    return opportunity

def convert_prices(data):
    return float(data[3]) * ((float(data[10]) - float(data[9]))/3600)

# send SMS via Twilio
def send_sms(message):
    account_sid = "XXXX"
    auth_token = "XXXX"
    client = TwilioRestClient(account_sid, auth_token)
 
    message = client.sms.messages.create(to=MY_NUMBER, from_="+441277420131",
                                         body=message)
    print "Message sent: ", message.sid

# main program starts here
previous_load = []
previous_opportunity = False
for goes in range(NUMBER_OF_CHECKS):
    start_time = time.time()
    buy_orders = []
    sell_orders = []
    
    # always useful to have try/fail with long running urllib programs - lots of potential failures that aren't serious
    try:
        response = urllib2.urlopen('https://datacapture.apxgroup.com/?report=orders&user=XXXX&pass=XXXX&platform=UKPX')
        html = response.read()

        if html[0:4] != '#-16':         # check if refused data because next request made within 10secs
            current_load = []
            # format data
            lines = html.split('\n')
            for line in lines:
                if line[0] == '#':
                    continue
                data = line.split(',')
                current_load.append(data[:])
                data.append(convert_datetime_to_seconds(data[7]))
                data.append(convert_datetime_to_seconds(data[8]))
                data[3] = convert_prices(data)
                # seperate buy and sell orders
                if data[2] == 'B':
                    buy_orders.append(data)  
                elif data[2] == 'S':
                    sell_orders.append(data)                                                   
        else:
            print html
        
    except:
        e = sys.exc_info()
        print( "Error: %s" % str(e) )

# printing of returned data - useful for testing
##    print "***BUY***"
##    for pos in range(len(buy_orders)):
##        print pos, buy_orders[pos]
##
##    print "--------------------------------------------"
##
##    print "***SELL***"
##    for pos in range(len(sell_orders)):
##        print pos, sell_orders[pos]

    opportunity = False
    for buy in buy_orders:
        individual_opportunity = check_buy(buy, sell_orders)
        if individual_opportunity == True:
            opportunity = True

    # if opportunity status changes, send an SMS message
    if opportunity != previous_opportunity:
        send_sms('Opportunity: ' + str(opportunity))
        previous_opportunity = opportunity

    # build up and save all offer data
    compare_loads(previous_load, current_load, goes, start_time)
    previous_load = current_load
                        
    end_time = time.time()
    time_taken = end_time - start_time
    print goes, time_taken
    time_to_wait = 11 - time_taken
    if time_to_wait > 0:
        time.sleep(time_to_wait)

# end of program - close db connection
CONN.close()
