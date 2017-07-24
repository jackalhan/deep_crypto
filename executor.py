import locale

import stock.coinmarketcap.gainers_losers as gl
import stock.coinmarketcap.stats as ms
import stock.coinmarketcap.cap as mc
import time
import _thread
import pandas as pd
import db.connector as connector

#locale.setlocale( locale.LC_ALL, '' )
#locale.currency( 188518982.18, grouping=True )

#SOURCES
#1 bitfinex
#2,poloniex
#3,coinmarketcap

# -------------------------------------------------------
#  MARKET CAP
# -------------------------------------------------------
import stock.coinmarketcap.markets as market
from utility.config_parser import get_config

# market_cap = mc.Cap()
# data_cap = market_cap.get_data()
# print(data_cap.head())
# # -------------------------------------------------------
#
# # -------------------------------------------------------
# #  STATS
# # -------------------------------------------------------
# market_stats = ms.Stats()
# data_stats = market_stats.get_data()
# print(data_stats)
# # -------------------------------------------------------
#
# # -------------------------------------------------------
# #  GAINERS LOSERS DATA
# # -------------------------------------------------------
# gainers_losers = gl.Gainers_Losers()
# data_gl = gainers_losers.get_data()
# print(data_gl.head())

def read_currecies_from_db():
    socket_generic_arguments_dict = get_config(config_type='stock', section='generic-arguments')
    currency_update_interval = eval(socket_generic_arguments_dict['currency_update_interval'])

    while True:
        con = connector.create().cursor()
        market_cap = mc.Cap()
        # currency and 24-h volume as USD
        currencies_from_marketcap = market_cap.get_data()[['symbol', 'id', 'Insertion_Time']]


        # filter currencies based on their volume value. take currencies greater than 500.000
        #threshold_24h_volume = 500000
        #currencies_from_marketcap = currencies_from_marketcap[currencies_from_marketcap['24h_volume_usd'] >= threshold_24h_volume ]

        # con.execute("Truncate table currency ")
        # print('Currency table is truncated')
        # for _index, _row in currencies_from_marketcap.iterrows():
        #     values = "'" +_row['symbol']+ "','" +str(_row['Insertion_Time'])+ "',3," +str(_row['24h_volume_usd'])
        #     con.execute("INSERT INTO currency(currency, inserted_time, source_id, 24hint_volume) VALUES (" +values+ ")")
        # print('New currencies are inserted to currecny table')
        #currencies_from_marketcap.to_sql(name='currency', con=engine, if_exists='replace', flavor='mysql')


        #connector.disconnect(con)

        time.sleep(currency_update_interval)


# -------------------------------------------------------
# Define a function for the thread
def print_time( threadName, delay):
   count = 0
   while count < 5:
      time.sleep(delay)
      count += 1
      print ("%s: %s" % ( threadName, time.ctime(time.time()) ))

if __name__ == '__main__':
    # Create two threads as follows
    #try:
        #_thread.start_new_thread(read_currecies_from_db(), ())
        #read_currecies_from_db()

        mar = market.Markets("bitcoin")
        #mar.
        #_thread.start_new_thread(print_time)
    # except:
    #     print("Error: unable to start thread")
    #
    # while 1:
    #     pass



