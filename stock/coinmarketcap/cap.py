from datetime import datetime
import pandas as pd

class Cap(object):
    def __init__(self, convert='USD', limit=1000):
        self.__dataframe = None
        self.__convert = convert.upper()
        self.__limit = str(limit)
        self.refresh()

    def refresh(self):
        print(10 * '-', 'Market Caps are getting parsed', 10 * '-')
        self.__dataframe = pd.read_json(
            'https://api.coinmarketcap.com/v1/ticker/?convert=' + self.__convert  + '&limit=' + self.__limit)
        self.__dataframe['Insertion_Time'] = datetime.now()
        self.__dataframe['rank'] = self.__dataframe['rank'].astype(int)
        self.__dataframe['price_usd'] = self.__dataframe.price_usd.astype(float)
        self.__dataframe['price_btc'] = self.__dataframe.price_btc.astype(float)
        self.__dataframe['24h_volume_usd'] = self.__dataframe['24h_volume_usd'].astype(float)
        self.__dataframe['market_cap_usd'] = self.__dataframe.market_cap_usd.astype(float)
        self.__dataframe['available_supply'] = self.__dataframe.available_supply.astype(float)
        self.__dataframe['total_supply'] = self.__dataframe.total_supply.astype(float)
        self.__dataframe['percent_change_1h'] = self.__dataframe.percent_change_1h.astype(float)
        self.__dataframe['percent_change_24h'] = self.__dataframe.percent_change_24h.astype(float)
        self.__dataframe['percent_change_7d'] = self.__dataframe.percent_change_7d.astype(float)

        print(10 * '-', 'Market Caps are done', 10 * '-')
    def get_data(self):
        return self.__dataframe
