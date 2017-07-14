from datetime import datetime
import pandas as pd

class Cap(object):
    def __init__(self, convert='USD', limit=100):
        self.__dataframe = None
        self.__convert = convert.upper()
        self.__limit = str(limit)
        self.refresh()

    def refresh(self):
        self.__dataframe = pd.read_json(
            'https://api.coinmarketcap.com/v1/ticker/?convert=' + self.__limit + '&limit=' + self.__limit)
        self.__dataframe['Insertion_Time'] = datetime.now()

    def get_data(self):
        return self.__dataframe
