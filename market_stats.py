from datetime import datetime
import pandas as pd
from coinmarketcap import Market

class Stats(object):
    def __init__(self):
        self.__dataframe = None
        self. __insertion_time = datetime.now()
        self.refresh()


    def refresh(self):
        coinmarketcap = Market()
        dict = coinmarketcap.stats()
        rows = []
        for key, value in dict.items():
            rows.append((key, value, 0))

        data = pd.DataFrame(data=rows, columns=['key', 'value', 'index'])
        self.__dataframe = data.pivot(columns='key', values='value', index='index')
        self.__dataframe['insertion_time'] = self. __insertion_time

    def get_data(self):
        return self.__dataframe