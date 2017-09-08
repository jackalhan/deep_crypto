from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests


class Markets(object):
    def __init__(self):
        self.__lxml = 'lxml'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['Id', 'Name', 'Insertion_Time']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__url = "https://coinmarketcap.com/exchanges/volume/24-hour/all/"


    def parse_data(self):
        print(10 * '-', 'Markets are getting parsed', 10 * '-')
        self.__page = requests.get(self.__url)
        self.__soup = BeautifulSoup(self.__page.content, self.__parser)
        self.__insertion_time = datetime.now()
        exchange_table_part = self.__soup.find_all("h3", class_="volume-header")
        for _ in exchange_table_part:
            _childsoup = BeautifulSoup(str(_), self.__lxml)
            a_part = _childsoup.find_all('a')
            exchange_id = [_item for _item in a_part[0]['href'].split('/') if _item != ''][1]
            exchange_name = a_part[0].text
            self.__rows.append((exchange_id, exchange_name, self.__insertion_time,))
        print(10 * '-', 'Markets are getting parsed', 10 * '-')
        return self.__rows

    def get_data(self):
        self.parse_data()
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        return self.__dataframe
