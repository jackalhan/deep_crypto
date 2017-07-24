from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests


class Markets(object):
    def __init__(self, currency_id):
        self.__lxml = 'lxml'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['Source', 'Pair', 'Insertion_Time', 'Volume_24H_USD', 'Volume_24H_Percent', 'Price']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__replacement_word = "#currency_id#"
        self.__url = "https://coinmarketcap.com/currencies/"+self.__replacement_word+"/#markets"
        self.__url = self.__url.replace(self.__replacement_word, currency_id)
        self.refresh()

    def refresh(self):
        self.__page = requests.get(self.__url)
        self.__soup = BeautifulSoup(self.__page.content, self.__parser)
        self.__insertion_time = datetime.now()
        self.parse_data()

    def parse_data(self):


        first_part = str(self.__soup.find_all("table", id="markets-table")[0])
        soup_first_part= BeautifulSoup(first_part, self.__lxml)
        second_part = str(soup_first_part.find_all("tbody")[0])
        soup_second_part = BeautifulSoup(second_part, self.__lxml)
        third_part = soup_second_part.find_all("tr")
        currency_order = 0
        for _ in third_part:
            _childsoup = BeautifulSoup(str(_), self.__lxml)
            market_part = _childsoup.find_all("td")

            market_name_part = str(market_part[1])
            soup_market_name_part = BeautifulSoup(market_name_part, self.__lxml)
            currency_name = soup_market_name_part.a.string

            currency_symbol_td_str = str(_childsoup.find_all("td", class_="text-left")[0])
            currency_symbol_td_soup = BeautifulSoup(currency_symbol_td_str, self.__lxml)
            currency_symbol = currency_symbol_td_soup.string

            currency_volume_a_str = str(_childsoup.findAll("a", class_="volume")[0])
            currency_volume_a_soup = BeautifulSoup(currency_volume_a_str, self.__lxml)
            # currency_volume_btc = currency_volume_a_soup.a[self.__data_btc]
            # currency_volume_usd = currency_volume_a_soup.a[self.__data_usd]
            #
            # currency_price_a_str = str(_childsoup.findAll("a", class_="price")[0])
            # currency_price_a_soup = BeautifulSoup(currency_price_a_str, self.__lxml)
            # currency_price_btc = currency_price_a_soup.a[self.__data_btc]
            # currency_price_usd = currency_price_a_soup.a[self.__data_usd]
            #
            # _class_name = "no-wrap percent-" + time_interval + " " + change_type +" text-right"
            # currency_change_td_str = str(
            #     _childsoup.findAll("td", class_=_class_name)[0])
            # currency_change_td_soup = BeautifulSoup(currency_change_td_str, self.__lxml)
            # currency_change_btc = currency_change_td_soup.td[self.__data_btc]
            # currency_change_usd = currency_change_td_soup.td[self.__data_usd]
            #
            # currency_order += 1
            #
            # self.__rows.append((list_type, time_interval, self.__insertion_time, currency_order,
            #                     currency_name, currency_symbol, currency_volume_btc, currency_volume_usd,
            #                     currency_price_btc, currency_price_usd, currency_change_btc,
            #                     currency_change_usd))

    def get_data(self):
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        return self.__dataframe
