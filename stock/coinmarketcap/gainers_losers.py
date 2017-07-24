from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests


class Gainers_Losers(object):
    def __init__(self):
        self.__lxml = 'lxml'
        self.__data_btc = 'data-btc'
        self.__data_usd = 'data-usd'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['List_Type', 'Time_Interval', 'Insertion_Time', 'Order_Number', 'Currency_Name',
                                    'Currency_Symbol', 'Currency_Volume_BTC', 'Currency_Volume_USD',
                                    'Currency_Price_BTC', 'Currency_Price_USD', 'Currency_Percent_Change_BTC',
                                    'Currency_Percent_Change_USD']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__list_types = ['gainers', 'losers']
        self.__time_intervals = ['1h', '24h', '7d']
        self.refresh()

    def refresh(self):
        self.__page = requests.get('https://coinmarketcap.com/gainers-losers/')
        self.__soup = BeautifulSoup(self.__page.content, self.__parser)
        self.__insertion_time = datetime.now()
        self.parse_data()

    def parse_data(self):

        for _lt in self.__list_types:
            list_type = _lt
            for _ti in self.__time_intervals:
                time_interval = _ti

                if list_type == 'losers':
                    change_type = 'negative_change'
                else:
                    change_type = 'positive_change'
                part = list_type + '-' + time_interval
                div_part = str(self.__soup.find_all("div", id=part, class_="tab-pane")[0])
                soup_of_div_part = BeautifulSoup(div_part, self.__lxml)
                tr_part = soup_of_div_part.find_all("tr")
                currency_order = 0
                for _ in tr_part:
                    _childsoup = BeautifulSoup(str(_), self.__lxml)
                    try:
                        id = _childsoup.tr['id']
                    except:
                        continue
                    currency_name_td_str = str(_childsoup.find_all("td", class_="no-wrap currency-name")[0])
                    currency_name_td_soup = BeautifulSoup(currency_name_td_str, self.__lxml)
                    currency_name = currency_name_td_soup.a.string

                    currency_symbol_td_str = str(_childsoup.find_all("td", class_="text-left")[0])
                    currency_symbol_td_soup = BeautifulSoup(currency_symbol_td_str, self.__lxml)
                    currency_symbol = currency_symbol_td_soup.string

                    currency_volume_a_str = str(_childsoup.findAll("a", class_="volume")[0])
                    currency_volume_a_soup = BeautifulSoup(currency_volume_a_str, self.__lxml)
                    currency_volume_btc = currency_volume_a_soup.a[self.__data_btc]
                    currency_volume_usd = currency_volume_a_soup.a[self.__data_usd]

                    currency_price_a_str = str(_childsoup.findAll("a", class_="price")[0])
                    currency_price_a_soup = BeautifulSoup(currency_price_a_str, self.__lxml)
                    currency_price_btc = currency_price_a_soup.a[self.__data_btc]
                    currency_price_usd = currency_price_a_soup.a[self.__data_usd]

                    _class_name = "no-wrap percent-" + time_interval + " " + change_type +" text-right"
                    currency_change_td_str = str(
                        _childsoup.findAll("td", class_=_class_name)[0])
                    currency_change_td_soup = BeautifulSoup(currency_change_td_str, self.__lxml)
                    currency_change_btc = currency_change_td_soup.td[self.__data_btc]
                    currency_change_usd = currency_change_td_soup.td[self.__data_usd]

                    currency_order += 1

                    self.__rows.append((list_type, time_interval, self.__insertion_time, currency_order,
                                        currency_name, currency_symbol, currency_volume_btc, currency_volume_usd,
                                        currency_price_btc, currency_price_usd, currency_change_btc,
                                        currency_change_usd))

    def get_data(self):
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        return self.__dataframe
