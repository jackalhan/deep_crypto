from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests


class Exchange_Coins(object):
    def __init__(self, exchange_list):
        self.__lxml = 'lxml'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['Exchange_Id', 'Coin_Id', 'Coin_Name', 'Pair', 'Coin_1', 'Coin_2', 'Volume_USD', 'Volume_BTC',
                                    'Volume_Native', 'Price_USD', 'Price_BTC', 'Price_Native', 'Insertion_Time']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__replacement_word = "#exchange_id#"
        self.__exchange_list = exchange_list
        self.__url = "https://coinmarketcap.com/exchanges/" + self.__replacement_word

    def refresh(self):
        self.__insertion_time = datetime.now()
        len_exchange_list = len(self.__exchange_list)
        global_index = 0
        old_percent = 0
        for _ in self.__exchange_list:
            _exchange_id = _[0]
            self.__url = self.__url.replace(self.__replacement_word, _exchange_id)
            self.__page = requests.get(self.__url)
            self.__soup = BeautifulSoup(self.__page.content, self.__parser)
            self.parse_data(_exchange_id)
            print(10 * '*')
            print(_exchange_id, 'is collected')
            print(10 * '*')
            percent = round(((global_index + 1) / len_exchange_list) * 100)
            if old_percent != percent:
                print(str(percent), '% of data processed')
                old_percent = percent

            global_index += 1
        return self.__rows

    def parse_data(self, exchange_id):
        table_part_as_str = str(self.__soup.find_all("table", class_="table no-border table-condensed")[0])
        table_soup = BeautifulSoup(str(table_part_as_str), self.__lxml)
        trs_part = table_soup.find_all('tr')
        counter = 0
        for _ in trs_part:
            if counter > 0:
                _childsoup = BeautifulSoup(str(_), self.__lxml)
                td_part = _childsoup.find_all('td')

                a_str = str(td_part[1])
                a_part = BeautifulSoup(a_str, self.__lxml)
                href = a_part.a['href']
                coin_id = [_item for _item in href.split('/') if _item != ''][1]
                coin_name = a_part.a.string.strip()

                a2_str = str(td_part[2])
                a2_str_part = BeautifulSoup(a2_str,self.__lxml)
                pair = a2_str_part.a.string
                pair_splitted = pair.split('/')
                coin1_short_id = pair_splitted[0]
                coin2_short_id = pair_splitted[1]

                volume_str = str(td_part[3])
                volume_part = BeautifulSoup(volume_str, self.__lxml)
                volume_btc = volume_part.td['data-btc']
                volume_native = volume_part.td['data-native']
                volume_usd = volume_part.td['data-usd']
                volume_string = volume_part.td.string.strip()

                price_str = str(td_part[4])
                price_part = BeautifulSoup(price_str, self.__lxml)
                price_btc = price_part.td['data-btc']
                price_native = price_part.td['data-native']
                price_usd = price_part.td['data-usd']
                price_string = price_part.td.string.strip()

                self.__rows.append((exchange_id, coin_id, coin_name, pair, coin1_short_id, coin2_short_id, volume_usd, volume_btc,
                                    volume_native, price_usd, price_btc, price_native, self.__insertion_time,))
            counter += 1
        return self.__rows

    def get_data(self):
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        return self.__dataframe
