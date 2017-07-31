from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests

class Markets(object):
    def __init__(self, currency_id_list):
        self.__lxml = 'lxml'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['Currency', 'Market', 'Pair', 'Volume_BTC', 'Volume_Native', 'Volume_USD','Price_BTC', 'Price_Native', 'Price_USD', 'Volume_Percent', 'Insertion_Time']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__replacement_word = "#currency_id#"
        self.__currency_id_list = currency_id_list
        self.__url = "https://coinmarketcap.com/currencies/"+self.__replacement_word+"/#markets"
        self.refresh()

    def refresh(self):
        self.__insertion_time = datetime.now()
        len_currency_id_list = len(self.__currency_id_list)
        global_index = 0
        old_percent = 0
        for _ in self.__currency_id_list:
            _currency_id = _
            self.__url = self.__url.replace(self.__replacement_word, _currency_id)
            self.__page = requests.get(self.__url)
            self.__soup = BeautifulSoup(self.__page.content, self.__parser)
            self.parse_data(_currency_id)
            print(10 * '*')
            print(_currency_id, 'is collected' )
            print(10 * '*')
            percent = round(((global_index + 1) / len_currency_id_list) * 100)
            if old_percent != percent:
                print(str(percent), '% of data processed')
                old_percent = percent

            global_index += 1

    def parse_data(self, currency_id):


        first_part = str(self.__soup.find_all("table", id="markets-table")[0])
        soup_first_part= BeautifulSoup(first_part, self.__lxml)
        second_part = str(soup_first_part.find_all("tbody")[0])
        soup_second_part = BeautifulSoup(second_part, self.__lxml)
        third_part = soup_second_part.find_all("tr")
        _star = '*'
        for _ in third_part:
            _childsoup = BeautifulSoup(str(_), self.__lxml)
            market_part = _childsoup.find_all("td")

            market_name_part = str(market_part[1])
            soup_market_name_part = BeautifulSoup(market_name_part, self.__lxml)
            market_name = soup_market_name_part.a.string

            pair_part = str(market_part[2])
            soup_pair_part = BeautifulSoup(pair_part, self.__lxml)
            pair_symbol = soup_pair_part.a.string

            volume_part = str(market_part[3])
            soup_volume_part = BeautifulSoup(volume_part, self.__lxml)
            volume_btc = soup_volume_part.span['data-btc']
            volume_native = soup_volume_part.span['data-native']
            volume_usd = soup_volume_part.span['data-usd']
            volume_string = soup_volume_part.span.string.strip()

            price_part = str(market_part[4])
            soup_price_part = BeautifulSoup(price_part, self.__lxml)
            price_btc = soup_price_part.span['data-btc']
            price_native = soup_price_part.span['data-native']
            price_usd = soup_price_part.span['data-usd']
            price_string = soup_price_part.span.string.strip()

            volume_percent_part = str(market_part[5])
            soup_volume_percent_part = BeautifulSoup(volume_percent_part, self.__lxml)
            volume_percent = soup_volume_percent_part.td.string
            volume_percent = volume_percent.replace('%','')

            if volume_string.startswith(_star) or price_string.startswith(_star):
                continue

            self.__rows.append((currency_id, market_name, pair_symbol, volume_btc, volume_native, volume_usd, price_btc, price_native, price_usd, volume_percent, self.__insertion_time, ))

        return self.__rows

    def get_data(self):
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        return self.__dataframe
