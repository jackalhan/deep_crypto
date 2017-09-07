from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import requests
from re import sub
from decimal import Decimal


class Markets_Coins(object):
    def __init__(self, market_id_list):
        self.__lxml = 'lxml'
        self.__insertion_time = None
        self.__dataframe = None
        self.__dataframe_columns = ['Market', 'Currency', 'Pair', 'Coin_1', 'Coin_2', 'Volume_BTC', 'Volume_Native',
                                    'Volume_USD', 'Price_BTC', 'Price_Native', 'Price_USD', 'Volume_Percent',
                                    'Insertion_Time']
        self.__page = None
        self.__parser = 'html.parser'
        self.__soup = None
        self.__rows = []
        self.__market_summary_rows = []
        self.__dataframe_coin_summary = None
        self.__dataframe_coin_summary_columns = ['Market', 'All_Cap_USD', 'All_Cap_BTC','Insertion_Time']
        self.__replacement_word = "#market_id#"
        self.__market_id_list = market_id_list
        self.__baseurl = "https://coinmarketcap.com"
        self.__url = self.__baseurl + '/exchanges/' + self.__replacement_word

    def refresh(self):
        self.__insertion_time = datetime.now()
        len_currency_id_list = len(self.__market_id_list)
        global_index = 0
        old_percent = 0

        for _ in self.__market_id_list:
            _market_id = _
            url = self.__url.replace(self.__replacement_word, _market_id)
            page = requests.get(url)
            self.__soup = BeautifulSoup(page.content, self.__parser)
            self.parse_summary_from_market(_market_id)
            self.parse_coins_from_market(_market_id)
            print(10 * '*')
            print(_market_id, 'is collected')
            print(10 * '*')
            percent = round(((global_index + 1) / len_currency_id_list) * 100)
            if old_percent != percent:
                print(str(percent), '% of data processed')
                old_percent = percent


        return self.__rows, self.__market_summary_rows

    def parse_summary_from_market(self, market_id):

        header_part = self.__soup.find_all("div", {"class": "text-left"})

        try:
            market_cap_amounts = header_part[0].text.strip().split('\n')
            market_cap_usd = Decimal(sub(r'[^\d.]', '', market_cap_amounts[0])).__float__()
            market_cap_btc = Decimal(sub(r'[^\d.]', '', market_cap_amounts[2])).__float__()
        except Exception as e:
            market_cap_usd = 0
            market_cap_btc = 0

        self.__market_summary_rows.append((
            market_id, market_cap_usd, market_cap_btc,self.__insertion_time))

        return self.__market_summary_rows

    def parse_coins_from_market(self, market_id):

        first_part = str(self.__soup.find_all("table", {"class":"table no-border table-condensed"})[0])
        soup_part = BeautifulSoup(first_part, self.__lxml)
        third_part = soup_part.find_all("tr")
        _star = '*'
        for _ in third_part:
            _childsoup = BeautifulSoup(str(_), self.__lxml)
            market_part = _childsoup.find_all("td")

            market_name_part = str(market_part[1])
            soup_market_name_part = BeautifulSoup(market_name_part, self.__lxml)
            coin_post_url = soup_market_name_part.a['href']
            coin_id = [_item for _item in coin_post_url.split('/') if _item != ''][1]

            pair_part = str(market_part[2])
            soup_pair_part = BeautifulSoup(pair_part, self.__lxml)
            pair_symbol = soup_pair_part.a.string
            pair_splitted = pair_symbol.split('/')
            coin1 = pair_splitted[0]
            coin2 = pair_splitted[1]

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
            volume_percent = volume_percent.replace('%', '')

            if volume_string.startswith(_star) or price_string.startswith(_star):
                continue

            self.__rows.append((market_id,  coin_id,pair_symbol, coin1, coin2, volume_btc, volume_native,
                                volume_usd, price_btc, price_native, price_usd, volume_percent, self.__insertion_time))

        return self.__rows

    def get_data(self):
        self.refresh()
        self.__dataframe = pd.DataFrame(data=self.__rows, columns=self.__dataframe_columns)
        self.__dataframe_coin_summary = pd.DataFrame(data=self.__market_summary_rows,
                                                     columns=self.__dataframe_coin_summary_columns)
        return self.__dataframe, self.__dataframe_coin_summary
