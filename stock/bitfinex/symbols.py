import requests
import pandas as pd

from utility.config_parser import get_config

bitfinex_stock_dict = get_config(config_type='stock', section='bitfinex')
symbols_with_details_api = bitfinex_stock_dict['symbols_with_details_api']
symbols_with_details_data = pd.read_json(symbols_with_details_api)




print(xx.head())