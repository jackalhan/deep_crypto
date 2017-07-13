from coinmarketcap import Market
import pandas as pd
from lxml import html
import requests
from bs4 import BeautifulSoup

# Limits
# Please limit requests to no more than 10 per minute.
# Endpoints update every 5 minutes.


coinmarketcap = Market()

data_list = pd.read_json('https://api.coinmarketcap.com/v1/ticker/?convert=USD&limit=1000')
print(data_list.head())

#stats
print(coinmarketcap.stats())


page = requests.get('https://coinmarketcap.com/gainers-losers/')

tree = html.fromstring(page.content)
#biggest_gainers = tree.xpath('//div[@id="gainers-1h"]/text()') #<div id="gainers-1h" class="tab-pane ">
#print(biggest_gainers)



soup = BeautifulSoup(page.content, 'html.parser')


div_gainer_part_1h = str(soup.find_all("div", id="gainers-1h", class_="tab-pane")[0])
soup_gainer_part_1h = BeautifulSoup(div_gainer_part_1h)
tr_gainer_part_1h = soup_gainer_part_1h.find_all("tr")
for _ in tr_gainer_part_1h:
    _childsoup = BeautifulSoup(str(_))
    try:
        id = _childsoup.tr['id']
    except:
        continue
    print(_)
print(soup_gainer_part_1h.prettify())
#html = html.find_all('tbody')
#print(soup_gainer.prettify())