import market_gainers_losers as gl
import market_stats as ms
import market_cap as mc


# -------------------------------------------------------
#  MARKET CAP
# -------------------------------------------------------
market_cap = mc.Cap()
data_cap = market_cap.get_data()
print(data_cap.head())
# -------------------------------------------------------

# -------------------------------------------------------
#  STATS
# -------------------------------------------------------
market_stats = ms.Stats()
data_stats = market_stats.get_data()
print(data_stats)
# -------------------------------------------------------

# -------------------------------------------------------
#  GAINERS LOSERS DATA
# -------------------------------------------------------
gainers_losers = gl.Gainers_Losers()
data_gl = gainers_losers.get_data()
print(data_gl.head())
# -------------------------------------------------------
