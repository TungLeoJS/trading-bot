from datetime import datetime

from vnstock_data import Market

market = Market()

print(market.equity("FPT").ohlcv(start="2026-07-14", end="2026-07-14", interval='1M'))