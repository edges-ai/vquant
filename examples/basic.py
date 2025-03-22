import sys
import os

# Add the project root directory to sys.path
project_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(project_root)

from vquant import Vquant, FactorNotFoundError, DataLoadError

import polars as pl
import matplotlib.pyplot as plt

# Initialize Vquant
BASE_URL = "https://storage.googleapis.com/edges-quant-data"
vquant = Vquant(market="stocks_vn", base_url=BASE_URL)

# Define assets
tickers = ["AAA", "ACB", "VNM"]

prices = vquant.get_ohlcv(tickers, cols=["open", "high", "low", "close", "volume"])
print(prices)