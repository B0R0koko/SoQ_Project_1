import pandas as pd
import numpy as np
import os

from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant
from statsmodels.tsa.stattools import adfuller

from itertools import combinations
from multiprocessing import Pool
from tqdm import tqdm
from typing import *


# To avoid memory error
CHUNK_SIZE = 1000


def run_cointegration_test(df_roll: pd.DataFrame, first_ticker: str, second_ticker: str) -> float:
    """apply Engle–Granger two-step method to check cointegration"""

    X = df_roll[f"close_{first_ticker}"].values
    X = add_constant(X)

    Y = df_roll[f"close_{second_ticker}"].values
    
    model = OLS(Y, X).fit()
    pvalue = adfuller(model.resid)[1]

    return pvalue


def find_cointegration(
        df_close: pd.DataFrame, first_ticker: str, second_ticker: str
) -> pd.DataFrame:
    """Find all time windows when pair was cointegrated"""

    # leave only rows when both tickers existed
    df_close = df_close[
        (df_close[f"close_{first_ticker}"].notna()) &
        (df_close[f"close_{second_ticker}"].notna())
    ].copy()

    df_close[f"close_{first_ticker}"] = df_close[f"close_{first_ticker}"].apply(np.log)
    df_close[f"close_{second_ticker}"] = df_close[f"close_{second_ticker}"].apply(np.log)

    if df_close.shape[0] <= 180 * 24:
        return pd.DataFrame()

    window_size = 24*180 # window size of essentially one month
    overlap = 24*60 # with overlap of half a week

    signals = []

    num_iterations = (df_close.shape[0] - window_size) // overlap + 1

    for i in range(num_iterations):

        start = i * overlap
        end = start + window_size
        df_roll = df_close.iloc[start:end]

        pval: float = run_cointegration_test(
            df_roll=df_roll, first_ticker=first_ticker, second_ticker=second_ticker
        )
        
        if pval <= 1e-6:
            signals.append({
                "start_coint": df_roll["time"].iloc[0],
                "end_coint": df_roll["time"].iloc[-1],
                "pval": pval
            })

        i += overlap
        
    df_signals = pd.DataFrame(signals)
    df_signals["pair"] = f"{first_ticker}-{second_ticker}"

    return df_signals


def run_multiprocess(
        df_close: pd.DataFrame, ticker_combinations: List[Tuple[str, str]], num_processes: int
    ) -> pd.DataFrame:
    """run find_cointegration in parallel processes"""
    
    df_signals = pd.DataFrame()

    with (
        tqdm(total=len(ticker_combinations), desc="Searching for cointegrations", leave=False) as pbar, 
        Pool(processes=num_processes) as pool
    ):
        results = []

        for combination in ticker_combinations:
            first_ticker, second_ticker = combination
            cols = ["time", f"close_{first_ticker}", f"close_{second_ticker}"]

            res = pool.apply_async(
                find_cointegration, args=(df_close[cols], first_ticker, second_ticker)
            )
            
            results.append(res)

        for result in results:
            df_signals = pd.concat([df_signals, result.get()])
            pbar.update(1)

    return df_signals


def main() -> int:

    """Use only top 100 tickers from cmc to find cointegrated pairs on Binance"""

    df_cmc = pd.read_csv("data/cmc_data.csv")
    df_cmc = df_cmc[df_cmc["cmcRank"] <= 100]

    cmc_top_100_tickers = [symbol + "USDT" for symbol in df_cmc["symbol"].unique()]
    tickers: List[str] = os.listdir("data/klines_usdt")

    tickers = list(set(cmc_top_100_tickers) & set(tickers))
    ticker_combinations = list(combinations(tickers, 2))
    
    np.random.shuffle(ticker_combinations)

    df_close = pd.read_csv("data/price_matrix.csv")
    df_close["time"] = pd.to_datetime(df_close["time"])

    num_iterations = len(ticker_combinations) // CHUNK_SIZE + 1

    for i in tqdm(range(num_iterations), desc=f"Running batches of size {CHUNK_SIZE}"):
        df_signals: pd.DataFrame = run_multiprocess(
            df_close=df_close, 
            ticker_combinations=ticker_combinations[i*(CHUNK_SIZE): (i+1)*CHUNK_SIZE], 
            num_processes=15
        )
        df_signals.to_csv(f"data/signals/batch_{i}.csv", index=False)

if __name__ == "__main__":
    main()



