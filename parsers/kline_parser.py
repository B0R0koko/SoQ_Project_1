from scrapy.utils.project import get_project_settings
from scrapy.exceptions import IgnoreRequest
from urllib.parse import urljoin, urlencode

from typing import *

import scrapy
import pandas as pd
import json


KLINE_ENDPOINT = "https://data.binance.vision/data/spot/monthly/klines/"
BINANCE_ENDPOINT = "https://api.binance.com/api/v3/klines"


class KlineSpider(scrapy.Spider):
    name = "kline_parser"

    custom_settings = {
        "ITEM_PIPELINES": {
            "parsers.pipes.parquet_pipe.ParquetPipeline": 1,
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.settings = get_project_settings()
        self.tickers: List[Dict[str, str]] = self.load_config()

    def load_config(self) -> List[Dict[str, str]]:
        with open(self.settings.get("CONFIG_PATH"), "rb") as file:
            return json.load(file)

    @staticmethod
    def gen_time_range(start_date: str, end_date: str) -> List[pd.Timestamp]:
        """Generates timestamps with hourly delta between two timestamps"""
        ts_range = pd.date_range(
            start=pd.Timestamp(start_date),
            end=pd.Timestamp(end_date),
            freq="MS",
            inclusive="both",
            normalize=True,
        ).tolist()
        return ts_range

    def start_requests(self) -> scrapy.Request:
        for ticker_cfg in self.tickers:
            ticker = ticker_cfg["ticker"]
            # Start with checking if this trading pair existed on Binance
            url = f'{BINANCE_ENDPOINT}?{urlencode({"symbol": f"{ticker}", "interval": "1h"})}'
            yield scrapy.Request(
                url=url, callback=self.query_data, meta={"config": ticker_cfg}
            )

    def query_data(self, response) -> scrapy.Request:
        ticker, start_date, end_date = response.meta["config"].values()

        # If either symbol doesn't exist on Binance or data already has been downloaded, them cancel downloading
        if response.status != 200:
            raise IgnoreRequest()

        ts_list: List[pd.Timestamp] = self.gen_time_range(
           start_date=start_date, end_date=end_date
        )

        for date in ts_list:

            year, month = date.year, str(date.month).zfill(2)
            file_path = f"{ticker}/1h/{ticker}-1h-{year}-{month}.zip"

            yield scrapy.Request( 
                url=urljoin(KLINE_ENDPOINT, file_path), 
                callback=self.write_data,
                meta={"ticker": ticker, "slug": f"{ticker}-{year}-{month}"},
            )

    def write_data(self, response) -> Dict[str, Any]:
        """if you want to write to zip files change to another pipeline in settings.py"""
        ticker, slug = response.meta["ticker"], response.meta["slug"]

        yield {"ticker": ticker, "slug": slug, "data": response.body}
