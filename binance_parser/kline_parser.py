from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from urllib.parse import urljoin

from typing import *

import scrapy
import csv
import zipfile
import io
import pandas as pd


KLINE_ENDPOINT = "https://data.binance.vision/data/spot/monthly/klines/"


cols = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_vol",
    "num_trades",
    "taker_buy_base_vol",
    "taker_buy_quote_vol",
    "garb",
]


class KlineSpider(scrapy.Spider):
    name = "kline_parser"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.settings = get_project_settings()

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

    def start_requests(self) -> Iterable[Request]:
        start_date: str = self.settings.get("START_DATE")
        end_date: str = self.settings.get("END_DATE")
        symbol: str = self.settings.get("SYMBOL")

        for date in self.gen_time_range(start_date, end_date):
            slug = f"{symbol}/1h/{symbol}-1h-{date.year}-{str(date.month).zfill(2)}.zip"

            yield scrapy.Request(
                url=urljoin(KLINE_ENDPOINT, slug), callback=self.parse_kline_data
            )

    def parse_kline_data(self, response) -> List[str]:
        with zipfile.ZipFile(io.BytesIO(response.body), "r") as zip_ref:
            for file_name in zip_ref.namelist():
                file_content = io.StringIO(zip_ref.read(file_name).decode("utf-8"))
                reader = csv.reader(file_content)

        for row in reader:
            yield dict(zip(cols, row))
