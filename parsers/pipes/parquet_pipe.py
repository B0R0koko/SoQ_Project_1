import zipfile
import io
import os

import pandas as pd


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


class ParquetPipeline:

    """Store collected data as folders with parquet files"""

    def __init__(self, output_dir: str):
        self.output_dir: str = output_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            output_dir=crawler.settings.get("OUTPUT_DIR"),
        )

    def process_item(self, response, spider):
        data, ticker, slug = response["data"], response["ticker"], response["slug"]

        with zipfile.ZipFile(io.BytesIO(data), "r") as zip_ref:
            for file_name in zip_ref.namelist():
                file_content = io.StringIO(zip_ref.read(file_name).decode("utf-8"))
                df: pd.DataFrame = pd.read_csv(
                    file_content, header=None, names=cols
                )

        # create output_dir/ticker
        ticker_dir = os.path.join(self.output_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)

        output_path = os.path.join(ticker_dir, f"{slug}.parquet")

        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

        df.to_parquet(output_path, compression="gzip", index=False)