import requests
import pymongo
import time
from cli_scheduler import SchedulerJob


class BitgetFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        # Map Bitget symbols to your simplified symbols
        self.product_mapping = {
            "BTCUSDT": "BTC",
            "ETHUSDT": "ETH"
        }
        self.base_url = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
        # you can raise this up to 100
        self.page_size = 100

    def get_history(self, symbol: str, product_type: str, page_no: int):
        params = {
            "symbol": symbol,
            "productType": product_type,
            "pageSize": self.page_size,
            "pageNo": page_no
        }
        resp = requests.get(self.base_url, params=params)
        resp.raise_for_status()
        return resp.json()

    def fetch_data(self):
        for bitget_symbol, mapped_symbol in self.product_mapping.items():
            print(f"Fetching Bitget funding rates for {bitget_symbol}")
            page_no = 1
            total = 0

            while True:
                payload = self.get_history(
                    symbol=bitget_symbol,
                    product_type="USDT-FUTURES",
                    page_no=page_no
                )

                data = payload.get("data", [])
                if not data:
                    print(f"No more data on page {page_no} for {bitget_symbol}")
                    break

                for entry in data:
                    ts = int(entry["fundingTime"])  
                    rec = {
                        "_id": f"bitget_{mapped_symbol}_{ts}",
                        "symbol": mapped_symbol,
                        "exchange": "bitget",
                        "timestamp": ts // 1000,
                        "funding_rate": float(entry["fundingRate"])
                    }
                    # upsert to avoid duplicates
                    self.collection.update_one(
                        {"_id": rec["_id"]},
                        {"$setOnInsert": rec},
                        upsert=True
                    )

                batch = len(data)
                total += batch
                print(f"Page {page_no}: fetched {batch} entries for {bitget_symbol}")

                # if fewer than page_size returned, this was the last page
                if batch < self.page_size:
                    break

                page_no += 1
                time.sleep(0.2)

            print(f"Finished {bitget_symbol}: {total} total entries\n")

