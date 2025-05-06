import pymongo
import time
import requests
from cli_scheduler import SchedulerJob


class BinanceFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.product_mapping = {
            "BTCUSDT": "BTC",
            "ETHUSDT": "ETH"
        }
        self.base_url = "https://fapi.binance.com/fapi/v1/fundingRate"

    def get_funding_rate_history(self, symbol, end_time, limit=1000):
        params = {
            "symbol": symbol,
            "limit": limit,
            "endTime": end_time
        }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_data(self):
        for binance_symbol, mapped_symbol in self.product_mapping.items():
            print(f"Fetching funding rate for {binance_symbol}")
            end_time = int(time.time() * 1000)  # current time in ms
            total_processed = 0

            while True:
                try:
                    funding_data = self.get_funding_rate_history(
                        symbol=binance_symbol,
                        end_time=end_time,
                        limit=1000  # Binance max limit per request
                    )

                    if not funding_data:
                        print(f"No more data for {binance_symbol}")
                        break

                    for entry in funding_data:
                        ts = int(entry["fundingTime"])
                        record = {
                            "_id": f"binance_{mapped_symbol}_{ts//1000}",
                            "timestamp": ts // 1000,
                            "symbol": mapped_symbol,
                            "exchange": "binance",
                            "funding_rate": float(entry["fundingRate"])
                        }
                        self.collection.insert_one(record)

                    total_processed += len(funding_data)
                    print(f"Fetched {len(funding_data)} entries for {binance_symbol}")

                    # Prepare for next page (older data)
                    oldest_ts = int(funding_data[-1]["fundingTime"])
                    end_time = oldest_ts - 1

                    time.sleep(0.3)

                except Exception as e:
                    print(f"Error fetching data for {binance_symbol}: {e}")
                    break

            print(f"Finished {binance_symbol}, total entries: {total_processed}")
