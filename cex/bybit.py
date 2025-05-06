import pymongo
import time
from pybit.unified_trading import HTTP
from cli_scheduler import SchedulerJob


class BybitFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.session = HTTP(testnet=False)
        self.product_mapping = {
            "BTCPERP": "BTC"
            # "ETHPERP": "ETH"
        }

    def fetch_data(self):
        for symbol, mapped_symbol in self.product_mapping.items():
            print(f"Fetching funding rate for {symbol}")
            cursor_end = int(time.time() * 1000)  # current time in ms
            total_processed = 0

            while True:
                try:
                    resp = self.session.get_funding_rate_history(
                        category="linear",
                        symbol=symbol,
                        endTime=cursor_end,
                        limit=200
                    )

                    block = resp["result"]["list"]
                    if not block:
                        print(f"No more data for {symbol}")
                        break

                    for entry in block:
                        ts = int(entry["fundingRateTimestamp"])
                        record = {
                            "_id": f"bybit_{mapped_symbol}_{ts//1000}",
                            "timestamp": ts // 1000,
                            "symbol": mapped_symbol,
                            "exchange": "bybit",
                            "funding_rate": float(entry["fundingRate"])
                        }
                        self.collection.insert_one(record)

                    total_processed += len(block)
                    print(f"Fetched {len(block)} entries for {symbol}")

                    # Prepare for next batch
                    oldest_ts = int(block[-1]["fundingRateTimestamp"])
                    cursor_end = oldest_ts - 1

                    time.sleep(0.3)

                except Exception as e:
                    print(f"Error fetching data for {symbol}: {e}")
                    break

            print(f"Finished {symbol}, total entries: {total_processed}")
