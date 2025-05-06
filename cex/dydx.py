import requests
import pymongo
import time
from datetime import datetime
from cli_scheduler import SchedulerJob


class DydxFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        # dYdX v1 markets to fetch
        self.product_mapping = {
            "BTC-USD": "BTC",
            "ETH-USD": "ETH",
        }
        self.base_url = "https://api.dydx.exchange/v1/historical-funding-rates"
        self.page_size = 100

    def get_history(self, market: str, starting_before: str = None):
        params = {
            "markets": market,
            "limit": str(self.page_size),
        }
        if starting_before:
            params["startingBefore"] = starting_before

        resp = requests.get(self.base_url, params=params)
        resp.raise_for_status()
        return resp.json()

    def fetch_data(self):
        for market, symbol in self.product_mapping.items():
            print(f"Fetching dYdX funding for {market}")
            starting_before = None
            total = 0

            while True:
                payload = self.get_history(market, starting_before)
                # payload keyed by market name
                history = payload.get(market, {}).get("history", [])
                if not history:
                    print(f"No more data for {market}")
                    break

                for entry in history:
                    # parse ISO timestamp to ms
                    dt = datetime.strptime(entry["effectiveAt"], "%Y-%m-%dT%H:%M:%SZ")
                    ts_ms = int(dt.timestamp() * 1000)
                    rec = {
                        "_id": f"dydx_{symbol}_{ts_ms}",
                        "symbol": symbol,
                        "exchange": "dydx",
                        "timestamp": ts_ms // 1000,
                        "funding_rate_8hr": float(entry["fundingRate8Hr"])
                    }
                    # upsert to avoid duplicates
                    self.collection.update_one(
                        {"_id": rec["_id"]},
                        {"$setOnInsert": rec},
                        upsert=True,
                    )

                batch = len(history)
                total += batch
                print(f"  → Fetched {batch} entries (total {total})")

                # next page
                starting_before = history[-1]["effectiveAt"]
                time.sleep(0.2)

            print(f"Finished {market}: {total} records\n")

