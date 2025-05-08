import requests
import pymongo
import time
from cli_scheduler import SchedulerJob


class CoinbaseFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        # Use the human-readable instrument IDs here:
        self.product_mapping = {
            "BTC-PERP": "BTC",
            "ETH-PERP": "ETH",
        }
        self.base_url = "https://api.international.coinbase.com/api/v1/instruments/{instrument_id}/funding"

    def get_funding_rate_history(self, instrument_id: str, limit: int = 100, offset: int = 0):
        url = self.base_url.format(instrument_id=instrument_id)
        params = {
            "result_limit": limit,
            "result_offset": offset
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def fetch_data(self):
        for inst_name, symbol in self.product_mapping.items():
            print(f"Fetching latest funding rate for {inst_name}")

            payload = self.get_funding_rate_history(
                instrument_id=inst_name,
                limit=1,
                offset=0
            )

            results = payload.get("results", [])
            if not results:
                print(f"No data returned for {inst_name}")
                continue

            entry = results[0]
            ts = int(time.mktime(time.strptime(entry["event_time"], "%Y-%m-%dT%H:%M:%SZ"))) * 1000
            rec = {
                "_id": f"coinbase_{symbol}_{ts}",
                "symbol": symbol,
                "exchange": "coinbase",
                "timestamp": ts // 1000,
                "funding_rate": float(entry["funding_rate"])
            }

            self.collection.update_one(
                {"_id": rec["_id"]},
                {"$setOnInsert": rec},
                upsert=True
            )

            print(f"Saved latest funding rate for {inst_name}: {rec['funding_rate']} at {entry['event_time']}")

