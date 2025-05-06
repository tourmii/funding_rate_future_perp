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
        self.max_offset = 17000

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
            print(f"Starting fetch for {inst_name}")
            offset = 0
            total = 0

            while offset <= self.max_offset:
                payload = self.get_funding_rate_history(
                    instrument_id=inst_name,
                    limit=100,
                    offset=offset
                )

                results = payload.get("results", [])
                pagination = payload.get("pagination", {})
                if not results:
                    print(f"No more data for {inst_name} (total fetched: {total})")
                    break

                for entry in results:
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

                batch_count = len(results)
                total += batch_count
                print(f"Fetched {batch_count} entries (offset {offset}) for {inst_name}")

                # Advance offset
                offset += batch_count
                if batch_count < pagination.get("result_limit", 100) or offset > self.max_offset:
                    print(f"Stopping pagination for {inst_name} (offset reached {offset})")
                    break

                time.sleep(0.2)

            print(f"Finished {inst_name}: {total} total entries\n")
