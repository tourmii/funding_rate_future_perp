import requests
import pymongo
import time
import urllib.parse
from cli_scheduler import SchedulerJob


class MerkleFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        # Map Move pair types to simpler symbols
        self.product_mapping = {
            'BTC_USD': 'BTC',
            'ETH_USD': 'ETH',
        }
        self.base_url = "https://node.prod.merkle.trade/v1/accounts"
        self.account = "0x5ae6789dd2fec1a9ec9cccfb3acaf12e93d432f0a3a42c92fe1a9d490b7bbc06"

    def get_pair_state(self, pair: str) -> dict:
        """
        Fetches the on-chain PairState resource for the given Move pair type.
        """
        struct = (
            f"{self.account}::trading::PairState<"
            f"{self.account}::pair_types::{pair},"
            f"{self.account}::fa_box::W_USDC>"
        )
        encoded = urllib.parse.quote(struct, safe='')
        url = f"{self.base_url}/{self.account}/resource/{encoded}"
        resp = requests.get(url, headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json().get("data", {})

    def fetch_data(self):
        """
        Fetch and store the latest funding rates for all mapped pairs.
        """
        for full_pair, symbol in self.product_mapping.items():
            try:
                print(f"Fetching PairState for {full_pair}")
                data = self.get_pair_state(full_pair)

                raw_rate = int(data.get("funding_rate", 0))
                positive = data.get("funding_rate_positive", False)
                signed_rate = (raw_rate / 1_000_000_000) if positive else -(raw_rate / 1_000_000_000)

                ts = int(data.get("last_accrue_timestamp", time.time()))

                record = {
                    "_id": f"merkle_{symbol}_{ts}",
                    "timestamp": ts,
                    "symbol": symbol,
                    "exchange": "merkle_trade",
                    "funding_rate": signed_rate,
                    "raw_funding_rate": raw_rate,
                }

                self.collection.insert_one(record)
                print(f"Inserted funding_rate for {symbol}: {signed_rate:.9f}")

            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error for {full_pair}: {http_err}")
            except pymongo.errors.DuplicateKeyError:
                print(f"Record for {symbol} at {ts} already exists, skipping.")
            except Exception as e:
                print(f"Error fetching data for {full_pair}: {e}")

            time.sleep(1)

if __name__ == "__main__":
    # Example usage
    mongodb_uri = "mongodb://localhost:27017/"
    database_name = "funding_rates"
    collection_name = "funding_rates"

    fetcher = MerkleFundingRateFetcher(mongodb_uri, database_name, collection_name)
    fetcher.fetch_data()