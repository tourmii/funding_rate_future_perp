import requests
import pymongo
import time
from cli_scheduler import SchedulerJob


class OKXFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.product_mapping = {
            'BTC-USDT-SWAP': 'BTC',
            'ETH-USDT-SWAP': 'ETH',
        }

    def get_funding_rate_history(self, inst_id="BTC-USDT-SWAP", before=None, after=None):
        base_url = "https://www.okx.com/api/v5/public/funding-rate-history"
        params = {
            "instId": inst_id,
        }
        if before:
            params["before"] = before
        if after:
            params["after"] = after

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def fetch_data(self):
        for inst_id, symbol in self.product_mapping.items():
            total_processed = 0
            after = None

            for _ in range(10):  
                try:
                    print(f"Fetching data for {inst_id} after {after}")
                    funding_data = self.get_funding_rate_history(inst_id=inst_id, after=after)
                    
                    if not funding_data or "data" not in funding_data:
                        print(f"No data found for {inst_id}")
                        break

                    data = funding_data["data"]
                    if not data:
                        print(f"No more funding data for {inst_id}")
                        break

                    for entry in data:
                        record = {
                            "_id": f"okx_{symbol}_{int(entry['fundingTime']) // 1000}",
                            "timestamp": int(entry["fundingTime"]) // 1000,
                            "symbol": symbol,
                            "exchange": "okx",
                            "funding_rate": float(entry["fundingRate"]),
                        }
                        self.collection.insert_one(record)

                    total_processed += len(data)
                    print(f"Processed {len(data)} entries for {inst_id}")
                    
                    after = data[-1]['fundingTime']
                    time.sleep(0.5)

                except Exception as e:
                    print(f"Error fetching data for {inst_id}: {e}")
                    break

            print(f"Finished processing {inst_id}. Total entries: {total_processed}")

