import requests
import pymongo
import time
from cli_scheduler import SchedulerJob


class ApexFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = pymongo.MongoClient(mongodb_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.product_mapping = {
            'BTC-USDT': 'BTC',
            'ETH-USDT': 'ETH',
        }

    def get_funding_rate_history(self, symbol="BTC-USDT", limit=100, begin_time=None, end_time=None, page=0):
        base_url = "https://omni.apex.exchange/api/v3/history-funding"
        params = {
            "symbol": symbol,
            "limit": limit,
            "page": page
        }
        if begin_time is not None:
            params["beginTimeInclusive"] = begin_time
        if end_time is not None:
            params["endTimeExclusive"] = end_time
        
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

    def fetch_data(self):
        """Fetch all available historical data by paging through results"""
        for original_symbol, mapped_symbol in self.product_mapping.items():
            page = 0
            total_processed = 0
            
            while page < 2:
                try:
                    print(f"Fetching page {page} for {original_symbol}")
                    funding_data = self.get_funding_rate_history(symbol=original_symbol, limit=100, page=page)
                    
                    if funding_data and "data" in funding_data and "historyFunds" in funding_data["data"]:
                        history_funds = funding_data["data"]["historyFunds"]
                        
                        if not history_funds:
                            print(f"No more data for {original_symbol} after page {page}")
                            break
                        
                        for funding_entry in history_funds:
                            record = {
                                "_id": f"apex_{mapped_symbol}_{funding_entry['fundingTimestamp']}",
                                "timestamp": int(funding_entry["fundingTimestamp"]) // 1000,
                                "symbol": mapped_symbol,
                                "exchange": "apex",
                                "funding_rate": float(funding_entry["rate"]),
                            }
                            
                            self.collection.insert_one(record)
                        
                        total_processed += len(history_funds)
                        print(f"Processed {len(history_funds)} entries on page {page} for {original_symbol}")
                        
                        if len(history_funds) < 100:
                            print(f"Reached end of data for {original_symbol}")
                            break
                        
                        page += 1
                        
                        time.sleep(0.5)
                        
                    else:
                        print(f"No funding data available for {original_symbol} on page {page}")
                        break
                        
                except Exception as e:
                    print(f"Error processing {original_symbol} on page {page}: {e}")
                    break
            
            print(f"Finished processing {original_symbol}. Total entries: {total_processed}")
