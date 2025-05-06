from vertex_protocol.client import create_vertex_client
import pymongo
from cli_scheduler import SchedulerJob

class VertexFundingRateFetcher(SchedulerJob):
    def __init__(self, private_key, mongodb_uri, database, collection, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.client = create_vertex_client("mainnet", private_key)
        self.py_client = pymongo.MongoClient(mongodb_uri)
        self.collection = self.py_client[database][collection]
        self.product_mapping = {
            '2': 'BTC',
            '4': 'ETH',
        }

    def fetch_data_vertex(self):
        funding_rate_data = self.client.market.get_perp_funding_rates([2, 4])
        
        formatted = {}
        for prod_id, fund_rate in funding_rate_data.items():
            rate_value = float(fund_rate.funding_rate_x18) / 1e18  
            timestamp = int(fund_rate.update_time)
            symbol = self.product_mapping.get(str(prod_id), f"Unknown({prod_id})")
            formatted[symbol] = {
                "_id": f"vertex_{symbol}_{timestamp}",
                "timestamp": timestamp,
                "symbol": symbol,
                "exchange": "vertex",
                "fund_rate": rate_value
            }
        
        for product in formatted:
            record = formatted[product]
            print(record)
            self.collection.insert_one(record)
            print(f"Inserted funding rate for {record['symbol']} into MongoDB.")

