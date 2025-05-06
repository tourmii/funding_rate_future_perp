import pymongo
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from cli_scheduler import SchedulerJob
from selenium.webdriver.chrome.service import Service

class AsterFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.mongo_uri = mongodb_uri
        self.py_client = pymongo.MongoClient(self.mongo_uri)
        self.database = self.py_client[database_name]
        self.collection = self.database[collection_name]
        self.symbols = ["BTC", "ETH"]
        self.product_mapping = {
            "BTC": "BTC",
            "ETH": "ETH",
        }
        self.driver = self._initialize_webdriver()

    def _initialize_webdriver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--window-size=1920,1080")

        try:
            # Change this path if chromedriver is installed somewhere else
            service = Service("/usr/bin/chromedriver")  
            return webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            raise Exception(f"Failed to initialize WebDriver: {str(e)}")

    def fetch_data_aster(self):
        try:
            for symbol in self.symbols:
                print(f"Navigating to AsterDEX for {symbol}...")
                self.driver.get(f"https://www.asterdex.com/en/futures/{symbol}USD")
                time.sleep(5)

                print(f"Waiting for {symbol} funding rate element...")
                xpath = "/html/body/div/main/div[1]/div/div[3]/div/div/div[2]/button/div[1]/div"
                wait = WebDriverWait(self.driver, 15)
                element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))

                funding_rate_text = element.text
                print(f"Current {symbol} funding rate text: {funding_rate_text}")

                numeric_match = re.search(r'([+-]?\d+\.\d+)', funding_rate_text)
                if numeric_match:
                    rate_value = float(numeric_match.group(1))
                    timestamp = int(time.time())
                    product = self.product_mapping.get(symbol, f"Unknown({symbol})")

                    record = {
                        "_id": f"asterdex_{product}_{timestamp}",
                        "timestamp": timestamp,
                        "symbol": product,
                        "exchange": "asterdex",
                        "funding_rate": rate_value / 100
                    }

                    print(record)

                    self.collection.insert_one(record)
                    print(f"Inserted funding rate for {record['symbol']} into MongoDB.")
                else:
                    print(f"Could not extract numeric value from {funding_rate_text}")

                time.sleep(3)

        except Exception as e:
            print(f"An error occurred: {str(e)}")

