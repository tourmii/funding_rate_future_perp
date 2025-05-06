import time
import re
import pymongo
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from cli_scheduler import SchedulerJob


class GMXFundingRateFetcher(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.symbols = ["BTC"]
        self.symbol_mapping = {"BTC": "BTC"}
        self.client = pymongo.MongoClient(self.mongodb_uri)
        self.collection = self.client[self.database_name][self.collection_name]

    def _initialize_webdriver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            service = Service("/usr/bin/chromedriver")  # Adjust if needed
            return webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize WebDriver: {str(e)}")

    def fetch_data(self):
        driver = self._initialize_webdriver()

        try:
            for symbol in self.symbols:
                print(f"Fetching funding rate for {symbol} from GMX...")

                driver.get("https://app.gmx.io/#/trade")
                time.sleep(15)  # Wait for page and data to fully load

                xpath = (
                    "/html/body/div[1]/div/div[1]/div/div/div[1]/div[1]/div[1]/div[1]"
                    "/div[2]/div[2]/div[5]/div[2]/div[1]/span"
                )

                try:
                    wait = WebDriverWait(driver, 20)
                    element = wait.until(EC.visibility_of_element_located((By.XPATH, xpath)))
                    funding_rate_text = element.text
                    print(f"Funding rate text: {funding_rate_text}")
                except Exception:
                    print(f"Unable to locate funding rate element for {symbol}.")
                    continue

                match = re.search(r'([+-]?\d+\.?\d*)%', funding_rate_text)
                if match:
                    rate_value = float(match.group(1)) / 100
                    timestamp = int(time.time())
                    mapped_symbol = self.symbol_mapping.get(symbol, symbol)

                    record = {
                        "_id": f"gmx_{mapped_symbol}_{timestamp}",
                        "timestamp": timestamp,
                        "symbol": mapped_symbol,
                        "exchange": "gmx",
                        "funding_rate": rate_value,
                    }

                    self.collection.insert_one(record)
                    print(f"Inserted funding rate record: {record}")
                else:
                    print(f"Could not extract numeric funding rate from text: {funding_rate_text}")

        except Exception as e:
            print(f"Unexpected error during fetch: {str(e)}")

        finally:
            driver.quit()
            print("WebDriver closed.")
