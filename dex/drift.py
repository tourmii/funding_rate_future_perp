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

class DriftFundingRateScraper(SchedulerJob):
    def __init__(self, mongodb_uri, database_name, collection_name, scheduler=None):
        super().__init__(scheduler=scheduler)
        self.mongo_uri = mongodb_uri
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self.symbols = ["BTC-PERP", "ETH-PERP"]
        self.driver = self._initialize_webdriver()

    def _initialize_webdriver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            service = Service("/usr/bin/chromedriver")  # Update path if needed
            return webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            raise Exception(f"Failed to initialize WebDriver: {str(e)}")

    def fetch_data(self):
        try:
            for symbol in self.symbols:
                self._scrape_symbol(symbol)
        except Exception as e:
            print(f"An error occurred during fetch: {str(e)}")
        finally:
            self.driver.quit()

    def _dismiss_wallet_popup(self):
        try:
            # Wait and attempt to close wallet modal
            close_btn = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class*='close'], .modal-close"))
            )
            close_btn.click()
            print("Closed wallet modal.")
        except:
            print("No wallet modal to dismiss.")

    def _scrape_symbol(self, symbol):
        try:
            print(f"Navigating to Drift Trade for {symbol}...")
            self.driver.get(f"https://app.drift.trade/{symbol}")
            time.sleep(5)

            self._dismiss_wallet_popup()

            # Adjust XPath to target actual funding rate (update if necessary)
            rate_xpath = "//div[contains(@class, 'funding-rate') or contains(@class, 'fundingRate')]"

            element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, rate_xpath))
            )
            funding_rate_text = element.text
            print(f"Funding rate text for {symbol}: {funding_rate_text}")

            numeric_match = re.search(r"([+-]?\d+\.?\d*)%", funding_rate_text)
            if numeric_match:
                rate_value = float(numeric_match.group(1)) / 100
                timestamp = int(time.time())

                record = {
                    "_id": f"drift_{symbol}_{timestamp}",
                    "timestamp": timestamp,
                    "symbol": symbol.replace("-PERP", ""),
                    "exchange": "drift",
                    "funding_rate": rate_value,
                }

                print(f"Inserting into MongoDB: {record}")
                self.collection.insert_one(record)
            else:
                print(f"Could not parse funding rate for {symbol}: '{funding_rate_text}'")

        except Exception as e:
            print(f"Error scraping {symbol}: {str(e)}")
        time.sleep(3)
