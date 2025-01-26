import requests
from datetime import datetime, timedelta
import traceback

class StockDataFetcher:
    def __init__(self, symbol, connection, logger, api_key):
        """
        Initializes the StockDataFetcher class.

        :param symbol: Stock symbol to fetch data for.
        :param connection: Database connection object.
        :param logger: Logger object for logging.
        :param api_key: RapidAPI key for authentication.
        """
        self.symbol = symbol
        self.connection = connection
        self.logger = logger
        self.api_key = api_key
        self.base_url = "https://yh-finance.p.rapidapi.com/market/v2/get-quotes"

    def fetch_latest_data(self):
        try:
            # Fetch the last timestamp from the database
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT MAX(timestamp) FROM stock_fetch_tracker_table WHERE stock_symbol = %s",
                (self.symbol,),
            )
            last_timestamp = cursor.fetchone()[0]

            # Use the last timestamp or fetch data for the past 7 days
            start_date = (
                (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
                if not last_timestamp
                else last_timestamp.strftime("%Y-%m-%d")
            )

            # API Request
            querystring = {"region": "US", "symbols": f"{self.symbol}"}

            headers = {
                "x-rapidapi-key": self.api_key,
                "x-rapidapi-host": "yh-finance.p.rapidapi.com",
            }

            response = requests.get(self.base_url, headers=headers, params=querystring)

            data = response.json()

            # Validate and extract data correctly from nested structure
            try:
                quote_response = data.get("quoteResponse", {})
                result = quote_response.get("result", [])

                if not result:
                    raise ValueError("No data found for the provided symbol.")

                stock_data = result[0]  # Assuming data for symbol is always the first element

                # Extract required values
                open_price = stock_data.get('regularMarketOpen')
                high_price = stock_data.get('regularMarketDayHigh')
                low_price = stock_data.get('regularMarketDayLow')
                close_price = stock_data.get('regularMarketPreviousClose') or stock_data.get('previousClose')
                volume = stock_data.get('regularMarketVolume')
                timestamp = datetime.utcnow()

                # Example of how you can calculate moving_average and volatility (if needed)
                moving_average = stock_data.get('fiftyDayAverage')
                volatility = stock_data.get('beta')  # This is just a placeholder; adjust as needed

                if not all([open_price, high_price, low_price, close_price, volume]):
                    raise ValueError("Required data is missing in the response.")

                # Return the extracted data
                return {
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "volume": volume,
                    "timestamp": timestamp,
                    "moving_average": moving_average,
                    "volatility": volatility,
                }

            except (KeyError, ValueError) as e:
                # Handle errors in data extraction or missing keys
                self.logger.error(f"Error extracting data for {self.symbol}: {e}")
                return None  # or raise an exception if preferred

        except requests.exceptions.RequestException as e:
            # Handle network-related errors
            self.logger.error(f"Error fetching data for {self.symbol}: {e}")
            return None  # or raise an exception if preferred

