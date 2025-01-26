from utils import create_table_if_not_exists
from db_connection import MySQLConnection
from data_fetcher import StockDataFetcher
import pandas as pd

class ETLPipeline:
    def __init__(self, symbols, api_key, logger):
        self.symbols = symbols
        self.api_key = api_key
        self.logger = logger
        self.db_connection = MySQLConnection(self.logger)

    def transform_data(self, raw_data):
        """
        Transforms raw stock data.
        """
        try:
            if raw_data is None:
                raise ValueError("No data to transform")

            # Create DataFrame from the raw data
            df = pd.DataFrame([raw_data])

            # Apply rolling window transformations (moving average and volatility) for all relevant columns
            rolling_window = 5  # Example: 5-day window for moving average and volatility

            # Calculate moving averages
            for col in ["open_price", "high_price", "low_price", "close_price"]:
                df[f"{col}_moving_average"] = df[col].rolling(window=rolling_window).mean()

            # Calculate volatility (standard deviation)
            for col in ["open_price", "high_price", "low_price", "close_price"]:
                df[f"{col}_volatility"] = df[col].rolling(window=rolling_window).std()

            # For volume, we could compute a moving average and volatility as well, depending on the use case
            df["volume_moving_average"] = df["volume"].rolling(window=rolling_window).mean()
            df["volume_volatility"] = df["volume"].rolling(window=rolling_window).std()

            # Log the successful transformation
            self.logger.info("Data transformed.")
            return df

        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            raise

    def load_to_table(self, transformed_data, table_name):
        """
        Inserts data into the individual stock table.
        """
        try:
            connection = self.db_connection.connect()
            create_table_if_not_exists(connection, table_name, self.logger)

            for _, row in transformed_data.iterrows():
                query = f"""
                INSERT IGNORE INTO `{table_name}` (
                    open_price, high_price, low_price, close_price, volume, timestamp,
                    moving_average, volatility
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                connection.cursor().execute(
                    query, (
                        row['open_price'], row['high_price'], row['low_price'],
                        row['close_price'], row['volume'], row['timestamp'],
                        row['moving_average'], row['volatility'],
                    )
                )
            connection.commit()
            self.logger.info(f"Data loaded into `{table_name}`.")
        except Exception as e:
            self.logger.error(f"Error loading data into `{table_name}`: {e}")
            raise

    def run(self):
        """
        Executes the ETL pipeline for all stocks.
        """
        for symbol in self.symbols:
            try:
                # Instantiate StockDataFetcher for each symbol
                fetcher = StockDataFetcher(symbol, self.db_connection.connect(), self.logger, self.api_key)
                raw_data = fetcher.fetch_latest_data()
                transformed_data = self.transform_data(raw_data)
                self.load_to_table(transformed_data, symbol)
            except Exception as e:
                self.logger.error(f"Pipeline error for {symbol}: {e}")