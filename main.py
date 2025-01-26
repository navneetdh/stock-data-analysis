from logger import get_logger
from etl_pipeline import ETLPipeline
from config import API_KEY

if __name__ == "__main__":
    logger = get_logger("ETL_Pipeline")
    STOCK_SYMBOLS = ["AAPL", "MSFT", "^SPX", "^NYA", "GAZP.ME", "SIBN.ME", "GEECEE.NS"]
    
    etl_pipeline = ETLPipeline(STOCK_SYMBOLS, API_KEY, logger)
    etl_pipeline.run()
