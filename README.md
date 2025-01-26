# stock-data-analysis

This project is designed to fetch stock data for a specific stock symbol, transform it, and store it in a database. The project fetches real-time stock data, calculates moving averages, volatility, and stores the data in a dynamic table structure within a database.

## Project Flow

### 1. **Fetching Stock Data**
   - **API Integration**: The stock data is fetched using the Yahoo Finance API via a `GET` request. The data is returned in JSON format and includes relevant stock attributes such as `close_price`, `open_price`, `high_price`, `low_price`, and `volume`.
   - **Dynamic Stock Symbol Handling**: The project handles different stock symbols dynamically and fetches their respective data.
   - **Error Handling**: In case of missing data or API errors, appropriate error messages are logged and the process continues.

### 2. **Storing Data**
   - **Dynamic Table Creation**: Each stock symbol has its own table. The table will be created dynamically if it doesn’t exist yet.
   - **Database Insertion**: Once the data is fetched, it's inserted into the corresponding table in the database.

### 3. **Data Transformation**
   - The raw data is transformed by calculating moving averages and volatility for each of the stock price fields (`open_price`, `high_price`, `low_price`, `close_price`).
   - **Rolling Window Calculation**: A 5-day rolling window is applied to compute the moving average and volatility for each field.
   - The transformed data is returned as a DataFrame with new columns: `moving_average`, `volatility`, and `volume` statistics.

### 4. **Database Structure**
   - Each stock symbol has its own table. The table is created dynamically if it doesn't exist.
   - The database stores stock data with columns like `timestamp`, `close_price`, `open_price`, `high_price`, `low_price`, and `volume`.

### 5. **Code Organization**
   - **Fetching Data**: `fetch_latest_data(self)` method fetches the stock data from the API.
   - **Transforming Data**: `transform_data(self, raw_data)` method transforms the raw data by calculating moving averages and volatility.
   - **Database Operations**: Includes functions for inserting data into the database, creating tables dynamically, and handling stock symbols.

## Requirements

To run this project, you need to install the following dependencies:

- `requests`: For making API requests.
- `pandas`: For data manipulation.
- `sqlite3` or another database connection library (depending on your database).
- `logging`: For logging messages.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/stock-data-fetcher.git
   cd stock-data-fetcher
