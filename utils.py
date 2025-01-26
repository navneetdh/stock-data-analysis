def create_table_if_not_exists(connection, table_name, logger):
    """
    Creates a table for the given stock symbol if it doesn't exist.
    """
    try:
        cursor = connection.cursor()
        query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume INT,
            timestamp DATETIME UNIQUE,
            moving_average FLOAT,
            volatility FLOAT
        )
        """
        cursor.execute(query)
        connection.commit()
        logger.info(f"Table `{table_name}` ensured.")
    except Exception as e:
        logger.error(f"Error creating table `{table_name}`: {e}")
        raise