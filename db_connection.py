import pymysql
from config import DB_CONFIG

class MySQLConnection:
    def __init__(self, logger):
        self.logger = logger
        self.connection = None

    def connect(self):
        """Establish a MySQL connection."""
        try:
            if not self.connection or not self.connection.open:
                self.connection = pymysql.connect(
                    host=DB_CONFIG["host"],
                    user=DB_CONFIG["user"],
                    password=DB_CONFIG["password"],
                    database=DB_CONFIG["database"],
                )
                self.logger.info("Connected to the database.")
            return self.connection
        except pymysql.MySQLError as e:
            self.logger.error(f"MySQL connection error: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed.")