import snowflake.connector
import os
from tenacity import retry, stop_after_attempt, wait_fixed
from .params import *
from dotenv import load_dotenv

load_dotenv()


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def create_connection():
    """
    Create a connection to Snowflake using the provided credentials.

    Retries the connection creation up to 3 times with a fixed wait time of 2 seconds between attempts.

    Returns:
        The Snowflake connection object.
    """

    conn = snowflake.connector.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        account=os.getenv("account_identifier"),
        database=os.getenv("database"),
        schema=os.getenv("schema"),
    )

    return conn


# Create a Snowflake connection
snowflakeConnection = create_connection()
