import snowflake.connector
from tenacity import retry, stop_after_attempt, wait_fixed
from .params import *


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def create_connection():
    """
    Create a connection to Snowflake using the provided credentials.

    Retries the connection creation up to 3 times with a fixed wait time of 2 seconds between attempts.

    Returns:
        The Snowflake connection object.
    """

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account_identifier,
        database=database,
        schema=schema,
    )

    return conn


# Create a Snowflake connection
snowflakeConnection = create_connection()
