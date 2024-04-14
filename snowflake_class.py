import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

class SnowflakeDB:
    def __init__(self, user=None, password=None, account=None, warehouse=None, database=None, schema=None):
        self.user = os.environ.get("SNOWFLAKE_USER", user)
        self.password = os.environ.get("SNOWFLAKE_PASSWORD", password)
        self.account = os.environ.get("SNOWFLAKE_ACCOUNT", account)
        self.warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", warehouse)
        self.database = os.environ.get("SNOWFLAKE_DATABASE", database)
        self.schema = os.environ.get("SNOWFLAKE_SCHEMA", schema)

    def create_connection(self):
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        conn.cursor().execute(f"USE SCHEMA {self.database}.{self.schema};")
        return conn

    def read(self, query, **kwargs):
        with self.create_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, kwargs)
                rows = cursor.fetchall()
                if rows:
                    df = pd.DataFrame(rows, columns=[x[0] for x in cursor.description])
                    return df

    def write(self, dataframe, table_name):
        with self.create_connection() as conn:
            # Convert columns to uppercase, otherwise the Snowflake connector fails
            dataframe.columns = map(lambda x: str(x).upper(), dataframe.columns)

            # The table name also needs to be specified in uppercase, otherwise it won't find the table
            success, nchunks, nrows, _ = write_pandas(
                conn, dataframe, table_name=table_name.upper(), auto_create_table=True
            )
            if not success:
                raise Exception("Failed to write file")