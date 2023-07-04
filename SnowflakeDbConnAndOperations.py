import snowflake.connector as snow


class ExecuteQueriesOnSnowflake:
    def __init__(self, **kwargs):
        self.creds = kwargs
        self.conn = snow.connect(**self.creds)

    def execute_read_query(self, query):
        """
        This Function Can Fetch Data From Snowflake And Return Pandas DataFrame Of That Data.

        :return: Pandas DataFrame
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            df = cur.fetch_pandas_all()
            return df
        except Exception as e:
            return e

    def execute_dml_query(self, query):
        """
        This Function Executes DML Query On Snowflake And Return Execution Status.

        :return: Execution Status
        """
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            return "Query Executed Successfully"
        except Exception as e:
            return e
