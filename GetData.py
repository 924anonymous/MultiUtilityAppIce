import boto3
import awswrangler as wr


class ExecuteQueriesOnIceberg:
    def __init__(self):
        self.session = boto3.Session(profile_name="dai-local-usage")

    def fetch_df(self):
        """
        This Function Can Fetch Data From Iceberg And Return Pandas DataFrame Of That Data.

        :return: Pandas DataFrame
        """
        df = wr.athena.read_sql_query(sql='select * from mining_data',
                                      database='target',
                                      boto3_session=self.session)

        return df

    def execute_query(self, query, database):
        """
        This Function Executes The Given Query On Postgres.

        :param query: Query To Execute On Iceberg Table
        :param database: Database Name In Which Table Is Residing
        :return: Pandas DataFrame
        """
        df = wr.athena.read_sql_query(sql=query,
                                      database=database,
                                      boto3_session=self.session)

        return df

    def execute_dml_query(self, query, database):
        """
        This Function Executes The Given Query On Postgres.

        :param query: Query To Execute On Iceberg Table
        :param database: Database Name In Which Table Is Residing
        :return: Pandas DataFrame
        """
        res = wr.athena.start_query_execution(sql=query,
                                              database=database,
                                              boto3_session=self.session)

        return res
