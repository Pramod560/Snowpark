from snowflake_connection import get_snowflake_session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
import os

class SnowparkTableManager:
    def __init__(self, session, table_name):
        self.session = session
        self.table_name = table_name

    def create_table_from_dataframe(self, df: DataFrame):
        df.write.save_as_table(self.table_name, mode="overwrite")
        print(f"Table '{self.table_name}' created from DataFrame.")

    def show_table_data(self, n=10):
        df = self.session.table(self.table_name)
        df.show(n)

def run_analysis():
    session = get_snowflake_session()
    # Create named stage if not exists
    session.sql("CREATE STAGE IF NOT EXISTS LOCAL_FILE_STAGE").collect()
    # Upload the CSV file to the stage (will overwrite if exists)
    # Test: another change to trigger workflow and PR automation
    local_csv_path = "snowflake_snowpark_brainstorming/sample_data.csv"
    if os.path.exists(local_csv_path):
        session.file.put(local_csv_path, "@LOCAL_FILE_STAGE", overwrite=True)
        print(f"Uploaded '{local_csv_path}' to @LOCAL_FILE_STAGE.")
    else:
        print(f"File '{local_csv_path}' not found.")

    # Define schema explicitly
    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("value", IntegerType())
    ])
    # Load CSV file from stage using Snowpark with defined schema
    csv_df = session.read.schema(schema).options({"FIELD_DELIMITER": ",", "SKIP_HEADER": 1}).csv("@LOCAL_FILE_STAGE/sample_data.csv")
    manager = SnowparkTableManager(session, "sample_table")
    manager.create_table_from_dataframe(csv_df)
    # Show data in the table
    manager.show_table_data(10)

if __name__ == "__main__":
    run_analysis()


