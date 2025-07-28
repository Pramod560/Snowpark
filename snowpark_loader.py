from snowflake_connection import get_snowflake_session
from snowflake.snowpark import DataFrame
import pandas as pd
import os

class SnowparkDataLoader:
    def __init__(self, session):
        self.session = session

    def upload_csv_to_stage(self, file_path, stage_name="LOCAL_FILE_STAGE"):
        # Create stage if not exists
        self.session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()
        if os.path.exists(file_path):
            self.session.file.put(file_path, f"@{stage_name}", overwrite=True)
            print(f"Uploaded '{file_path}' to @{stage_name}.")
        else:
            print(f"File '{file_path}' not found.")

    def load_csv_with_inferschema(self, stage_name, file_name, table_name):
        stage_file_path = f"@{stage_name}/{file_name}"
        options = {"FIELD_DELIMITER": ",", "infer_schema": True, "SKIP_HEADER": 1}
        df = self.session.read.options(options).csv(stage_file_path)
        df.write.save_as_table(table_name, mode="append")
        print(f"Loaded CSV '{stage_file_path}' into table '{table_name}' using inferred schema.")

    def load_excel_with_pandas(self, file_path, table_name):
        pdf = pd.read_excel(file_path, engine="openpyxl")
        df = self.session.create_dataframe(pdf.values.tolist(), schema=list(pdf.columns))
        df.write.save_as_table(table_name, mode="append")
        print(f"Loaded Excel '{file_path}' into table '{table_name}' using pandas.")

if __name__ == "__main__":
    session = get_snowflake_session()
    loader = SnowparkDataLoader(session)
    local_csv = "snowflake_snowpark_brainstorming/sample_data.csv"
    stage_name = "LOCAL_FILE_STAGE"
    file_name = "sample_data.csv"
    loader.upload_csv_to_stage(local_csv, stage_name)
    loader.load_csv_with_inferschema(stage_name, file_name, "sample_table")
    loader.load_excel_with_pandas("snowflake_snowpark_brainstorming/sample_data.xlsx", "sample_table")
