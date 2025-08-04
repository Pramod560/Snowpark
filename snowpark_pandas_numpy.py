import pandas as pd
import numpy as np
from snowflake_connection import get_snowflake_session
from snowflake.snowpark.types import StructType, StructField, StringType
def run_analysis():
    session = get_snowflake_session()
    #session.sql("CREATE OR REPLACE STAGE IF NOT EXISTS LOCAL_FILE_STAGE").collect()
    result = session.sql("select current_database() as db, current_schema() as schema").collect()
    print(result)
    result1 = session.sql('list @LOGS.PUBLIC.MY_STG').collect()
    print(result1)
    schema1 = StructType([
        StructField("name", StringType())
    ])
    result3 = session.read.option("header",False) \
                     .option("delimiter", ",") \
                     .schema(schema1) \
                     .csv("@LOGS.PUBLIC.MY_STG/one.txt") \
                     .collect()
    print(result3)
    result4 = pd.read_csv("sample_data.csv", delimiter=",")
    print(result4.head())
    print(type(result4))
    print(result4.dtypes)
    print(result4.astype(str))
    print(result4.astype(str).dtypes)
    sf_df = session.create_dataframe(result4)
    # Save as a table in Snowflake
    sf_df.write.save_as_table("sample_table1", mode="overwrite")
    # Query and show data from the new table
    result5 = session.table("sample_table1").collect()
    print(result5)
   
    

if __name__ == "__main__":
    run_analysis()