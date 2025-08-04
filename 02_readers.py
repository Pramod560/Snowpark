
import modin.pandas as mpd
import snowflake.snowpark.modin.plugin
import pandas as pd
from snowflake_connection import get_snowflake_session,  get_snowflake_session2
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
from snowflake.snowpark.functions import col
import numpy as np

session = get_snowflake_session()

def run_pandas_analysis():
    df = pd.read_csv("sample_data.csv", header=0, delimiter=",")
    print(type(df))
    print(df)
    return df

def run_modin_analysis():
    df = mpd.read_csv('@my_stg/one.txt', header=None)
    print(type(df))
    print(df)

def run_snowpark_analysis():
    df = session.read.option("header", False).csv("@my_stg/one.txt")
    print(type(df))
    df.show()
    df1 = run_pandas_analysis()
    df2 = session.create_dataframe(df1)
    df3 = df2.select(col('"name"').alias("Name"), col('"value"').cast(IntegerType()).alias("Days"))\
        .filter(col('"value"') > 200).count()
    print(df3)

def run_specific_reader():
    df = mpd.read_snowflake('"sample_table3"')
    print(type(df))
    print(df)
    df1 = mpd.read_sql('SELECT * FROM "sample_table3"', con=get_snowflake_session())
    print(type(df1))
    print(df1)
    df3 = mpd.read_sql_table('"sample_table3"')
    print(type(df3))
    print(df3)

if __name__ == "__main__":
    
    run_modin_analysis()
    run_snowpark_analysis()
    run_specific_reader()


