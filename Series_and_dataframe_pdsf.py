import pandas as pd
import numpy as np
from snowflake_connection import get_snowflake_session
from snowflake.snowpark.types import StructType, StructField, StringType
def run_analysis():
    session = get_snowflake_session()
    data = ['Steve', '35', 'Male', '3.5']
    series = pd.Series(data, index=['Name', 'Age', 'Gender', 'Rating'])
    #print(series)
    df = series.to_frame()
    #print(df)
    df.reset_index(inplace=True)
    #print(df)
    df.columns = ['Attribute', 'Value']
    #print(df)
    df1 = session.create_dataframe(df)
    df1.show()

    df2 = pd.DataFrame([series], columns=['Name', 'Age', 'Gender', 'Rating'], dtype=str)
    print(df2)
    
    df2 = session.create_dataframe(df2)
    df2.show()
    '''
    df3 = ['20', 'Alice', 'Female', '4.5']
    df3 = session.create_dataframe([df3], schema=StructType([
        StructField("Age", StringType()),
        StructField("Name", StringType()),
        StructField("Gender", StringType()),
        StructField("Rating", StringType())
    ]))
    df3.show()

    df4 = ['25', 'Bob', 'Male', '4.0']
    df4 = session.create_dataframe(df4)
    df4.show()
    df5 = {'Age': '30', 'Name': 'Charlie', 'Gender': 'Male', 'Rating': '4.8'}
    df5 = session.create_dataframe([df5] , schema=('Age String, Name String, Gender String, Rating String'))
    df5.show()
    df6 = {'Age': '28', 'Name': 'Diana', 'Gender': 'Female', 'Rating': '4.9'}
    df6 = session.create_dataframe([df6], schema=('AGE', 'NAME', 'GENDER', 'RATING'))
    df6.show()
    df6.write.save_as_table("sample_table2", mode="overwrite")
    result = session.table("sample_table2")
    result.show()
    print(result.collect()) 
    '''
    #print(session.sql("select current_database() as db, current_schema() as schema").collect())
    df7 = {'Age': '22', 'Name': 'Eva', 'Gender': 'Female', 'Rating': '4.6'}
    df7 = pd.DataFrame([df7])
    session.write_pandas(df7, "sample_table3", auto_create_table=True ,  overwrite=True)
    res = session.sql("SHOW TABLES LIKE 'SAMPLE_TABLE3'")
    res.show()
    result2 = session.table('"sample_table3"').collect()
    print(result2)

if __name__ == "__main__":
    run_analysis()