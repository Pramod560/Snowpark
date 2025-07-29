# Step-by-step instructions are provided as comments. Write your code below each instruction.

# 1. Import required modules
# (Write your import statements here)
from snowflake_connection import get_snowflake_session
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType
import os



# 2. Define the SnowparkTableManager class
# (Write your class definition here)

class SnowparkTableManager:
    def __init__(self, session, table_name):
        self.session = session
        self.table_name = table_name

    def create_table_from_dataframe(self, df):
        # Create a table from the DataFrame
        df.write.save_as_table(self.table_name, mode="overwrite")
        print(f"Table '{self.table_name}' created from DataFrame.")


    def show_table_data(self):
        # Show data in the table
        df = self.session.table(self.table_name)
        df.show(10)
    
# 3. Implement the __init__ method for the class
# (Write the __init__ method here)

# 4. Implement the create_table_from_dataframe method
# (Write the method to create a table from a DataFrame here)

# 5. Implement the show_table_data method
# (Write the method to show table data here)

# 6. Define the run_analysis function
# (Write the function to run the analysis here)
def run_analysis():
    # 7. Get a Snowflake session
    session = get_snowflake_session()

    # 8. Create named stage if not exists
    stage_name = "my_stage"
    session.sql(f"CREATE OR REPLACE STAGE {stage_name}").collect()

    # 9. Check if the local CSV file exists and upload it
    local_file_path = "data/my_data.csv"
    if os.path.exists(local_file_path):
        session.file.put(local_file_path, f"@{stage_name}/my_data.csv", auto_compress=False)
    else:
        raise FileNotFoundError(f"The file {local_file_path} does not exist.")

    # 10. Define schema explicitly
    schema = "id INT, name STRING, age INT"

    # 11. Load CSV file from stage using Snowpark with defined schema
    df = session.read.option("header", True).option("delimiter", ",").csv(f"@{stage_name}/my_data.csv", schema=schema)

    # 12. Use the TableManager to create a table from the DataFrame
    table_name = "my_table"
    table_manager = SnowparkTableManager(session, stage_name, table_name)
    table_manager.create_table_from_dataframe(df)

    # 13. Show data in the table
    data = table_manager.show_table_data()
    for row in data:
        print(row)

# 7. Inside run_analysis: Get a Snowflake session
# (Write code to get the session here)

# 8. Create named stage if not exists
# (Write code to create the stage here)

# 9. Check if the local CSV file exists and upload it
# (Write code to check and upload the file here)

# 10. Define schema explicitly
# (Write code to define the schema here)

# 11. Load CSV file from stage using Snowpark with defined schema
# (Write code to load the CSV into a DataFrame here)

# 12. Use the TableManager to create a table from the DataFrame
# (Write code to create the table here)

# 13. Show data in the table
# (Write code to show the data here)

# 14. Add the script entry point
# (Write the if __name__ == "__main__": block here)
