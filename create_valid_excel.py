import pandas as pd

data = [
    {"id": 1, "name": "Alice", "value": 100},
    {"id": 2, "name": "Bob", "value": 200},
    {"id": 3, "name": "Charlie", "value": 300}
]
df = pd.DataFrame(data)
df.to_excel("snowflake_snowpark_brainstorming/sample_data.xlsx", index=False)
print("Valid Excel file created at snowflake_snowpark_brainstorming/sample_data.xlsx")
