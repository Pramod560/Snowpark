import pandas as pd

data = [
    [1, "Alice", 100],
    [2, "Bob", 200],
    [3, "Charlie", 300]
]
columns = ["id", "name", "value"]
df = pd.DataFrame(data, columns=columns)
df.to_excel("sample_data.xlsx", index=False)
