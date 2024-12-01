import pandas as pd

file_path = "D:\BigData\HUST_IT4931\datasets\\2019-Nov.csv"
data = pd.read_csv(file_path)

subset_data = data.head(20000000)

subset_data.to_csv("D:\BigData\HUST_IT4931\datasets\\2019-Nov-100.csv", index=False)
