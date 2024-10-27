import pandas as pd
import time

file_path = 'datasets/2019-Nov-100.csv'

chunk_size = 1000

previous_time = None

for chunk in pd.read_csv(file_path, chunksize=chunk_size, parse_dates=['event_time']):
    for timestamp, rows in chunk.groupby('event_time'):
        if previous_time is not None:
            delay = (timestamp - previous_time).total_seconds()
            if delay > 0:
                time.sleep(delay)

        print(rows)
        previous_time = timestamp