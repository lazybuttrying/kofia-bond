import pandas as pd
import os

for o in os.listdir('cd_data'):
    if not o.endswith('.csv'):
        continue
    df = pd.read_csv(f'cd_data/{o}')
    print(o)
    print(df.shape, df.iloc[:, 0].min(), df.iloc[:, 0].max())
    print(df.head())
    print()
