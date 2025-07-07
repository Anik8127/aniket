import pandas as pd

df = pd.read_csv('aum_data_transformed.csv')
result = df[(df['Broker_Name'] == 'DB') & (df['Date'].str.startswith('2025-05'))]
result.to_csv('filtered_aum_data.csv', index=False)