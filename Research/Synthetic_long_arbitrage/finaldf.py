import pandas as pd
df = pd.read_csv("/root/aniket/Research/Synthetic_long_arbitrage/synthetic_with_c_symbol.csv")
df.rename(columns={"c": "Future_close", "symbol": "FutureSymbol"}, inplace=True)
df['Arbitrage'] = df['Future_close'] - df['Synthetic']
df['Straddle'] = df['Call_Premium'] + df['Put_Premium']
df.to_csv("final_df.csv", index=False)