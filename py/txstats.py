import pandas as pd

df = pd.read_csv("txs.csv")

# get useful info from this df

# Calculate the total conversion fee for each asset
total_fee_zeph = df[df['conversion_fee_asset'] == 'ZEPH']['conversion_fee_amount'].sum()
total_fee_zephusd = df[df['conversion_fee_asset'] == 'ZEPHUSD']['conversion_fee_amount'].sum()
total_fee_zephrsv = df[df['conversion_fee_asset'] == 'ZEPHRSV']['conversion_fee_amount'].sum()

# Total number of transactions
total_txns = len(df)

# Number of transactions by conversion type
txns_by_type = df['conversion_type'].value_counts()

# Average number of transactions per block
avg_txns_per_block = total_txns/max(df['block'])

# Conversion rate statistics for each conversion type
conversion_types = df['conversion_type'].unique()
conversion_rate_stats = {}
for c_type in conversion_types:
    sub_df = df[df['conversion_type'] == c_type]
    stats = {
        "min": sub_df['conversion_rate'].min(),
        "max": sub_df['conversion_rate'].max(),
        "median": sub_df['conversion_rate'].median(),
        "mean": sub_df['conversion_rate'].mean()
    }
    conversion_rate_stats[c_type] = stats

# Total mint and burns for each asset
assets = ['ZEPH', 'ZEPHUSD', 'ZEPHRSV']
asset_balances = {}
for asset in assets:
    if asset != 'ZEPH':
        mint = df[df['to_asset'] == asset]['to_amount'].sum()
        redeem = df[df['from_asset'] == asset]['from_amount'].sum()
        asset_balances[asset] = {"mint": mint, "redeem": redeem, "net": mint - redeem}
    else:
        redeem = df[df['to_asset'] == asset]['to_amount'].sum()
        added = df[df['from_asset'] == asset]['from_amount'].sum()
        asset_balances[asset] = {"added": added, "redeem": redeem, "net": added - redeem}

    

# Print results
print(f"Total number of transactions: {total_txns}")
print("\nNumber of transactions by conversion type:")
print(txns_by_type)
print(f"\nAverage number of transactions per block: {avg_txns_per_block:.2f}")

print("\nConversion rate statistics:")
for c_type, stats in conversion_rate_stats.items():
    print(f"For {c_type}:")
    print(f"  Min rate: {stats['min']:.4f}")
    print(f"  Max rate: {stats['max']:.4f}")
    print(f"  Median rate: {stats['median']:.4f}")
    print(f"  Mean rate: {stats['mean']:.4f}")
    print("")

print(f"\nTotal ZEPH conversion fees: {total_fee_zeph}")
print(f"Total ZEPHUSD conversion fees: {total_fee_zephusd}")
print(f"Total ZEPHRSV conversion fees: {total_fee_zephrsv}")

print("\nMint and Redeem figures (Asset Balance/Totals):")
for asset, figures in asset_balances.items():
    if asset != "ZEPH":
        print(f"For {asset}:")
        print(f"  Minted: {figures['mint']}")
        print(f"  Redeemed: {figures['redeem']}")
        print(f"  Net (circ): {figures['net']}")
        print("")
    else:
        print(f"For {asset}:")
        print(f"  Added: {figures['added']}")
        print(f"  Redeemed: {figures['redeem']}")
        print(f"  Net (in RES): {figures['net']}")
        print("")

