import redis
import json
import pandas as pd
import matplotlib.pyplot as plt

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Get all the data from the 'txs' key
raw_data = redis_client.hgetall('txs')

# Prepare a list to store the parsed transaction data
transactions = []

# Iterate over raw data and extract valid JSON transactions
for key, value in raw_data.items():
    try:
        transaction = json.loads(value)
        transactions.append(transaction)
    except json.JSONDecodeError:
        print(f"Skipping invalid JSON entry for key: {key}")

# Convert the list of transactions to a DataFrame
df = pd.DataFrame(transactions)

# Save the DataFrame to a CSV file
df.to_csv('transactions.csv', index=False)

print("Data has been saved to transactions.csv")


# Read the df to get some stats

#   conversion_transactions: total rows
#   yield_conversion_transactions: either mint_yield or redeem_yield
#   mint_reserve_count: total mint_reserve transactions
#   mint_reserve_volume: total mint_reserve volume
#   fees_zephrsv: where conversion_fee_asset = ZEPHRSV
#   redeem_reserve_count: total redeem_reserve transactions
#   redeem_reserve_volume: total redeem_reserve volume
#   fees_zephusd: where conversion_fee_asset = ZEPHUSD && conversion type = mint_stable
#   mint_stable_count: total mint_stable transactions
#   mint_stable_volume: total mint_stable volume
#   redeem_stable_count: total redeem_stable transactions
#   redeem_stable_volume: total redeem_stable volume
#   fees_zeph: where conversion_fee_asset = ZEPH
#   mint_yield_count: total mint_yield transactions
#   mint_yield_volume: total mint_yield volume
#   fees_zyield: where conversion_fee_asset = ZYIELD
#   redeem_yield_count: total redeem_yield transactions
#   redeem_yield_volume: total redeem_yield volume
#   fees_zephusd_yield: where conversion_fee_asset = ZEPHUSD && conversion type = mint_yield

# output a bunch of graphs e.g. number of conversions per day, volume of conversions per day, etc


# Read the df to get some stats

# conversion_transactions: total rows
conversion_transactions = len(df)

# yield_conversion_transactions: either mint_yield or redeem_yield
yield_conversion_transactions = df[df['conversion_type'].isin(['mint_yield', 'redeem_yield'])]

# mint_reserve_count: total mint_reserve transactions
mint_reserve_count = len(df[df['conversion_type'] == 'mint_reserve'])

# mint_reserve_volume: total mint_reserve volume
mint_reserve_volume = df[df['conversion_type'] == 'mint_reserve']['to_amount'].sum()

# fees_zephrsv: where conversion_fee_asset = ZEPHRSV
fees_zephrsv = df[df['conversion_fee_asset'] == 'ZEPHRSV']['conversion_fee_amount'].sum()

# redeem_reserve_count: total redeem_reserve transactions
redeem_reserve_count = len(df[df['conversion_type'] == 'redeem_reserve'])

# redeem_reserve_volume: total redeem_reserve volume
redeem_reserve_volume = df[df['conversion_type'] == 'redeem_reserve']['from_amount'].sum()

# fees_zephusd: where conversion_fee_asset = ZEPHUSD && conversion type = mint_stable
fees_zephusd = df[(df['conversion_fee_asset'] == 'ZEPHUSD') & (df['conversion_type'] == 'mint_stable')]['conversion_fee_amount'].sum()

# mint_stable_count: total mint_stable transactions
mint_stable_count = len(df[df['conversion_type'] == 'mint_stable'])

# mint_stable_volume: total mint_stable volume
mint_stable_volume = df[df['conversion_type'] == 'mint_stable']['to_amount'].sum()

# redeem_stable_count: total redeem_stable transactions
redeem_stable_count = len(df[df['conversion_type'] == 'redeem_stable'])

# redeem_stable_volume: total redeem_stable volume
redeem_stable_volume = df[df['conversion_type'] == 'redeem_stable']['from_amount'].sum()

# fees_zeph: where conversion_fee_asset = ZEPH
fees_zeph = df[df['conversion_fee_asset'] == 'ZEPH']['conversion_fee_amount'].sum()

# mint_yield_count: total mint_yield transactions
mint_yield_count = len(df[df['conversion_type'] == 'mint_yield'])

# mint_yield_volume: total mint_yield volume
mint_yield_volume = df[df['conversion_type'] == 'mint_yield']['to_amount'].sum()

# fees_zyield: where conversion_fee_asset = ZYIELD
fees_zyield = df[df['conversion_fee_asset'] == 'ZYIELD']['conversion_fee_amount'].sum()

# redeem_yield_count: total redeem_yield transactions
redeem_yield_count = len(df[df['conversion_type'] == 'redeem_yield'])

# redeem_yield_volume: total redeem_yield volume
redeem_yield_volume = df[df['conversion_type'] == 'redeem_yield']['from_amount'].sum()

# fees_zephusd_yield: where conversion_fee_asset = ZEPHUSD && conversion type = mint_yield
fees_zephusd_yield = df[(df['conversion_fee_asset'] == 'ZEPHUSD') & (df['conversion_type'] == 'redeem_yield')]['conversion_fee_amount'].sum()

# Print the stats
print("Conversion Transactions:", conversion_transactions)
print("Yield Conversion Transactions:", len(yield_conversion_transactions))
print("Mint Reserve Count:", mint_reserve_count)
print("Mint Reserve Volume:", mint_reserve_volume)
print("Fees (ZEPHRSV):", fees_zephrsv)
print("Redeem Reserve Count:", redeem_reserve_count)
print("Redeem Reserve Volume:", redeem_reserve_volume)
print("Fees (ZEPHUSD for mint_stable):", fees_zephusd)
print("Mint Stable Count:", mint_stable_count)
print("Mint Stable Volume:", mint_stable_volume)
print("Redeem Stable Count:", redeem_stable_count)
print("Redeem Stable Volume:", redeem_stable_volume)
print("Fees (ZEPH):", fees_zeph)
print("Mint Yield Count:", mint_yield_count)
print("Mint Yield Volume:", mint_yield_volume)
print("Fees (ZYIELD):", fees_zyield)
print("Redeem Yield Count:", redeem_yield_count)
print("Redeem Yield Volume:", redeem_yield_volume)
print("Fees (ZEPHUSD for mint_yield):", fees_zephusd_yield)

# Work out expected circ amounts for each asset and what is in the reserves (not including block rewards)

# Expected Circulating Supply = Total Minted - Total Redeemed - Total Fees
ZEPHRSV_Circ = mint_reserve_volume - redeem_reserve_volume - fees_zephrsv
ZEPHUSD_Circ = mint_stable_volume - redeem_stable_volume - fees_zephusd
ZYIELD_Circ = mint_yield_volume - redeem_yield_volume - fees_zyield

# Print the expected circulating supply
print("Expected Circulating Supply (ZEPHRSV):", ZEPHRSV_Circ)
print("Expected Circulating Supply (ZEPHUSD):", ZEPHUSD_Circ)
print("Expected Circulating Supply (ZYIELD):", ZYIELD_Circ)




# Output a bunch of graphs

# Number of conversions per day
df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], unit='s')
df['date'] = df['block_timestamp'].dt.date
conversions_per_day = df['date'].value_counts().sort_index()
plt.figure(figsize=(10, 6))
conversions_per_day.plot(kind='line')
plt.xlabel('Date')
plt.ylabel('Number of Conversions')
plt.title('Number of Conversions per Day')
plt.savefig('conversions_per_day.png')

# Volume of conversions per day
volume_per_day = df.groupby('date')['from_amount'].sum()
plt.figure(figsize=(10, 6))
volume_per_day.plot(kind='line')
plt.xlabel('Date')
plt.ylabel('Volume of Conversions')
plt.title('Volume of Conversions per Day')
plt.savefig('volume_per_day.png')

# Conversion type counts
conversion_type_counts = df['conversion_type'].value_counts()
plt.figure(figsize=(10, 6))
conversion_type_counts.plot(kind='bar')
plt.xlabel('Conversion Type')
plt.ylabel('Count')
plt.title('Count of Each Conversion Type')
plt.savefig('conversion_type_counts.png')

# Fees by asset
fees_by_asset = df.groupby('conversion_fee_asset')['conversion_fee_amount'].sum()
plt.figure(figsize=(10, 6))
fees_by_asset.plot(kind='bar')
plt.xlabel('Fee Asset')
plt.ylabel('Total Fees')
plt.title('Total Fees by Asset')
plt.savefig('fees_by_asset.png')

