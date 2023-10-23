import requests
import json
import pandas as pd
from pathlib import Path

session = requests.Session()

def get_current_block_height():
    
    url = "http://127.0.0.1:17767/get_height"
    
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.post(url, headers=headers)

    response_data = response.json()
    if response_data and "height" in response_data:
        return response_data["height"]
    else:
        return 0

def get_block(height):

    url = "http://127.0.0.1:17767/json_rpc"

    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "jsonrpc": "2.0",
        "id": "0",
        "method": "get_block",
        "params": {"height": height}
    }

    # response = requests.post(url, headers=headers, data=json.dumps(data))
    # response_json = response.json()
    # response.close()
    # return response_json

    try:
        response = session.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


def get_pr_for_block(height):
    response_data = get_block(height)
    if response_data and "result" in response_data and "block_header" in response_data["result"]:
        pricing_record = response_data["result"]["block_header"]["pricing_record"]
        return pricing_record
    else:
        return None

current_height = get_current_block_height()
hf_height = 89300
starting_height = hf_height

pricing_records = []

print("Start")
print("Current Daemon height: ", current_height)
#check if pricing_records.csv exists
try:
    df_pricing_records = pd.read_csv(Path("./py/csvs/pricing_records.csv"))
    print("pricing_records.csv exists")
    input = input("continue from existing pricing_records.csv? (y/n): ").lower()
    if input == "y":
        pricing_records = df_pricing_records.values.tolist()
        starting_height = int(pricing_records[-1][0] + 1)
        print("Starting from block: ", starting_height)
except Exception as e:
    print("pricing_records.csv does not exist or error: ", e)
    
prev_timestamp = 0

for i in range(starting_height, current_height):
    print("Block: ", i, " of ", current_height)
    pricing_record = get_pr_for_block(i)

    if pricing_record:
        block = i
        timestamp = pricing_record["timestamp"] # Unix timestamp

        spot = pricing_record["spot"] * (10**-12)
        moving_average = pricing_record["moving_average"] * (10**-12)
        reserve = pricing_record["reserve"] * (10**-12)
        reserve_ma = pricing_record["reserve_ma"] * (10**-12)
        stable = pricing_record["stable"] * (10**-12)
        stable_ma = pricing_record["stable_ma"] * (10**-12)
        # add to priceing_records list
        pricing_records.append([block, timestamp, spot, moving_average, reserve, reserve_ma, stable, stable_ma])

        # Update prev_timestamp
        prev_timestamp = timestamp
    else:
        pricing_records.append([block, 0, 0, 0, 0, 0, 0, 0])
        print("No pricing record for block: ", i)


df_pricing_records = pd.DataFrame(pricing_records, columns=["block","timestamp", "spot", "moving_average", "reserve", "reserve_ma", "stable", "stable_ma"])
print(df_pricing_records)

df_pricing_records.to_csv(Path("./py/csvs/pricing_records.csv"), index=False)