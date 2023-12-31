import requests
import json
import pandas as pd
from pathlib import Path

session = requests.Session()

df_pricing_records = pd.read_csv(Path("./py/csvs/pricing_records.csv"))


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

    response = requests.post(url, headers=headers, data=json.dumps(data))
    return response.json()

def read_tx(hash, height):
    #global df_pricing_records

    url = "http://127.0.0.1:17767/get_transactions"

    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "txs_hashes": [hash], "decode_as_json": True
    }

    # print(hash)
    # print(json.dumps(data))
    response = session.post(url, headers=headers, data=json.dumps(data))
    # print(response.text)
    response_data = response.json()
    
    # Extract transaction data from the "txs" key
    tx_data = response_data.get("txs", [{}])[0]
    tx_json = tx_data.get("as_json", {})

    # print(tx_json)  # Print the JSON data of the transaction
    tx_json = json.loads(tx_json)


    # Check if the transaction is a conversion transaction
    if tx_json["amount_burnt"] == 0 or tx_json["amount_minted"] == 0:
        tx_amount = tx_json["vout"][0]["amount"]
        if tx_amount > 0:
            # Block Reward
            miner_reward = tx_amount * (10**-12)
            governance_reward = tx_json["vout"][1]["amount"] * (10**-12)
            reserve_reward = (miner_reward / 0.75) * 0.2

            print("\tBlock reward transaction!")
            block_reward_info = [int(height), miner_reward, governance_reward, reserve_reward]
            return None, block_reward_info
        else:
            return None, None  # Not a conversion transaction
    
    # Extract asset types for input and output
    input_asset_type = tx_json["vin"][0]["key"]["asset_type"]
    output_asset_types = [vout["target"]["tagged_key"]["asset_type"] for vout in tx_json["vout"]]
    conversion_type = "na"
    # Determine the conversion type
    if input_asset_type == "ZEPH" and "ZEPHUSD" in output_asset_types:
        conversion_type = "mint_stable"
    elif input_asset_type == "ZEPHUSD" and "ZEPH" in output_asset_types:
        conversion_type = "redeem_stable"
    elif input_asset_type == "ZEPH" and "ZEPHRSV" in output_asset_types:
        conversion_type = "mint_reserve"
    elif input_asset_type == "ZEPHRSV" and "ZEPH" in output_asset_types:
        conversion_type = "redeem_reserve"
    
    if conversion_type != "na":
        amount_burnt = tx_json["amount_burnt"] * (10**-12)
        amount_minted = tx_json["amount_minted"] * (10**-12)
        print(f"Conversion Type: {conversion_type}")
        print(f"Amount Burnt: {amount_burnt}")
        print(f"Amount Minted: {amount_minted}")
    else:
        print("Not a conversion transaction")
        # count this?
        return
    
    
    # Get more info on the tx
    pr_height = tx_json["pricing_record_height"]
    # print(f"Pricing Record Height: {pr_height}")

    relevant_pr = df_pricing_records[df_pricing_records["block"] == pr_height]
    if relevant_pr.empty:
        return
    
    print(relevant_pr)
    spot = relevant_pr["spot"].values[0]
    moving_average = relevant_pr["moving_average"].values[0]
    reserve = relevant_pr["reserve"].values[0]
    reserve_ma = relevant_pr["reserve_ma"].values[0]
    stable = relevant_pr["stable"].values[0]
    stable_ma = relevant_pr["stable_ma"].values[0]

    #determine conversion rate

    # conversion fees are a lost in the mint value. But this fee is not added the the reserve directly... although the difference kind of is.


    conversion_rate = 0
    from_asset = ""
    from_amount = 0
    to_asset = ""
    to_amount = 0
    if conversion_type == "mint_stable":
        conversion_rate = max(spot, moving_average)
        from_asset = "ZEPH"
        from_amount = amount_burnt
        to_asset = "ZEPHUSD"
        to_amount = amount_minted

        #conversion fees        
        conversion_fee_asset = to_asset
        conversion_fee_amount = (amount_minted / 0.98) * 0.02

        tx_fee_asset = from_asset



    elif conversion_type == "redeem_stable":
        conversion_rate = min(spot, moving_average)
        from_asset = "ZEPHUSD"
        from_amount = amount_burnt
        to_asset = "ZEPH"
        to_amount = amount_minted

        #conversion fees        
        conversion_fee_asset = to_asset
        conversion_fee_amount = (amount_minted / 0.98) * 0.02

        tx_fee_asset = from_asset

    elif conversion_type == "mint_reserve":
        #NO FEE
        conversion_rate = max(reserve, reserve_ma)
        from_asset = "ZEPH"
        from_amount = amount_burnt
        to_asset = "ZEPHRSV"
        to_amount = amount_minted

        #conversion fees        
        conversion_fee_asset = "N/A"
        conversion_fee_amount = 0
        
        tx_fee_asset = from_asset


    elif conversion_type == "redeem_reserve":
        conversion_rate = min(reserve, reserve_ma)
        from_asset = "ZEPHRSV"
        from_amount = amount_burnt
        to_asset = "ZEPH"
        to_amount = amount_minted

        #conversion fees
        conversion_fee_asset = to_asset
        conversion_fee_amount = (amount_minted / 0.98) * 0.02

        tx_fee_asset = from_asset



    tx_fee_amount = tx_json["rct_signatures"]["txnFee"] * (10**-12)


    tx_info = [hash, conversion_type, conversion_rate, from_asset, from_amount, to_asset, to_amount, conversion_fee_asset, conversion_fee_amount, tx_fee_asset, tx_fee_amount]
    return tx_info, None


def process_tx_per_block(height):
    response_data = get_block(height)
    if response_data and "result" in response_data:
        block_data = response_data["result"]
        timestamp = block_data["block_header"]["timestamp"]
        tx_hashes = block_data.get("tx_hashes", [])
        miner_tx = block_data["miner_tx_hash"]
        _, block_reward_info = read_tx(miner_tx, height) # only do this when setting HF height to 89300
        if block_reward_info and height >= block_reward_height_start:
            block_rewards.append(block_reward_info)
        for hash in tx_hashes:
            tx_info,_ = read_tx(hash, height)
            if tx_info:
                tx_info.append(timestamp)
                tx_info.append(height)
                tx_info = [timestamp, height, *tx_info]
                txs.append(tx_info)
    else:
        return None


txs = []
block_rewards = []

current_height = get_current_block_height()
hf_height = 89300
starting_height = hf_height
block_reward_height_start = starting_height


try:
    df_txs = pd.read_csv(Path("./py/csvs/txs.csv"))
    print("txs.csv exists")
    input = input("continue from existing txs.csv and block_rewards.csv (y/n): ").lower()
    if input == "y":
        txs = df_txs.values.tolist()
        starting_height = int(txs[-1][1] + 1)
        print("Starting from block: ", starting_height)
        block_rewards = pd.read_csv(Path("./py/csvs/block_rewards.csv")).values.tolist()
        block_reward_height_start = int(block_rewards[-1][0] + 1)
except Exception as e:
    print("txs.csv does not exist or error: ", e)

print("Start")
print("Current Daemon height: ", current_height)
for i in range(starting_height, current_height):
    print("Block: ", i, " of ", current_height)
    process_tx_per_block(i)

# print(txs)
df_txs = pd.DataFrame(txs, columns=["timestamp", "block", "hash", "conversion_type", "conversion_rate", "from_asset", "from_amount", "to_asset", "to_amount", "conversion_fee_asset", "conversion_fee_amount", "tx_fee_asset", "tx_fee_amount", "timestamp", "block"])
print(df_txs)
df_txs.to_csv(Path("./py/csvs/txs.csv"), index=False)

df_block_rewards = pd.DataFrame(block_rewards, columns=["block", "miner_reward", "governance_reward", "reserve_reward"])
print("saving block rewards")
df_block_rewards.to_csv(Path("./py/csvs/block_rewards.csv"), index=False)

