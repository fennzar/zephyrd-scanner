import pandas as pd
from pathlib import Path

hf_height = 89300
starting_height = hf_height

df_pricing_records = pd.read_csv(Path("./py/csvs/pricing_records.csv"))
df_txs = pd.read_csv(Path("./py/csvs/txs.csv"))
df_block_rewards = pd.read_csv(Path("./py/csvs/block_rewards.csv"))

#get top height from df_pricing_records (last row)
current_height = int(df_pricing_records.tail(1)['block'].values[0])


reserve_stats = []

reserve = 0 #ZEPH
zephusd_circ = 0
zephrsv_circ = 0
assets = 0
assets_ma = 0
liabilities  = 0
equity = 0
equity_ma = 0
reserve_ratio = 0
reserve_ratio_ma = 0
reserve_ratio_pct = 0
reserve_ratio_ma_pct = 0


print("Start")
print("Going to: ", current_height)

try:
    df_reserve_stats = pd.read_csv(Path("./py/csvs/reserve_stats.csv"))
    print("Reserve stats csv found")
    input = input("continue from existing reserve_stats.csv? (y/n): ").lower()
    if input == "y":
        reserve_stats = df_reserve_stats.values.tolist()
        starting_height = int(reserve_stats[-1][0] + 1)
except Exception as e:
    print("Loading Reserve stats error", e)

for i in range(starting_height, current_height):
    try:
        print("Block: ", i, " of ", current_height)
        #get block reward for this block
        df_current_block_reward = df_block_rewards[df_block_rewards['block'] == i]
        reserve_reward = df_current_block_reward['reserve_reward'].values[0]

        reserve += reserve_reward

        #get txs for this block
        df_current_block_txs = df_txs[df_txs['block'] == i]
        if not df_current_block_txs.empty:
            for index, row in df_current_block_txs.iterrows():
                conversion_type = row['conversion_type']
                from_amount = row['from_amount']
                to_amount = row['to_amount']

                if conversion_type == "mint_stable":
                    zephusd_circ += to_amount
                    reserve += from_amount
                
                elif conversion_type == "mint_reserve":
                    zephrsv_circ += to_amount
                    reserve += from_amount

                elif conversion_type == "redeem_stable":
                    zephusd_circ -= from_amount
                    reserve -= to_amount

                elif conversion_type == "redeem_reserve":
                    zephrsv_circ -= from_amount
                    reserve -= to_amount

        
        #get pricing record for this block
        df_current_block_pricing_record = df_pricing_records[df_pricing_records['block'] == i]
        spot = df_current_block_pricing_record['spot'].values[0]
        moving_average = df_current_block_pricing_record['moving_average'].values[0]


        #set current reserve stats
        assets = reserve * spot
        assets_ma = reserve * moving_average
        liabilities  = zephusd_circ
        equity = assets - liabilities 
        equity_ma = assets_ma - liabilities 

        #calculate reserve ratio
        reserve_ratio = 0
        reserve_ratio_ma = 0
        reserve_ratio_pct = 0
        reserve_ratio_ma_pct = 0
        
        if liabilities  > 0:
            reserve_ratio = assets / liabilities 
            reserve_ratio_ma = assets_ma / liabilities 
            
            reserve_ratio_pct = assets / liabilities  * 100
            reserve_ratio_ma_pct = assets_ma / liabilities  * 100
        

        #add to reserve_stats list

        reserve_stats.append([i, spot, moving_average, reserve, zephusd_circ, zephrsv_circ, assets, assets_ma, liabilities , equity, equity_ma, reserve_ratio, reserve_ratio_ma, reserve_ratio_pct, reserve_ratio_ma_pct])
    except Exception as e:
        print("Error: ", e)
        continue



#convert reserve_stats list to df
df_reserve_stats = pd.DataFrame(reserve_stats, columns=['block', 'spot', 'moving_average','reserve', 'zephusd_circ', 'zephrsv_circ', 'assets', 'assets_ma', 'liabilities', 'equity', 'equity_ma', 'reserve_ratio', 'reserve_ratio_ma', 'reserve_ratio_pct', 'reserve_ratio_ma_pct'])
print(df_reserve_stats)
df_reserve_stats.to_csv(Path("./py/csvs/reserve_stats.csv"), index=False)

print("Done")
print(df_reserve_stats.tail(1).transpose())






            


    




