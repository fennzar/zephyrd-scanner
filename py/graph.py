import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Load the data from the CSV
df_pricing_records = pd.read_csv(Path("./py/csvs/pricing_records.csv"))

# clear rows where spot == 0
# df_pricing_records = df_pricing_records[df_pricing_records.spot != 0]


################## GRAPH 1 ##################

metric_sets = [
    ["spot", "moving_average"],
    ["reserve", "reserve_ma"],
    ["stable", "stable_ma"]
]

metric_labels = [
    ["ZEPH", "ZEPH MA"],
    ["ZRS (Reserve Share)", "ZRS (Reserve Share) MA"],
    ["ZSD (Stable Dollar)", "ZSD (Stable Dollar) MA"]

]

# Plot each set of metrics
# for metrics in metric_sets:
# for i in range(len(metric_sets)):
#     plt.figure(figsize=(15, 6))
#     for metric in metric_sets[i]:
#         plt.plot(df_pricing_records["block"], df_pricing_records[metric], label=metric)
    
#     plt.title(f'{metric_labels[i][0]} and {metric_labels[i][1]} over Block Height')
#     plt.xlabel('Block Height')
#     plt.ylabel('Value')
#     plt.legend(loc='best')
#     plt.grid(True)
#     plt.tight_layout()

#     # Save the figure (optional)
#     plt.savefig(f'{metric_labels[i][0]}_and_{metric_labels[i][1]}_over_block_height.png')

#     # Show the plot
#     # plt.show()

################## GRAPH 1 ##################

# Replace zeros with NaNs to break the plot lines
df_pricing_records.replace(0, np.nan, inplace=True)

# Create a new column to help identify the continuous blocks of zeros
df_pricing_records['zero_flag'] = df_pricing_records['spot'].isna()

# Identify the start and end blocks of the missing data
start_blocks = df_pricing_records.loc[df_pricing_records['zero_flag'] & ~df_pricing_records['zero_flag'].shift(1, fill_value=False), 'block']
end_blocks = df_pricing_records.loc[df_pricing_records['zero_flag'] & ~df_pricing_records['zero_flag'].shift(-1, fill_value=False), 'block']

for start, end in zip(start_blocks, end_blocks):
    # print(f"Start: {start}, End: {end}")
    if start != end:
        print(f"Start: {start}, End: {end}")
        start_idx = df_pricing_records[df_pricing_records['block']==start-1].index[0]
        # start_spot = df_pricing_records.at[start_idx, 'spot']
        start_ma = df_pricing_records.at[start_idx, 'moving_average']
        # start_reserve = df_pricing_records.at[start_idx, 'reserve']
        start_reserve_ma = df_pricing_records.at[start_idx, 'reserve_ma']
        # start_stable = df_pricing_records.at[start_idx, 'stable']
        start_stable_ma = df_pricing_records.at[start_idx, 'stable_ma']
        
        end_idx = df_pricing_records[df_pricing_records['block']==end+1].index[0]
        # end_spot = df_pricing_records.at[end_idx, 'spot']
        end_ma = df_pricing_records.at[end_idx, 'moving_average']
        # end_reserve = df_pricing_records.at[end_idx, 'reserve']
        end_reserve_ma = df_pricing_records.at[end_idx, 'reserve_ma']
        # end_stable = df_pricing_records.at[end_idx, 'stable']
        end_stable_ma = df_pricing_records.at[end_idx, 'stable_ma']

        # diff_spot = end_spot - start_spot
        diff_ma = end_ma - start_ma
        # diff_reserve = end_reserve - start_reserve
        diff_reserve_ma = end_reserve_ma - start_reserve_ma
        # diff_stable = end_stable - start_stable
        diff_stable_ma = end_stable_ma - start_stable_ma

        # print(f"diff_spot: {diff_spot} | diff_ma: {diff_ma} | diff_reserve: {diff_reserve} | diff_reserve_ma: {diff_reserve_ma} | diff_stable: {diff_stable} | diff_stable_ma: {diff_stable_ma}")
        print(f"diff_ma: {diff_ma} | diff_reserve_ma: {diff_reserve_ma} | diff_stable_ma: {diff_stable_ma}")

        
        total_blocks = end - start + 1

        for i in range(start + 1, end):
            # print(f"i: {i}")
            idx = df_pricing_records[df_pricing_records['block'] == i].index[0]

            # df_pricing_records.at[idx, 'spot'] = start_spot + (diff_spot / total_blocks) * (i - start)
            df_pricing_records.at[idx, 'moving_average'] = start_ma + (diff_ma / total_blocks) * (i - start)
            # df_pricing_records.at[idx, 'reserve'] = start_reserve + (diff_reserve / total_blocks) * (i - start)
            df_pricing_records.at[idx, 'reserve_ma'] = start_reserve_ma + (diff_reserve_ma / total_blocks) * (i - start)
            # df_pricing_records.at[idx, 'stable'] = start_stable + (diff_stable / total_blocks) * (i - start)
            df_pricing_records.at[idx, 'stable_ma'] = start_stable_ma + (diff_stable_ma / total_blocks) * (i - start)

# print(df_pricing_records.at[5450, 'spot'])

# Plotting
for i in range(len(metric_sets)):
    plt.figure(figsize=(15, 6))
    for metric in metric_sets[i]:
        plt.plot(df_pricing_records["block"], df_pricing_records[metric], label=metric)
    
    outage_label_added = False  # Flag to check if the "Outage" label has been added
    
    # # Shade the regions for outages
    for start, end in zip(start_blocks, end_blocks):
        label = 'Outage' if not outage_label_added else ""
        if start != end:
            plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
            if not outage_label_added:
                outage_label_added = True  # Mark the flag as True after adding the label once

    plt.title(f'{metric_labels[i][0]} and {metric_labels[i][1]} over Block Height')
    plt.xlabel('Block Height')
    plt.ylabel('Value')
    plt.legend(loc='best')
    plt.grid(True)
    plt.tight_layout()

    # Save the figure (optional)
    figname = f'{metric_labels[i][0]}_and_{metric_labels[i][1]}_over_block_height.png'
    plt.savefig(Path(f'./py/graphs/{figname}'))

    # Show the plot
    # plt.show()

################## GRAPH 2 ##################

# Calculate the percentage change for 'spot' and 'reserve' compared to their initial values
df_pricing_records['spot_pct_change'] = (df_pricing_records['spot'] - df_pricing_records['spot'].iloc[0]) / df_pricing_records['spot'].iloc[0] * 100
df_pricing_records['reserve_pct_change'] = (df_pricing_records['reserve'] - df_pricing_records['reserve'].iloc[0]) / df_pricing_records['reserve'].iloc[0] * 100

# Plot the percentage changes
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['spot_pct_change'], label='ZEPH % Change')
plt.plot(df_pricing_records['block'], df_pricing_records['reserve_pct_change'], label='ZephRSV % Change')

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once

plt.title('Percentage Change of Spot vs Reserve over Block Height')
plt.xlabel('Block Height')
plt.ylabel('Percentage Change')
plt.legend(loc='best')
plt.grid(True)
plt.tight_layout()

# Save the figure (optional)
plt.savefig(Path('./py/graphs/percentage_change_ZEPH_vs_ZephRSV.png'))

# Show the plot
# plt.show()

################## GRAPH 2 ##################

# List of metrics and their corresponding percentage change columns
metrics = ['spot', 'moving_average', 'reserve', 'reserve_ma']
pct_change_cols = [f"{metric}_pct_change" for metric in metrics]

# Calculate the percentage change for each metric compared to its initial value
for metric, pct_change_col in zip(metrics, pct_change_cols):
    df_pricing_records[pct_change_col] = (df_pricing_records[metric] - df_pricing_records[metric].iloc[0]) / df_pricing_records[metric].iloc[0] * 100

# Plot the percentage changes
plt.figure(figsize=(15, 6))
for pct_change_col, metric in zip(pct_change_cols, metrics):
    if metric == 'spot' or metric == 'reserve':
        plt.plot(df_pricing_records['block'], df_pricing_records[pct_change_col], label=f'{metric} % Change', alpha=0.3)
    else:
        plt.plot(df_pricing_records['block'], df_pricing_records[pct_change_col], label=f'{metric} % Change')


outage_label_added = False  # Flag to check if the "Outage" label has been added
    
# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once

plt.title('Percentage Change of Metrics over Block Height')
plt.xlabel('Block Height')
plt.ylabel('Percentage Change')
plt.legend(loc='best')
plt.grid(True)
plt.tight_layout()

# Save the figure (optional)
plt.savefig(Path('./py/graphs/percentage_change_of_metrics.png'))

# Show the plot
# plt.show()

#################

for i in range(len(df_pricing_records)):
    reserve_spot_in_usd = df_pricing_records.at[i, "reserve"] * df_pricing_records.at[i, "spot"]
    df_pricing_records.at[i, "reserve_spot_in_usd"] = reserve_spot_in_usd


# Calculate the percentage change for 'spot' and 'reserve' compared to their initial values
df_pricing_records['spot_pct_change'] = (df_pricing_records['spot'] - df_pricing_records['spot'].iloc[0]) / df_pricing_records['spot'].iloc[0] * 100
df_pricing_records['reserve_pct_change'] = (df_pricing_records['reserve_spot_in_usd'] - df_pricing_records['reserve_spot_in_usd'].iloc[0]) / df_pricing_records['reserve_spot_in_usd'].iloc[0] * 100

# Plot the percentage changes
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['spot_pct_change'], label='ZEPH % Change')
plt.plot(df_pricing_records['block'], df_pricing_records['reserve_pct_change'], label='ZephRSV % Change')

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once

plt.title('Percentage Change of Zeph vs Reserve (in USD) over Block Height')
plt.xlabel('Block Height')
plt.ylabel('Percentage Change')
plt.legend(loc='best')
plt.grid(True)
plt.tight_layout()

# Save the figure (optional)
plt.savefig(Path('./py/graphs/percentage_change_ZEPH_vs_ZephRSV_in_USD.png'))

# Show the plot
# plt.show()


# Add a new column for ZephRSV in USD
df_pricing_records['reserve_in_usd'] = df_pricing_records['reserve'] * df_pricing_records['spot']
df_pricing_records['reserve_in_usd_ma'] = df_pricing_records['reserve_ma'] * df_pricing_records['moving_average']


# Calculate what an initial $10,000 investment in ZephRSV would be worth over time
initial_investment = 10000  # $10,000
initial_reserve_spot_usd = df_pricing_records.at[0, 'reserve_in_usd']  # Initial 'spot' value
initial_investment_in_reserve = initial_investment / initial_reserve_spot_usd  # Amount of ZephRSV bought
print(f"Initial Investment: ${initial_investment}")
print(f"Initial Reserve Spot: {initial_reserve_spot_usd}")
print(f"Initial Investment in Reserve Coins: {initial_investment_in_reserve}")

# Create a new column to store the value of the investment over time
df_pricing_records['zephrsv_investment_in_usd'] = df_pricing_records['reserve_in_usd'] * initial_investment_in_reserve
df_pricing_records['zephrsv_investment_in_usd_ma'] = df_pricing_records['reserve_in_usd_ma'] * initial_investment_in_reserve


#If we invested 10,000 into Zeph 
initial_zeph_spot = df_pricing_records.at[0, 'spot']  # Initial 'spot' value
initial_investment_in_zeph = initial_investment / initial_zeph_spot  # Amount of Zeph bought

df_pricing_records['zeph_investment_in_usd'] = df_pricing_records['spot'] * initial_investment_in_zeph
df_pricing_records['zeph_investment_in_usd_ma'] = df_pricing_records['moving_average'] * initial_investment_in_zeph


################## ADDITIONAL GRAPH 1 ##################
# Plot ZephRSV (in Zeph) vs ZephRSV (in USD)
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['reserve_in_usd'], label='ZephRSV in USD')
plt.plot(df_pricing_records['block'], df_pricing_records['reserve_in_usd_ma'], label='ZephRSV in USD (MA)')
plt.xlabel('Block Height')
plt.ylabel('Value')
plt.title('ZephRSV (in USD)')
plt.legend(loc='best')

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once

plt.grid(True)
plt.tight_layout()
plt.savefig(Path('./py/graphs/ZephRSV_in_USD_spotvsma.png'))
# plt.show()

################## ADDITIONAL GRAPH 2 ##################
# Plot the value of an initial $10,000 investment in ZephRSV over time
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['zephrsv_investment_in_usd'], label='$10,000 Investment in ZephRSV')
plt.plot(df_pricing_records['block'], df_pricing_records['zephrsv_investment_in_usd_ma'], label='$10,000 Investment in ZephRSV (MA)')
plt.xlabel('Block Height')
plt.ylabel('Investment Value in USD')
plt.title('Value of a $10,000 Investment in ZephRSV Over Time')
plt.legend(loc='best')

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once

plt.grid(True)
plt.tight_layout()
plt.savefig(Path('./py/graphs/Investment_in_ZephRSV_over_time.png'))
# plt.show()

################## ADDITIONAL GRAPH 3 ##################
# Plot the value of an initial $10,000 investment in ZephRSV over time
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['zeph_investment_in_usd_ma'], label='$10,000 Investment in Zeph (MA)')
plt.plot(df_pricing_records['block'], df_pricing_records['zephrsv_investment_in_usd_ma'], label='$10,000 Investment in ZephRSV (MA)')
plt.xlabel('Block Height')
plt.ylabel('Investment Value in USD')
plt.title('Value of a $10,000 Investment in ZephRSV vs ZEPH Over Time')
plt.legend(loc='best')
plt.grid(True)

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once
plt.tight_layout()
plt.savefig(Path('./py/graphs/Investment_in_ZephRSV_vs_ZEPH_over_time.png'))
# plt.show()

################## ADDITIONAL GRAPH 4 ##################
# Plot ZephRSV (in Zeph) vs ZephRSV (in USD)
plt.figure(figsize=(15, 6))
plt.plot(df_pricing_records['block'], df_pricing_records['reserve_in_usd_ma'], label='ZephRSV in USD (MA)')
plt.plot(df_pricing_records['block'], df_pricing_records['moving_average'], label='ZEPH in USD (MA)')
plt.xlabel('Block Height')
plt.ylabel('Value')
plt.title('ZephRSV in USD')
plt.legend(loc='best')
plt.grid(True)

outage_label_added = False  # Flag to check if the "Outage" label has been added

# Shade the regions for outages
for start, end in zip(start_blocks, end_blocks):
    label = 'Outage' if not outage_label_added else ""
    if start != end:
        plt.axvspan(start, end, color='gray', alpha=0.3, label=label)
        if not outage_label_added:
            outage_label_added = True  # Mark the flag as True after adding the label once
plt.tight_layout()
plt.savefig(Path('./py/graphs/ZephRSV_in_USD_vs_ZEPH_mas.png'))
# plt.show()

df_pricing_records.to_csv(Path("./py/csvs/pricing_records_with_graphing_additions.csv"), index=False)
# save updated df_pricing_records to csv
df_reserve_stats = pd.read_csv(Path("./py/csvs/reserve_stats.csv"))

################## ADDITIONAL GRAPH 4 ##################
# Plot reserve_ratio and reserve_ratio_ma over block

plt.figure(figsize=(15, 6))
plt.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_pct'], label='Reserve Ratio')
plt.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_ma_pct'], label='Reserve Ratio (MA)')

#y axis range 0->4000
plt.ylim(0, 3000)
lastest_block = df_reserve_stats['block'].iloc[-1]
plt.xlim(89300, lastest_block)
#draw horizontal lines at 4 and 8

y1 = 400
y2 = 800

plt.axhline(y=y1, color='green', linestyle='--')
plt.axhline(y=y2, color='purple', linestyle='--')
# x_text_pos = df_reserve_stats['block'].median() 
x_text_pos = 90000

# Adding text directly on the graph
# You can adjust the x-coordinate (here it's set as a fraction of xlim) and the vertical offset to position the text as needed
plt.text(x_text_pos, y1-20, 'Minimum Ratio - No ZSD Minting/ZRS Redeeming', color='black', verticalalignment='top')
plt.text(x_text_pos, y2+20, 'Maximum Ratio - No Additional ZRS Minting', color='black', verticalalignment='bottom')

plt.xlabel('Block Height')
plt.ylabel('Reserve Ratio %')
plt.title('Reserve Ratio')
plt.legend(loc='best')
plt.grid(True)

plt.tight_layout
plt.savefig(Path('./py/graphs/Reserve_Ratio.png'))


#### VARIATION 1

# plt.figure(figsize=(15, 6))

# # Create the main axis
# ax1 = plt.gca()  # gets the current axis

# # Create the secondary y-axis for liabilities and assets
# ax2 = ax1.twinx()

# # Plot and fill for liabilities
# ax2.plot(df_reserve_stats['block'], df_reserve_stats['liabilities'], color='blue', alpha=0.3, zorder=1)
# ax2.fill_between(df_reserve_stats['block'], df_reserve_stats['liabilities'], color='blue', alpha=0.15, label='Liabilities (ZSD Circ.)', hatch='//', zorder=1)

# # Plot and fill for assets on top of liabilities
# ax2.plot(df_reserve_stats['block'], df_reserve_stats['assets'] + df_reserve_stats['liabilities'], color='green', alpha=0.3, zorder=1)
# ax2.fill_between(df_reserve_stats['block'], df_reserve_stats['liabilities'], df_reserve_stats['assets'] + df_reserve_stats['liabilities'], color='green', alpha=0.15, label='Assets (Zeph in Reserve * Price)', hatch='//', zorder=1)

# ax2.set_ylabel('Liabilities & Assets $', color='black')
# ax2.tick_params(axis='y', labelcolor='black')
# ax2.legend(loc='upper right')
# ax2_ylim = ax2.get_ylim()[1] * 1.1
# ax2.set_ylim(0, ax2_ylim)

# ax1.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_pct'], label='Reserve Ratio', zorder=3)
# ax1.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_ma_pct'], label='Reserve Ratio (MA)', zorder=3)

# # Set limits and labels for the primary y-axis
# ax1.set_ylim(0, 3000)
# ax1.set_ylabel('Reserve Ratio %')
# ax1.legend(loc='upper left')
# ax1.grid(True, zorder=2)  # Grid with a higher z-order to be on top of ax2 but below ax1 lines

# # Draw the horizontal lines and text annotations
# y1 = 400
# y2 = 800
# ax1.axhline(y=y1, color='green', linestyle='--', zorder=3)
# ax1.axhline(y=y2, color='purple', linestyle='--', zorder=3)
# x_text_pos = 90000
# ax1.text(x_text_pos, y1-20, 'Minimum Ratio - No ZSD Minting/ZRS Redeeming', color='black', verticalalignment='top')
# ax1.text(x_text_pos, y2+20, 'Maximum Ratio - No Additional ZRS Minting', color='black', verticalalignment='bottom')

# # Set x-axis and title
# plt.xlabel('Block Height')
# plt.title('Reserve Ratio with Assets and Liabilities Overlay')

# plt.tight_layout()
# plt.savefig(Path('./py/graphs/Reserve_Ratio_Assets_Liabilities_Overlay.png'))


### variation 2

plt.figure(figsize=(15, 6))

# Create the main axis for liabilities and assets
ax1 = plt.gca()  # gets the current axis

# Plot and fill for liabilities
ax1.plot(df_reserve_stats['block'], df_reserve_stats['liabilities'], color='blue', alpha=0.3)
ax1.fill_between(df_reserve_stats['block'], df_reserve_stats['liabilities'], color='blue', alpha=0.15, label='Liabilities (ZSD Circ.)', hatch='//')

# Plot and fill for assets on top of liabilities
ax1.plot(df_reserve_stats['block'], df_reserve_stats['assets'] + df_reserve_stats['liabilities'], color='green', alpha=0.3)
ax1.fill_between(df_reserve_stats['block'], df_reserve_stats['liabilities'], df_reserve_stats['assets'] + df_reserve_stats['liabilities'], color='green', alpha=0.15, label='Assets (Zeph in Reserve * Price)', hatch='//')

ax1.set_ylabel('Liabilities & Assets $', color='black')
ax1.tick_params(axis='y', labelcolor='black')
ax1.legend(loc='upper center')
ax1_ylim = ax1.get_ylim()[1] * 1.1
ax1.set_ylim(0, ax1_ylim)

# Create the secondary y-axis for reserve ratios
ax2 = ax1.twinx()

ax2.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_pct'], label='Reserve Ratio', color='orange')
ax2.plot(df_reserve_stats['block'], df_reserve_stats['reserve_ratio_ma_pct'], label='Reserve Ratio (MA)', color='red')

# Set limits and labels for the secondary y-axis
ax2.set_ylim(0, 3000)
ax2.set_ylabel('Reserve Ratio %', color='black')
ax2.tick_params(axis='y', labelcolor='black')
ax2.legend(loc='upper right')
ax2.grid(True)

# Draw the horizontal lines and text annotations
y1 = 400
y2 = 800
ax2.axhline(y=y1, color='green', linestyle='--')
ax2.axhline(y=y2, color='purple', linestyle='--')
x_text_pos = 90000
ax2.text(x_text_pos, y1-20, 'Minimum Ratio - No ZSD Minting/ZRS Redeeming', color='black', verticalalignment='top')
ax2.text(x_text_pos, y2+20, 'Maximum Ratio - No Additional ZRS Minting', color='black', verticalalignment='bottom')

# Set x-axis and title
plt.xlabel('Block Height')
plt.title('Reserve Ratio with Assets and Liabilities Overlay')
lastest_block = df_reserve_stats['block'].iloc[-1]
plt.xlim(89300, lastest_block)
plt.tight_layout()
plt.savefig(Path('./py/graphs/Reserve_Ratio_Assets_Liabilities_Overlay.png'))





# Plot zephusd_circ over block
plt.figure(figsize=(15, 6))
plt.plot(df_reserve_stats['block'], df_reserve_stats['zephusd_circ'], label='ZSD Circulation')
plt.xlabel('Block Height')

#y axis range 0->4000
# plt.ylim(0, 400000)
lastest_block = df_reserve_stats['block'].iloc[-1]
plt.xlim(89300, lastest_block)

plt.ylabel('ZSD Circulation')
plt.title('ZSD Circulation')
plt.legend(loc='best')
plt.grid(True)

plt.tight_layout
plt.savefig(Path('./py/graphs/ZSD_Circulation.png'))


##################

plt.figure(figsize=(15, 6))

# Main axis for ZRS price in ZEPH
ax1 = plt.gca()
ax1.plot(df_pricing_records['block'], df_pricing_records['reserve'], color='blue', label='ZRS Price in ZEPH')
ax1.set_xlabel('Block Height')
ax1.set_ylabel('ZRS Price in ZEPH', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')
ax1.legend(loc='upper left')

# Secondary axis for ZEPH price
ax2 = ax1.twinx()
ax2.plot(df_pricing_records['block'], df_pricing_records['spot'], color='green', label='ZEPH Price')
ax2.set_ylabel('ZEPH Price', color='green')
ax2.tick_params(axis='y', labelcolor='green')
ax2.legend(loc='upper right')

plt.title('ZRS Price in ZEPH and ZEPH Price over Block Height')
plt.grid(True)
plt.tight_layout()

# Save the figure (optional)
plt.savefig(Path('./py/graphs/ZRS_Price_ZEPH_and_ZEPH_Price.png'))

# Show the plot
plt.show()


# PLOT THEM
plt.show()