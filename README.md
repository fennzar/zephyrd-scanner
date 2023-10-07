# zephyrdscanner

Tool for scanning the Zephyr Blockchain specificially for tracking conversion transactions and stablecoin protocol.

NOTE - WIP

## Node

`npm run start`

Uses redis to store information.
Polls every minute to check for new blocks/transactions.

Keeps track of:

- Pricing Records (Asset prices over time recorded in each block)
- Conversion Transactions
- Block Rewards (Miner Reward, Reserve Rewards and Governance Reward)
- Totals
  - Block Rewards
  - Number of Conversion Transactions (and of each type)
  - Amount of Zeph/ZephUSD/ZephRSV converted (volume)
  - Fees generated from conversions

## Python

Was used as a prototype for the node version, but has graphing functionality.
Uses CSV files as apposed to redis at the moment.

Run `prscan.py` first to generate the pricing records CSV file.
Run `graph.py` to generate graphs from the CSV files.

Run `txscan.py` to generate the transactions CSV file
Run `txstats.py` to generate stats from the transactions CSV file
