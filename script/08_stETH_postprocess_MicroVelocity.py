"""Convert Transfer to TransferShares dataset.

This function postprocess the MicroVelocity data.
As output:

- Wallets by size
- Whale (>= 10k stETH)
- Orca (3-10k)
- Dolphin (1-3k)
- Fish (100-1000)
- Shrimp (10-100)
- Krill (1-10)
- Plankton (<1)
- Total number of wallets
"""

import argparse
import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
import pickle as pkl
from tqdm import tqdm
from web3 import Web3
from datetime import datetime

# Argument parsing
parser = argparse.ArgumentParser(description='Process MicroVelocity data.')
parser.add_argument('--input', type=str, required=True, help='Input file to load velocities')
parser.add_argument('--output', type=str, required=True, help='Output file to save the processed dataframe')
parser.add_argument('--nblocks', type=int, required=True, help='number of blocks interevent')
args = parser.parse_args()

load_dotenv()
INPUT = os.getenv("INPUT")
PROCESSED = os.getenv("PROCESSED")
LIDO_ADDRESS=os.getenv("LIDO_ADDRESS")
RPC = os.getenv("RPC_REMOTE")
OUTPUT=os.getenv("OUTPUT")
wei=1e18
DEPOSIT_SIZE=32

#DEBUG
# args = argparse.Namespace(
#     input=f'{PROCESSED}/general_velocities-7200.pickle',
#     output=f'{OUTPUT}/stETH-money-velocity-7200',
#     nblocks=7200
# )
    
def category(x):
    if x >= 10000:
        return 'Whale'
    if (x < 10000) and (x >= 3000):
        return 'Orca'
    if (x >= 1000) and (x < 3000):
        return 'Dolphin'
    if (x >= 100) and (x < 1000):
        return 'Fish'
    if (x >= 10) and (x < 100):
        return 'Shrimp'
    if (x >= 1) and (x < 10):
        return 'Krill'
    if (x < 1):
        return 'Plankton'
    else:
        return False

# load transfer in shares
df = pd.read_csv(f"{PROCESSED}/pre-process/stETH-shares-transfers.csv")
# load velocities
with open(args.input, 'rb') as pfile:
    backup_accounts, velocities, balances = pkl.load(pfile)

# with open(f'{PROCESSED}/general_velocities-7200.pickle', 'rb') as pfile:
#     backup_accounts, velocities, balances = pkl.load(pfile)

# CLEAN VELOCITIES
# -------------
removed_addr = []
# remove minting
address0 = '0x0000000000000000000000000000000000000000'
removed_addr.append(address0)
del backup_accounts[address0]
# from velocity because is negative
del velocities[address0]
# from balances because is negative
del balances[address0]

# Allow small negative balances due to rounding errors
NEGATIVE_TOLERANCE = -1e-8  # You can adjust this threshold

neg_addr = []
for addr in list(balances.keys()):
    # If all values >= tolerance, clip small negatives to zero
    if (balances[addr] >= NEGATIVE_TOLERANCE).all():
        balances[addr] = np.where(balances[addr] < 0, 0, balances[addr])
    else:
        # If any value is significantly negative, remove address
        neg_addr.append(addr)
        del balances[addr]
        if addr in velocities:
            del velocities[addr]
        removed_addr.append(addr)

# determines block range and block steps
block_min  = df['block_number'].min()
block_max = df['block_number'].max()
NBLOCKS = args.nblocks
block_range = range(block_min, block_max, NBLOCKS)
n = len(block_range)

# set user categories for addresses
# ---------------------------------
# for each address compute total received
df['amount'] = df['amount'].apply(int)/ wei

received = df.groupby('to_address')['amount'].sum()
# classify in categories
categories = received.apply(category)
# convert to dataframe
rec = pd.DataFrame(received)
rec['category'] = categories
rec = rec.reset_index(names='address')
# checksum
rec['address'] = rec['address'].apply(lambda x: x.lower())
# save
rec.to_parquet(f'{OUTPUT}/stETH-categories.parquet')
# nice to have as a series for fast indexing trough addresses
categories = pd.Series(data=rec['category'].values, index=rec['address'], name='category')
# useful for final check
keys = [
    'Whale', # (>= 10k stETH)
    'Orca',  # (3-10k)
    'Dolphin', # (1-3k)
    'Fish', # (100-1000)
    'Shrimp', # (10-100)
    'Krill', # (1-10)
    'Plankton', # (<1)
    'high', # >= 100 stETH (from Whale to Fish)
    'low',  # < 100 stETH (from Shrimp to Plankton)
    'total' # all
]
# additional classification
high = [
    'Whale', # (>= 10k stETH)
    'Orca',  # (3-10k)
    'Dolphin', # (1-3k)
    'Fish' # (100-1000)
]
low = [
    'Shrimp', # (10-100)
    'Krill', # (1-10)
    'Plankton' # (<1)
]

# Compute Velocity
# ----------------
# identify addresses
addresses = set(categories.index).intersection(set(velocities.keys()))
# init for each class
MV = {}
M = {}
V = {}
for k in keys:
    MV[k] = np.zeros(n)
    M[k] = np.zeros(n)
    V[k] = np.zeros(n)

for i in tqdm(range(n), desc="Velocity: processing block batches"):
    for key in addresses:
        if velocities[key][i] > 0:
            MV['total'][i] += velocities[key][i] * balances[key][i]
            M['total'][i] += balances[key][i]
            k = categories[key]
            MV[k][i] += velocities[key][i] * balances[key][i]
            M[k][i] += balances[key][i]
            if k in high:
                MV['high'][i] += velocities[key][i] * balances[key][i]
            elif k in low:
                MV['low'][i] += velocities[key][i] * balances[key][i]
    for k in keys:
        V[k][i] = MV[k][i]/M['total'][i]

# Compute Transaction Volume at the end of each block batch.
# ---------------------------------------------------------
PQ = np.zeros(n)
# Ensure the DataFrame is sorted by block number for efficient operations
df = df.sort_values(by='block_number')
# drop minting transactions
df = df[df['from_address'] != address0]
# Precompute cumulative sums for the 'amount' column
df['cumulative_amount'] = df['amount'].cumsum()
# Convert the block numbers to arrays for efficient indexing
block_numbers = df['block_number'].to_numpy()
cumulative_amounts = df['cumulative_amount'].to_numpy()
# Process each block batch
for i, block in enumerate(tqdm(block_range, desc="PQ: processing block batches")):
    prev_block = block - NBLOCKS
    # Find indices for the start and end of the range
    start_idx = np.searchsorted(block_numbers, prev_block, side='right')
    end_idx = np.searchsorted(block_numbers, block, side='left')
    # Compute the sum for the range using cumulative sums
    PQ[i] = cumulative_amounts[end_idx - 1] - (cumulative_amounts[start_idx - 1] if start_idx > 0 else 0)

# Convert block numbers to timestamps (and dates)
# -----------------------------------------------
# init the Ethereum node rpc provider
web3 = Web3(Web3.HTTPProvider(RPC))
dates = []
for block in tqdm(block_range, desc="Timestamp Conversion: Processing blocks"):
    block_data = web3.eth.get_block(block)
    timestamp = block_data['timestamp']  # Unix timestamp
    date = datetime.utcfromtimestamp(timestamp)  # Convert to UTC datetime
    dates.append(date)

# save processed output
# ---------------------

output_dict = {
    'timestamp':dates,
    'blockNumber': list(block_range),
    'PQ_total': PQ
}
for k in keys:
    output_dict = output_dict | {f"V_{k}":V[k] }
    output_dict = output_dict | {f"M_{k}":M[k] }
    output_dict = output_dict | {f"MV_{k}":MV[k]}

output = pd.DataFrame(output_dict)
output.to_parquet(args.output, compression='snappy')