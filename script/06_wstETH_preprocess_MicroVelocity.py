"""work on data prep for Micro Velocity


"""

import pandas as pd
import os
import numpy as np
from dotenv import load_dotenv
import web3


# Load environment variables from the .env file

load_dotenv()
INPUT = os.getenv("INPUT")
PROCESSED = os.getenv("PROCESSED")
LIDO_ADDRESS=os.getenv("wstETH_ADDRESS")
wei = 10**18 
DEPOSIT_SIZE=32*wei

def merge_parquet_files(folder_path, prefix):
    """
    Merge all Parquet files with a given prefix in a folder into a single file.

    Args:
        folder_path (str): Path to the folder containing Parquet files.
        prefix (str): Prefix to filter the Parquet files.
        output_file (str): Path to save the merged Parquet file.

    Returns:
        pd.DataFrame: The merged DataFrame.
    """
    # Step 1: Find all Parquet files with the given prefix
    parquet_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.startswith(prefix) and f.endswith('.parquet')
    ]
    
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files with prefix '{prefix}' found in folder '{folder_path}'.")

    # Step 2: Load and concatenate all Parquet files
    dataframes = []
    for file in parquet_files:
        # print(f"Loading {file}")
        dataframes.append(pd.read_parquet(file))
    
    merged_df = pd.concat(dataframes, ignore_index=True)
    
    return merged_df


folder_path = f"{INPUT}/wstETH_event"
prefix = f"{LIDO_ADDRESS}-Transfer-"

dfTransfer = merge_parquet_files(folder_path, prefix)
# convert to string hexbyte transaction hash
dfTransfer['transactionHash'] = dfTransfer['transactionHash'].apply(lambda x: x.hex())
# sort by block
dfTransfer = dfTransfer.sort_values(by='blockNumber')
# drop duplicates
#dfTransfer = dfTransfer.drop_duplicates(keep='first') proviamo a vedere
# convert value to int
#dfTransfer['value'] = dfTransfer['value'].apply(int)/wei
#dfTransfer['value'] = dfTransfer['value']

# write to parquet file dfTransfer
est_size_bytes = len(dfTransfer.to_parquet(compression='snappy'))
est_size_mb = est_size_bytes / (1024 * 1024)
print(f"Estimated dfTransfer file size: {est_size_mb:.2f} MB")
dfTransfer.to_parquet(f'{PROCESSED}/pre-process/wstETH-transfer.parquet', compression='snappy')


# PREPROCESSING FOR MICROVELOCITY
# -------------------------------
"""
output:
  - shares-transfers.csv
  - shares-allocated.csv
"""

dfTransfer = dfTransfer.drop(columns=['transactionHash'])

dfTransfer.columns = [
    'block_number',	
    'from_address',
    'to_address',
    'amount'
]

# drop nan value
dfTransfer = dfTransfer.dropna(subset=['amount'])
# save
dfTransfer.to_csv(f'{PROCESSED}/pre-process/wstETH-transfers.csv', index=False)

# Create an empty DataFrame for MicroVelocity premint
df = pd.DataFrame(columns=['block_number', 'to_address', 'amount'])
dfTransfer['amount'] = dfTransfer['amount'].apply(int)
# only premint all minted tokens in the interval
new_row = {
    'block_number': dfTransfer.iloc[0]['block_number'] - 1,
    'to_address': '0x0000000000000000000000000000000000000000',
    'amount': dfTransfer[dfTransfer['from_address']=='0x0000000000000000000000000000000000000000']['amount'].sum()
}
df.loc[len(df)] = new_row
# Save to CSV for MicroVelocity input
df.to_csv(f'{PROCESSED}/pre-process/wstETH-transfer-allocated.csv', index=False)
