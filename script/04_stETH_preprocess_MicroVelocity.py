"""Convert Transfer to TransferShares dataset.

This function preprocess the data to run the MicroVelocity module.
As output:
- it creates a DataFrame with all stETH Transfer in shares and stETH token value, 
- a dataframe with the variables values and conversion rate
- and two csv to run the microvelocity.
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
LIDO_ADDRESS=os.getenv("LIDO_ADDRESS")
wei=1e18
DEPOSIT_SIZE=32

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

# MAIN

# COMPUTE CONVERSION RATE AND SAVE 
# --------------------------------
# output: stETH-TransferShares.parquet

# 1. load all variables and merge in a dataframe
storage_keys = [
    "beaconBalance",       # in wei
    "depositedValidators", # in number of validators
    "beaconValidators", # in number of validators
    "bufferedEther",       # in wei
    "totalShares"        # in number of shares
]

folder_path = f"{INPUT}/stETH_variable"

df = merge_parquet_files(folder_path, prefix=f"{LIDO_ADDRESS}-{storage_keys[0]}-")
for key in storage_keys[1:]:
    df = pd.merge(df, merge_parquet_files(folder_path, prefix=f"{LIDO_ADDRESS}-{key}-"), on='blockNumber')
# 2. convert numbers from str to int
for col in df.columns:
    df[col] = df[col].apply(int)
# 3. convert from wei to Ether 
#ether_keys = [
#    "lido.Lido.beaconBalance",
#    "lido.Lido.bufferedEther",
#    "lido.StETH.totalShares"
#]
#
#for key in ether_keys:
#    df[key] = df[key]/wei
# 4. sort by block
df = df.sort_values(by='blockNumber')  
df = df.drop_duplicates()                                                                                                                                                                                            
# 5. compute TotalPooledEther
df['TotalPooledEther'] = (df['lido.Lido.bufferedEther'] + df['lido.Lido.beaconBalance'] + (df['lido.Lido.depositedValidators'] - df['lido.Lido.beaconValidators'])*DEPOSIT_SIZE*wei)

## 6. conversion rate from stETH to shares
#df['stETH2Shares'] = df["lido.StETH.totalShares"]/df['TotalPooledEther'].replace(0, np.nan)
df.loc[:, 'stETH2Shares'] = df["lido.StETH.totalShares"] / df['TotalPooledEther'].replace(0, np.nan).infer_objects(copy=False)


# 7. Drop rows where stETH2Shares is NaN
df = df.dropna(subset=['stETH2Shares'])

## 7.1 conver in wei for the parque file
df_save = df.copy()
ether_keys = [
    "lido.Lido.beaconBalance",
    "lido.Lido.bufferedEther",
    "lido.StETH.totalShares"
]

for key in ether_keys:
   df_save[key] = df_save[key]/wei
# 8. write to parquet file the df
est_size_bytes = len(df_save.to_parquet(compression='snappy'))
est_size_mb = est_size_bytes / (1024 * 1024)
print(f"Estimated df file size: {est_size_mb:.2f} MB")
df_save.to_parquet(f'{PROCESSED}/pre-process/stETH-variables.parquet', compression='snappy')

# CONVERT TRANSFERS AND SAVE
# --------------------------
folder_path = f"{INPUT}/stETH_event"
prefix = f"{LIDO_ADDRESS}-Transfer-"

dfTransfer = merge_parquet_files(folder_path, prefix)
# convert to string hexbyte transaction hash
dfTransfer['transactionHash'] = dfTransfer['transactionHash'].apply(lambda x: x.hex())
# sort by block
dfTransfer = dfTransfer.sort_values(by='blockNumber')
# convert value to int
dfTransfer['value'] = dfTransfer['value'].apply(int)#/wei
# add stETH2Shares column
dfTransfer = dfTransfer.merge(
   df[['blockNumber', 'stETH2Shares']], 
   on='blockNumber', 
   how='left'
)
# convert to valueShares
dfTransfer['valueShares'] = dfTransfer['stETH2Shares'] * dfTransfer['value']

# write to parquet file dfTransfer
dfTransferSave = dfTransfer.copy()
dfTransferSave['value'] = dfTransferSave['value'].apply(int)/wei
est_size_bytes = len(dfTransferSave.to_parquet(compression='snappy'))
est_size_mb = est_size_bytes / (1024 * 1024)
print(f"Estimated dfTransfer file size: {est_size_mb:.2f} MB")
dfTransferSave.to_parquet(f'{PROCESSED}/pre-process/stETH-TransferShares.parquet', compression='snappy')



# PREPROCESSING FOR MICROVELOCITY
# -------------------------------
"""
output:
  - shares-transfers.csv
  - shares-allocated.csv
"""

# Drop columns transactionHash, value, stETH2Shares
dfTransfer = dfTransfer.drop(columns=['transactionHash', 'value', 'stETH2Shares'])

dfTransfer.columns = [
    'block_number',	
    'from_address',
    'to_address',
    'amount'
]
# drop nan value
dfTransfer = dfTransfer.dropna(subset=['amount'])
# save
dfTransfer.to_csv(f'{PROCESSED}/pre-process/stETH-shares-transfers.csv', index=False)

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
df.to_csv(f'{PROCESSED}/pre-process/stETH-shares-allocated.csv', index=False)

