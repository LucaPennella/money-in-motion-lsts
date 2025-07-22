#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# # preprocess data to run MicroVelocity
python 04_stETH_preprocess_MicroVelocity.py
# # run MicroVelocity 7200 blocks (1 day)
micro-velocity-analyzer --allocated_file "$PROCESSED/pre-process/stETH-shares-allocated.csv" --transfers_file "$PROCESSED/pre-process/stETH-shares-transfers.csv" --output_file "$PROCESSED/post-process/stETH_general_velocities-7200.pickle" --save_every_n 7200 --n_cores 10
# run MicroVelocity 300 blocks (1 hour)
#micro-velocity-analyzer --allocated_file "$PROCESSED/pre-process/stETH-shares-allocated.csv" --transfers_file "$PROCESSED/pre-process/stETH-shares-transfers.csv" --output_file "$PROCESSED/post-process/stETH_general_velocities-300.pickle" --save_every_n 300 --n_cores 10