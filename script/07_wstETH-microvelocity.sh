#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# # preprocess data to run MicroVelocity
python 06_wstETH_preprocess_MicroVelocity.py
# # run MicroVelocity 7200 blocks (1 day)
micro-velocity-analyzer --allocated_file "$PROCESSED/pre-process/wstETH-transfer-allocated.csv" --transfers_file "$PROCESSED/pre-process/wstETH-transfers.csv" --output_file "$PROCESSED/post-process/wstETH_general_velocities-7200.pickle" --save_every_n 7200 --n_cores 10
# run MicroVelocity 300 blocks (1 hour)
micro-velocity-analyzer --allocated_file "$PROCESSED/pre-process/wstETH-transfer-allocated.csv" --transfers_file "$PROCESSED/pre-process/wstETH-transfers.csv" --output_file "$PROCESSED/post-process/wstETH_general_velocities-300.pickle" --save_every_n 300 --n_cores 10