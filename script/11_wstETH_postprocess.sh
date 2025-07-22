#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# postprocess MicroVelocity 7200 blocks (1 day)
python 10_wstETH_postprocess_MicroVelocity.py --input "$PROCESSED/post-process/wstETH_general_velocities-7200.pickle" --output "$OUTPUT/wstETH-money-velocity-7200.parquet" --nblocks 7200

# postprocess MicroVelocity 300 blocks (1 hour)
python 10_wstETH_postprocess_MicroVelocity.py --input "$PROCESSED/post-process/wstETH_general_velocities-300.pickle" --output "$OUTPUT/wstETH-money-velocity-300.parquet" --nblocks 300
