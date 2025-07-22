#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Variable Collection
# lido variables
VARIABLES=("depositedValidators" "beaconValidators" "beaconBalance" "bufferedEther")
# VARIABLES=("beaconBalance" "bufferedEther")
for VAR in "${VARIABLES[@]}"; do
  parallel_variable_tracker --contract-address $LIDO_ADDRESS --storage-key "lido.Lido.$VAR" --output-type int --cores 10 --from-block 11000000  --rpc $RPC_LOCAL --log-dir $LOGS --output-dir "$INPUT/stETH_variable/" --output-prefix "$LIDO_ADDRESS-$VAR"
done
# stETH variables
VAR="totalShares"
parallel_variable_tracker --contract-address $LIDO_ADDRESS --storage-key "lido.StETH.$VAR" --output-type int --cores 10 --from-block 11000000  --rpc $RPC_LOCAL --log-dir $LOGS --output-dir "$INPUT/stETH_variable/" --output-prefix "$LIDO_ADDRESS-$VAR"
# burn address
VAR="BURN_ROLE"
parallel_variable_tracker --contract-address $LIDO_ADDRESS --storage-key "$VAR" --output-type int --cores 10 --from-block 11000000  --rpc $RPC_LOCAL --log-dir $LOGS --output-dir "$INPUT/stETH_variable/" --output-prefix "$LIDO_ADDRESS-$VAR"