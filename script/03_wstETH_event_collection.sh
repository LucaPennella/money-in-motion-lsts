#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Transfer
parallel_event_tracker --contract-file "$ABI/wstETH.json" --contract-address $wstETH_ADDRESS --event-file "$EVENT/wstETH-Transfer.sol" --cores 10 --from-block 11000000 --rpc $RPC_LOCAL  --log-dir $LOGS --output-dir "$INPUT/wstETH_event_new/" --output-prefix "$wstETH_ADDRESS-Transfer"
#Test for see duplicate in the blocks
#parallel_event_tracker --contract-file "$ABI/wstETH.json" --contract-address $wstETH_ADDRESS --event-file "$EVENT/wstETH-Transfer.sol" --cores 10 --from-block 19350000 --to-block 19400000 --rpc $RPC_LOCAL  --log-dir $LOGS --output-dir "$INPUT/eventtest/" --output-prefix "$wstETH_ADDRESS-Transfer"
