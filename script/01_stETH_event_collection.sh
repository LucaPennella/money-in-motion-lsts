#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Transfer
parallel_event_tracker --contract-file "$ABI/stETH.json" --contract-address $LIDO_ADDRESS --event-file "$EVENT/stETH-Transfer.sol" --cores 10 --from-block 19000000  --rpc $RPC_LOCAL  --log-dir $LOGS --output-dir "$INPUT/stETH_event/" --output-prefix "$LIDO_ADDRESS-Transfer"
# TransferShares
#parallel_event_tracker --contract-file "$ABI/stETH.json" --contract-address $LIDO_ADDRESS --event-file "$EVENT/stETH-TransferShares.sol" --cores 10 --from-block 17650000 --to-block 17700000  --rpc $RPC_LOCAL  --log-dir $LOGS --output-dir "$INPUT/stETH_event/" --output-prefix "$LIDO_ADDRESS-TransferShares"
# SharesBurnt
#parallel_event_tracker --contract-file "$ABI/stETH.json" --contract-address $LIDO_ADDRESS --event-file "$EVENT/stETH-SharesBurnt.sol" --cores 10 --from-block 17650000 --to-block 17700000   --rpc $RPC_LOCAL --log-dir $LOGS --output-dir "$INPUT/stETH_event/" --output-prefix "$LIDO_ADDRESS-SharesBurnt"