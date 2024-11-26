#!/bin/bash

# Configuration
PRIMARY_HOST="127.0.0.1"
PRIMARY_PORT="6379"
REPLICA_HOST="127.0.0.1"
REPLICA_PORT="6380"
KEY_COUNT=100000
KEY_SIZE=409600  # 40 KiB

# Start time
START_TIME=$(date +%s%N)
echo "Start time: $START_TIME"

# Populate Redis with memtier
echo "Filling Redis with $KEY_COUNT keys of $KEY_SIZE bytes each..."
memtier_benchmark --protocol=redis --server=$PRIMARY_HOST --port=$PRIMARY_PORT \
    --key-maximum=$KEY_COUNT --data-size=$KEY_SIZE --pipeline=64 --randomize \
    --clients=6 --threads=6 --requests=allkeys --ratio=1:0 --key-pattern=P:P
echo "Data population complete."

PRIMARY_FILL_TIME=$(date +%s)

# Wait for replica to catch up
echo "Waiting for replica to catch up..."
while true; do
    PRIMARY_REPL_OFFSET=$(redis-cli -h $PRIMARY_HOST -p $PRIMARY_PORT info replication | grep "master_repl_offset" | awk -F: '{print $2}' | tr -d '\r')
    REPLICA_REPL_OFFSET=$(redis-cli -h $REPLICA_HOST -p $REPLICA_PORT info replication | grep "master_repl_offset" | awk -F: '{print $2}' | tr -d '\r')
    
    if [ "$PRIMARY_REPL_OFFSET" == "$REPLICA_REPL_OFFSET" ]; then
        echo "Replica caught up."
        break
    fi
    
    echo "Primary offset: $PRIMARY_REPL_OFFSET, Replica offset: $REPLICA_REPL_OFFSET. Waiting..."
done

# End time
END_TIME=$(date +%s%N)
echo "End time: $END_TIME"

# Calculate elapsed time
CATCH_UP_TIME=$((END_TIME - PRIMARY_FILL_TIME))
echo "Replica catch-up time: $CATCH_UP_TIME nanoseconds"
TOTAL_TIME=$((END_TIME - START_TIME))
echo "Total time: $TOTAL_TIME nanoseconds"
