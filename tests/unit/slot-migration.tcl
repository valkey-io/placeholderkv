

# TEST CASES
# ---- General ----
# - Only migrating slots are synced
# - Changes in non-migrating slots are not sent to target
# - Parsing test
# - Slot must have available primary
#
# ---- Reslience ----
# - Target gives up if primary is unavailable
# - Source unpauses itself if replica is unavailable
# - Client is closed by target during migration
#
# ---- Importing slot is not exposed ----
# - KEYS command on importing node
# - RANDOMKEY on importing node
#
# ---- Replication
# - Replica receives updates through target primary
# - Time out results in replica dropping slots
# - Failover during migration cleans up slots