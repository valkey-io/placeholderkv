

# TEST CASES
# ---- General ----
# - Only migrating slots are synced
# - Changes in non-migrating slots are not sent to target
# - Parsing test
# - Slot must have available primary
#
# ---- Error handling ----
# - Target gives up if primary is unavailable
# - Source unpauses itself if replica is unavailable
# - Client is closed by target during migration
# - Client is closed by source during migration
#
# ---- Importing slot is not exposed ----
# - KEYS command on importing node
# - RANDOMKEY on importing node
#
# ---- Replication ----
# - Replica receives updates through target primary
# - Time out results in replica dropping slots
# - Failover during migration cleans up slots
# - Full sync with pending migration includes pending slots, is cleaned up if migration fails
#
# ---- Loading ----
# - Partial slot migration is cleaned up after AOF load
# - Partial slot migration is cleaned up after RDB load