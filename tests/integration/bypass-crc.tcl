start_server {tags {"repl tls"} overrides {save {}}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    set primary_bypassed_crc_counter 0
    foreach mds {no yes} {
        foreach sdl {disabled on-empty-db swapdb flush-before-load} {
            test "Bypass CRC sync - tls:$::tls, repl_diskless_sync:$mds, repl_diskless_load:$sdl" {
                $primary config set repl-diskless-sync $mds
                start_server {overrides {save {}}} {
                    set replica [srv 0 client]
                    $replica config set repl-diskless-load $sdl
                    $replica replicaof $primary_host $primary_port
                    
                    wait_for_condition 50 100 {
                        [string match {*master_link_status:up*} [$replica info replication]]
                    } else {
                        fail "Replication not started"
                    }

                    set replica_bypassing_crc_count [string match {*total_sync_bypass_crc:1*} [$replica info stats]]
                    set stats [regexp -inline {total_sync_bypass_crc:(\d+)} [$primary info stats]]
                    set primary_bypass_crc_count [lindex $stats 1]
                    
                    if {$sdl eq "disabled" || $mds eq "no" || !$::tls} {
                        assert_equal $primary_bypassed_crc_counter $primary_bypass_crc_count "Primary should not bypass CRC in this scenario"
                        assert_equal 0 $replica_bypassing_crc_count "Replica should not bypass CRC in this scenario"
                    } else {
                        incr primary_bypassed_crc_counter
                        assert_equal $primary_bypassed_crc_counter $primary_bypass_crc_count "Primary should bypass CRC in this scenario"
                        assert_equal 1 $replica_bypassing_crc_count "Replica should bypass CRC in this scenario"
                    }
                }
            }
        }
    }
}
