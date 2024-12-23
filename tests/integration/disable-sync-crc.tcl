start_server {tags {"repl tls"} overrides {save {}}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    foreach master_disable_crc {yes no} {
        foreach replica_disable_crc {yes no} {
            foreach mds {no yes} {
                foreach sdl {disabled on-empty-db swapdb flush-before-load} {
                    test "CRC disabled sync - master:$master_disable_crc, replica:$replica_disable_crc, tls:$::tls, repl_diskless_sync:$mds, repl_diskless_load:$sdl" {
                        $master config set disable-sync-crc $master_disable_crc
                        $master config set repl-diskless-sync $mds
                        start_server {overrides {save {}}} {
                            set replica [srv 0 client]
                            
                            $replica config set disable-sync-crc $replica_disable_crc
                            $replica config set repl-diskless-load $sdl
                            
                            $replica replicaof $master_host $master_port
                            
                            wait_for_condition 50 100 {
                                [string match {*master_link_status:up*} [$replica info replication]]
                            } else {
                                fail "Replication not started"
                            }
                            set is_master_crc_disabled [string match {*total_crc_disabled_syncs_started:1*} [$master info stats]]
                            set is_replica_crc_disabled [string match {*total_crc_disabled_syncs_started:1*} [$replica info stats]]
                            
                            if {$replica_disable_crc eq "no" || $sdl eq "disabled" || $mds eq "no" || !$::tls} {
                                assert_equal 0 $is_master_crc_disabled "Master should not have CRC disabled"
                                assert_equal 0 $is_replica_crc_disabled "Replica should not have CRC disabled"
                            } else {
                                if {$replica_disable_crc eq "no"} {
                                    assert_equal 0 $is_master_crc_disabled "Master should not have CRC disabled"
                                } else {
                                    assert_equal 1 $is_master_crc_disabled "Master should have CRC disabled"
                                }
                                assert_equal 1 $is_replica_crc_disabled "Replica should have CRC disabled"
                            }
                        }
                    }
                }
            }
        }
    }
}
