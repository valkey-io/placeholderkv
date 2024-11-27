start_server [list overrides [list "key-load-delay" 50 loading-process-events-interval-bytes 1024] tags [list "external:skip"]] {
    test "Memory inspection commands no longer return loading errors" {
        # Set up some initial data
        r debug populate 100000 key 1000
        
        # Save and restart
        r save
        restart_server 0 false false

        # At this point, keys are loaded one at time, busy looping 50usec
        # between each. Further, other events are processed every 1024 bytes
        # of RDB. We're sending all our commands deferred, so they have a
        # chance to be processed all at once between loading two keys.

        set rd [valkey_deferring_client]

        # Allowed during loading
        $rd memory help
        $rd memory malloc-stats
        $rd memory purge

        # Disallowed during loading (because directly dependent on the dataset)
        $rd memory doctor
        $rd memory stats
        $rd memory usage key:1

        # memory help
        assert_match {{MEMORY <subcommand> *}} [$rd read]
        # memory malloc-stats
        assert_match {*alloc*} [$rd read]
        # memory purge
        assert_match OK [$rd read]
        # memory doctor
        assert_error {*LOADING*} {$rd read}
        # memory stats
        assert_error {*LOADING*} {$rd read}
        # memory usage key:1
        assert_error {*LOADING*} {$rd read}

        $rd close
    }
}
