start_server [list overrides [list "key-load-delay" 50 loading-process-events-interval-bytes 1024]] {
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

        # The ping at the end should still return LOADING error
        $rd memory doctor
        $rd memory malloc-stats
        $rd memory stats
        $rd memory help
        # Memory usage on key while loading is not well defined -> keep error
        $rd memory usage key:1
        $rd memory purge
        $rd ping

        assert_match {Hi Sam, *} [$rd read]
        assert_match {*} [$rd read]
        assert_match {peak.allocated *} [$rd read]
        assert_match {{MEMORY <subcommand> *}} [$rd read]
        # Memory usage keeps getting rejected in loading because the dataset is not visible
        assert_error {*LOADING*} {$rd read}
        assert_match OK [$rd read]
        assert_error {*LOADING*} {$rd read}

        $rd close
    }
}
