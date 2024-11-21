#!/bin/sh

prefix=/Users/zhubinbin/github/valkey/cmake-build-debug/jemalloc-build
exec_prefix=/Users/zhubinbin/github/valkey/cmake-build-debug/jemalloc-build
libdir=${exec_prefix}/lib

DYLD_INSERT_LIBRARIES=${libdir}/libjemalloc.2.dylib
export DYLD_INSERT_LIBRARIES
exec "$@"
