# Install script for directory: /Users/zhubinbin/github/valkey/src

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE EXECUTABLE PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ FILES "/Users/zhubinbin/github/valkey/cmake-build-debug/bin/valkey-server")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-server" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-server")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" -u -r "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-server")
    endif()
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-server     redis-server       )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE EXECUTABLE PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ FILES "/Users/zhubinbin/github/valkey/cmake-build-debug/bin/valkey-cli")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-cli" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-cli")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" -u -r "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-cli")
    endif()
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-cli     redis-cli       )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/bin" TYPE EXECUTABLE PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ FILES "/Users/zhubinbin/github/valkey/cmake-build-debug/bin/valkey-benchmark")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-benchmark" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-benchmark")
    if(CMAKE_INSTALL_DO_STRIP)
      execute_process(COMMAND "/usr/bin/strip" -u -r "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/valkey-benchmark")
    endif()
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-benchmark     redis-benchmark       )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-server     valkey-sentinel       )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-server     valkey-check-rdb       )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "valkey" OR NOT CMAKE_INSTALL_COMPONENT)
  execute_process(                          COMMAND /bin/bash /Users/zhubinbin/github/valkey/cmake-build-debug/CreateSymlink.sh     valkey-server     valkey-check-aof       )
endif()

