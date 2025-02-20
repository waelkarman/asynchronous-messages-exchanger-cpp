cmake_minimum_required(VERSION 3.16)

project(udp-task)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED OFF)
set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_BUILD_PARALLEL_LEVEL 18)

if(CMAKE_BUILD_TYPE MATCHES Debug)
# -fsanitize=thread -Wextra -Wall
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak -fsanitize=address -fsanitize=undefined -g -O1 -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=leak  -fsanitize=undefined -fsanitize=address")
endif()

include_directories(include)

add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(libs)