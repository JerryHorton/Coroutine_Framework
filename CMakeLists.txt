cmake_minimum_required(VERSION 3.21)
project(coroutine_framework C)

set(CMAKE_C_STANDARD 99)

add_executable(nty_client core/nty_coroutine.c core/nty_epoll.c core/nty_schedule.c core/nty_socket.c sample/nty_client.c)

add_executable(nty_server core/nty_coroutine.c core/nty_epoll.c core/nty_schedule.c core/nty_socket.c sample/nty_server.c)

target_include_directories(nty_client PRIVATE core)

target_include_directories(nty_server PRIVATE core)
