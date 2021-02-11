cmake_minimum_required(VERSION 2.8.4)
project(PubSub)

set(CMAKE_CXX_STANDARD 14)

add_executable(pub PubSub/publisher.cpp PubSub/RDMAHelper.cpp)
target_link_libraries(pub pthread rdmacm ibverbs)

add_executable(sub PubSub/subscriber.cpp PubSub/RDMAHelper.cpp)
target_link_libraries(sub pthread rdmacm ibverbs)
