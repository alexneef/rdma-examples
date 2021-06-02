cmake_minimum_required(VERSION 2.8.4)
project(PubSub)

set(CMAKE_CXX_STANDARD 14)

add_executable(pub PubSub/publisher.cpp PubSub/RDMAHelper.cpp)
target_link_libraries(pub pthread rdmacm ibverbs)

add_executable(sub PubSub/subscriber.cpp PubSub/RDMAHelper.cpp)
target_link_libraries(sub pthread rdmacm ibverbs)

add_executable(star_pub PubSub-MultiSub/publisher.cpp PubSub-MultiSub/rdmautil.cpp PubSub-MultiSub/message.h)
target_link_libraries(star_pub pthread rdmacm ibverbs)

add_executable(star_sub PubSub-MultiSub/subscriber.cpp PubSub-MultiSub/rdmautil.cpp PubSub-MultiSub/message.h)
target_link_libraries(star_sub pthread rdmacm ibverbs)

add_executable(mcpub PubSubMC/publisher.cpp)
target_link_libraries(mcpub pthread rdmacm ibverbs)

add_executable(mcsub PubSubMC/subscriber.cpp)
target_link_libraries(mcsub pthread rdmacm ibverbs)

#add_executable(ucthello PubSubUCT/uct_hello_world.cpp)
#target_link_libraries(ucthello pthread rdmacm ibverbs ucs uct)