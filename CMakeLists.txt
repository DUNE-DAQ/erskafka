cmake_minimum_required(VERSION 3.12)
project(erskafka VERSION 1.5.1)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(ers REQUIRED)     
find_package(nlohmann_json REQUIRED)
find_package(RdKafka REQUIRED)
find_package(Boost COMPONENTS iostreams REQUIRED)
set(BOOST_LIBS Boost::iostreams ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_LIBRARIES})

daq_add_library(KafkaStream.cpp ProtoBufStream.cpp ERSPublisher.cpp LINK_LIBRARIES ers::ers nlohmann_json::nlohmann_json RdKafka::rdkafka RdKafka::rdkafka++)
target_include_directories(erskafka PUBLIC)


daq_add_application( ers_publisher_test ers_publisher_test.cxx TEST LINK_LIBRARIES erskafka )
daq_add_application( protobuf_stream_test protobuf_stream_test.cxx TEST LINK_LIBRARIES erskafka )

##############################################################################

daq_install()



