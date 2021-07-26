cmake_minimum_required(VERSION 3.12)
project(erskafka VERSION 1.1.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(ers REQUIRED)     
find_package(nlohmann_json REQUIRED)
find_package(RdKafka REQUIRED)
find_package(Boost COMPONENTS iostreams REQUIRED)
set(BOOST_LIBS Boost::iostreams ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_LIBRARIES})

daq_add_library(KafkaStream.cpp LINK_LIBRARIES curl ers::ers nlohmann_json::nlohmann_json RdKafka::rdkafka RdKafka::rdkafka++)
target_include_directories(erskafka PUBLIC)

##############################################################################
#daq_add_application( erskafka_test_program test.cxx TEST LINK_LIBRARIES erskafka )

##############################################################################
#daq_add_unit_test(ProdConsumeTest ProdConsumeTest.cxx TEST LINK_LIBRARIES erskafka ${BOOST_LIBS})

daq_install()



