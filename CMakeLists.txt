cmake_minimum_required(VERSION 3.12)
project(erskafka VERSION 1.1.0)

find_package(daq-cmake REQUIRED)

daq_setup_environment()

find_package(ers REQUIRED)     
find_package(nlohmann_json REQUIRED)
find_package(RdKafka REQUIRED)

set(ERSKAFKA_DEPENDENCIES ers::ers nlohmann_json::nlohmann_json)


##############################################################################

# Note that when the ers library is linked in, it uses a "::" because
# it's in an external package, not a local CMake target. The semantics
# are <namespace>::<shared library / executable>
daq_add_library(ESStream.cpp LINK_LIBRARIES curl ${ERSKAFKA_DEPENDENCIES} RdKafka::rdkafka RdKafka::rdkafka++)
target_include_directories(erskafka PUBLIC)

##############################################################################
daq_add_application( erskafka_test_program test.cxx TEST LINK_LIBRARIES erskafka )

##############################################################################
##daq_add_unit_test(ValueWrapper_test)

##############################################################################

# Could add "erses_test_program" to the argument list here, but
# presumably end users wouldn't care about an integration test program
# and just want the shared object library installed

daq_install()



