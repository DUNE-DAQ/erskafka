/**
 * @file ERSPublisher.cpp ERSPublusher Class Implementation
 *  
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "erskafka/ERSPublisher.hpp"

using namespace dunedaq;

namespace erskafka{

ERSPublisher::ERSPublisher(const nlohmann::json& conf);

}