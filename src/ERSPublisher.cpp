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

ERSPublisher::ERSPublisher(const nlohmann::json& conf) {

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;

    auto it = conf.find("bootstrap");
    if ( it == conf.end() ) {
        throw MissingInfo(ERS_HERE, "bootstrap");
    }

    conf->set("bootstrap.servers", *it, errstr);
    if(errstr != ""){
      throw FailedConfig("bootstrap.servers", errstr);
    }

    std::string cliend_id;
    if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
        client_id = env_p;
    else
        client_id = "erskafkaproducerdefault";
      
    
    conf->set("client.id", client_id, errstr);    
    if(errstr != ""){
       throw FailedConfig("client.id", errstr);
    }

    //Create producer instance
    m_producer.reset(RdKafka::Producer::create(conf, errstr));

    if(errstr != ""){
      throw FailedConfig("Producer creation", errstr);
    }


}

}