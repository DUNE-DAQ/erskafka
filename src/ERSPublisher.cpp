/**
 * @file ERSPublisher.cpp ERSPublusher Class Implementation
 *  
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "erskafka/ERSPublisher.hpp"

#include <iostream>

#include <stdexcept>

using namespace dunedaq::erskafka;


ERSPublisher::ERSPublisher(const nlohmann::json& conf) {

    RdKafka::Conf * k_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;

    auto it = conf.find("bootstrap");
    if ( it == conf.end() ) {
      std::cerr << "Missing bootstrap from json file";
      throw std::runtime_error( "Missing bootstrap from json file" );
    }

    k_conf->set("bootstrap.servers", *it, errstr);
    if(errstr != ""){
      throw std::runtime_error( errstr );
    }

    std::string client_id;
    it = conf.find( "cliend_id" );
    if ( it != conf.end() )
        client_id = *it;
    else if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
        client_id = env_p;
    else
        client_id = "erskafkaproducerdefault";

    k_conf->set("client.id", client_id, errstr);    
    if(errstr != ""){
      throw std::runtime_error( errstr );
    }

    //Create producer instance
    m_producer.reset(RdKafka::Producer::create(k_conf, errstr));

    if(errstr != ""){
      throw std::runtime_error( errstr );
    }

    it = conf.find("default_topic");
    if (it != conf.end()) m_default_topic = *it;

    it = conf.find("partition");
    if ( it != conf.end() ) m_partition = *it;
    else if(const char* env_p = std::getenv("DUNEDAQ_PARTITION")) 
      m_partition = env_p;
    else {
      throw std::runtime_error( "Unable to find parition information" );
    }
    
}

bool ERSPublisher::publish( ersschema::IssueChain && issue ) const {

  std::string binary;
  issue.SerializeToString( & binary );
  
  // get the topic
  auto topic = ERSPublisher::topic(issue);

  auto key = ERSPublisher::key(issue);
  
  //      RdKafka::Producer::RK_MSG_COPY to be investigated
  RdKafka::ErrorCode err = m_producer->produce(topic, 
					       RdKafka::Topic::PARTITION_UA, 
					       RdKafka::Producer::RK_MSG_COPY, 
					       const_cast<char *>(binary.c_str()), binary.size(), 
					       key.c_str(),
					       key.size(),
					       0,
					       nullptr);
  if (err != RdKafka::ERR_NO_ERROR) {
    return false;
  }

  return true;
}

