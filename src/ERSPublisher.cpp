/**
 * @file ERSPublisher.cpp ERSPublusher Class Implementation
 *  
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "erskafka/ERSPublisher.hpp"

#include "erskafka/CommonIssues.hpp"

using namespace dunedaq::erskafka;


ERSPublisher::ERSPublisher(const nlohmann::json& conf) {

    RdKafka::Conf * k_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;

    auto it = conf.find("bootstrap");
    if ( it == conf.end() ) {
      throw erskafka::MissingInfo(ERS_HERE, "bootstrap");
    }

    k_conf->set("bootstrap.servers", *it, errstr);
    if(errstr != ""){
      throw erskafka::FailedConfig(ERS_HERE, "bootstrap.servers", errstr);
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
      throw FailedConfig(ERS_HERE, "client.id", errstr);
    }

    //Create producer instance
    m_producer.reset(RdKafka::Producer::create(k_conf, errstr));

    if(errstr != ""){
      throw FailedConfig(ERS_HERE, "Producer creation", errstr);
    }

    it = conf.find("default_topic");
    if (it != conf.end()) m_default_topic = *it;

}

// bool ERSPublisher::publish( ers::IssueChain && issue ) const {

//   try
//     {

//       std::string binary;
//       issue.SerializeToString( & binary );
      
//       // get the topic
//       auto topic = topic(issue);

//       // RdKafka::Producer::RK_MSG_COPY to be investigated
//       RdKafka::ErrorCode err = m_producer->produce(topic, 
//         RdKafka::Topic::PARTITION_UA, 
//         RdKafka::Producer::RK_MSG_COPY, 
//         const_cast<char *>(binary.c_str()), binary.size(), 
//         nullptr, 0, 0, nullptr, nullptr);
//       if (err != RdKafka::ERR_NO_ERROR) { 
//         throw ProductionFailedOnTopic(ERS_HERE, topic, RdKafka::err2str(err));        
//     }
//     catch(const std::exception& e)
//     {
//       throw ProductionFailed(ERS_HERE, e.what());  
//     }
//   }
//}

