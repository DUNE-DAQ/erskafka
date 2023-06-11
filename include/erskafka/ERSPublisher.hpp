/**
 * @file ERSPublisher.hpp
 *
 * This is the interface to broadcast ERS schema object in our DAQ system
 *
 * This is part of the DUNE DAQ Application Framework, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef ERSKAFKA_INCLUDE_ERSKAFKA_ERSPUBLISHER_HPP_
#define ERSKAFKA_INCLUDE_ERSKAFKA_ERSPUBLISHER_HPP_

#include "nlohmann/json.hpp"

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

#include "ers/issue.pb.h"

namespace dunedaq {


namespace erskafka {

    class ERSPublisher {

        public:

        ERSPublisher(const nlohmann::json& conf);

        ERSPublisher() = delete;
        ERSPublisher(const ERSPublisher & ) = delete;
        ERSPublisher & operator = (const ERSPublisher & ) = delete;
        ERSPublisher(ERSPublisher && ) = delete;
        ERSPublisher & operator = (ERSPublisher && ) = delete;

        ~ERSPublisher();


        
      bool publish( dunedaq::ers::IssueChain && ) const;
      // template<class Iterator>
        // bool publish( Iterator begin, Iterator end) const;

    protected:
      std::string topic( const ::dunedaq::ers::IssueChain &  ) const {
	return m_default_topic;
      }

      std::string key( const ::dunedaq::ers::IssueChain &  ) const {
	return m_partition;
      }

    private:
        
      std::unique_ptr<RdKafka::Producer> m_producer;
      std::string m_default_topic = "ers_stream";
      std::string m_partition ;
      

    };
}  // erskafka namespace 
}  // dunedaq namespace

#endif  //ERSKAFKA_INCLUDE_ERSKAFKA_ERSPUBLISHER_HPP_
