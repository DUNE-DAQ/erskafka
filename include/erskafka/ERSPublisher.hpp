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
#include "ers/issue.pb.h"

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>


namespace dunedaq::erskafka {


ERS_DECLAR_ISSUE(erskafka,
                 MissingInfo,
                 "JSON Missing " << json_entry,
                 ((std::string)json_entry )

ERS_DECLAR_ISSUE(erskafka,
                 FailedConfig,
                 json_entry << " failed to configure: " << message,
                 ((std::string)json_entry)((std::string)message)


ERS_DECLAR_ISSUE(erskafka,
                 FailingCreatingStreamer,
                 "ERS Streamer creation failed",
                 ERS_EMPTY)




    class ERSPublisher {

        public:

        ERSPublisher(const nlohmann::json& conf);

        ERSPublisher() = delete;
        ERSPublisher(const ERSPublisher & ) = delete;
        ERSPublisher & operator = (const ERSPublisher & ) = delete;
        ERSPublisher(ERSPublisher && ) = delete;
        ERSPublisher & operator = (ERSPublisher && ) = delete;

        ~ERSPublisher();


        
        bool publish( ers::IssueChain && ) const;
        // template<class Iterator>
        // bool publish( Iterator begin, Iterator end) const;

        protected:
          std::string topic( [[maybe_unsed]] const ers::IssueChain & ) const {
            
          }

        private:
        
        std::unique_ptr<RdKafka::Producer> m_producer;
        std::string m_default_topic = "ers_test";
        

    };
}

#endif  //ERSKAFKA_INCLUDE_ERSKAFKA_ERSPUBLISHER_HPP_
