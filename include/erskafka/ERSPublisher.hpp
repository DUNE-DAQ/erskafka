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
#include "ers/issue.pn.h"

namespace dunedaq::erskafka {

    class ERSPublisher {

        public:

        ERSPublisher(const nlohmann::json& conf);

        ERSPublisher() = delete;
        ERSPublisher(const ERSPublisher & ) = delete;
        ERSPublisher(ERSPublisher && ) = delete;

        ~ERSPublisher();
        
        bool publish( ers::IssueChain && ) const;
        template<class Iterator>
        bool publish( Iterator begin, Iterator end) const;


    };
}

#endif  //ERSKAFKA_INCLUDE_ERSKAFKA_ERSPUBLISHER_HPP_
