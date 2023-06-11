
/** 
* @file ERSStream.hpp This file defines ERSStream.
* This is part of the DUNE DAQ software, copyright 2020.
*  Licensing/copyright details are in the COPYING file that you should have
*  received with this code.
*
*/

#ifndef ERSKAFKA_ERSSTREAM_HPP 
#define ERSKAFKA_ERSSTREAM_HPP 

#include <ers/OutputStream.hpp>
#include <ers/ERSPublisher.hpp>

#include <erskafka/issue.pb.h>

#include <string>
#include <vector>
#include <memory>

namespace erskafka
{    
    /** This stream offers capability of publishing Issues to a data stream, 
     *  So that other services can subscribe to the stream.
      * A stream configuration is composed of the stream name,
      * that is "ersstream". 
      * Messages are transported using the ERS schema defined in ers
      * 
      * \brief ERS stream implementation.
      */
    
    class ERSStream : public ers::OutputStream
    {
      public:
	      explicit ERSStream( const std::string & param);
        void write( const ers::Issue & issue ) override;
        
      private:	
       //  std::string m_partition;
       std::unique_ptr<ERSPublisher> m_publisher;
        
       ers::IssueChain ers_to_schema( const ers::Issue & ) const; 
        //void ers_to_json(const ers::Issue & issue, size_t chain, std::vector<nlohmann::json> & j_objs);
        //void kafka_exporter(std::string input, std::string topic);

    };
} //namespace erskafka

#endif  //ERSKAFKA_ERSSTREAM_HPP 
