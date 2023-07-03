
/** 
* @file ProtobufStream.hpp This file defines ProtoBufStream.
* This is part of the DUNE DAQ software, copyright 2020.
*  Licensing/copyright details are in the COPYING file that you should have
*  received with this code.
*
*/

#ifndef ERSKAFKA_PROTOBUFSTREAM_HPP 
#define ERSKAFKA_PROTOBUFSTREAM_HPP

#include <ers/OutputStream.hpp>
#include "erskafka/ERSPublisher.hpp"

#include <string>
#include <vector>
#include <memory>

namespace erskafka
{    
  /** This stream offers capability of publishing Issues to a data stream, 
   *  So that other services can subscribe to the stream.
   * A stream configuration is composed of the stream name,
   * that is "protobufstream". 
   * Messages are transported using the ERS schema defined in ers
   * 
   * \brief ERS stream implementation.
   */
    
  class ProtoBufStream : public ers::OutputStream {
  public:
    explicit ProtoBufStream( const std::string & param);
    void write( const ers::Issue & issue ) override;
        
  private:	
    std::unique_ptr<dunedaq::erskafka::ERSPublisher> m_publisher;
    std::string m_session;
    std::string m_application;
  };
} //namespace erskafka

#endif  //ERSKAFKA_ERSSTREAM_HPP 
