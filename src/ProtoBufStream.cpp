/**
* @file ProtoBufStream.cpp ProtoBufStream class implementation
* 
* This is part of the DUNE DAQ software, copyright 2020.
* Licensing/copyright details are in the COPYING file that you should have
* received with this code.
*/

#include "ProtoBufStream.hpp"
#include <ers/StreamFactory.hpp>
#include <string>
#include <iostream>
#include <chrono>
#include <boost/crc.hpp>
#include <vector>

ERS_REGISTER_OUTPUT_STREAM(erskafka::ProtoBufStream, "protobufstream", param)

/** Constructor that creates a new instance of the ersstream stream with the given configuration.
  * \param format elastic search connection string.
  */
namespace erskafka
{   
  erskafka::ProtoBufStream::ProtoBufStream(const std::string &param)
    : m_session("Uknown")
    , m_application("Uknown") {

    nlohmann::json conf;
    conf["bootstrap"] = param;

    m_publisher = std::make_unique<dunedaq::erskafka::ERSPublisher>(conf);

    if(auto env_p = std::getenv("DUNEDAQ_SESSION")) 
      m_session = env_p;

    if (auto app_p = std::getenv("DUNEDAQ_APPLICATION_NAME"))
      m_application = app_p;
  }


  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::ProtoBufStream::write(const ers::Issue &issue)
  {
    try {

      auto schema = to_schema_chain(issue);
      schema.set_session(m_session);
      schema.set_application(m_application);
      m_publisher -> publish(std::move(schema)) ; 

    }
    catch(const std::exception& e)
    {
      std::cerr << "Producer error : " << e.what() << '\n';
    }
  }
} // namespace erskafka
