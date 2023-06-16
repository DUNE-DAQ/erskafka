/**
* @file ERSStream.cpp ERSStream class implementation
* 
* This is part of the DUNE DAQ software, copyright 2020.
* Licensing/copyright details are in the COPYING file that you should have
* received with this code.
*/

#include "ERSStream.hpp"
#include <ers/StreamFactory.hpp>
#include <string>
#include <iostream>
#include <chrono>
#include <boost/crc.hpp>
#include <vector>

ERS_REGISTER_OUTPUT_STREAM(erskafka::ERSStream, "ersstream", param)

/** Constructor that creates a new instance of the ersstream stream with the given configuration.
  * \param format elastic search connection string.
  */
namespace erskafka
{   
  erskafka::ERSStream::ERSStream(const std::string &param)
    : m_session("Uknown")
    , m_application("Uknown") {

    nlohmann::json conf;
    conf["bootstrap"] = param;

    m_publisher = std::make_unique<dunedaq::erskafka::ERSPublisher>(conf);

    if(auto env_p = std::getenv("DUNEDAQ_PARTITION")) 
      m_session = env_p;

    if (auto app_p = std::getenv("DUNEDAQ_APPLICATION_NAME"))
      m_application = app_p;
  }


  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::ERSStream::write(const ers::Issue &issue)
  {
    try {

      auto schema = issue.schema_chain();
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
