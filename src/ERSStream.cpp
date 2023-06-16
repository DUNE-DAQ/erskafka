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
  erskafka::ERSStream::ERSStream(const std::string &param) {

    nlohmann::json conf;
    conf["bootstrap"] = param;

    m_publisher = std::make_unique<dunedaq::erskafka::ERSPublisher>(conf);

    if(const char* env_p = std::getenv("DUNEDAQ_PARTITION")) 
      m_session = env_p;
    else {
      throw std::runtime_error( "Unable to find parition information" );
    }

  }


  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::ERSStream::write(const ers::Issue &issue)
  {
    try {
      
      m_publisher -> publish(issue.schema_chain(m_session));

    }
    catch(const std::exception& e)
    {
      std::cout << "Producer error : " << e.what() << '\n';
    }
  }
} // namespace erskafka
