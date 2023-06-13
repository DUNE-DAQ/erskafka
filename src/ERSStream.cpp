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

  }


  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::ERSStream::write(const ers::Issue &issue)
  {
    try {
      
      m_publisher -> publish(ToChain( issue ));

    }
    catch(const std::exception& e)
    {
      std::cout << "Producer error : " << e.what() << '\n';
    }
  }
} // namespace erskafka
