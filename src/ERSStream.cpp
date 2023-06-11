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

  dunedaq::ers::IssueChain ERSStream::ers_to_schema( const ers::Issue & i) const {
    
  }


  // void erskafka::ERSStream::ers_to_json(const ers::Issue &issue, size_t chain, std::vector<nlohmann::json> &j_objs)
  // {
  //   try
  //   {
  //     nlohmann::json message;
  //     message["partition"] = m_partition.c_str();
  //     message["issue_name"] = issue.get_class_name();
  //     message["message"] = issue.message().c_str();
  //     message["severity"] = ers::to_string(issue.severity());
  //     message["usecs_since_epoch"] = std::chrono::duration_cast<std::chrono::microseconds>(issue.ptime().time_since_epoch()).count();
  //     message["time"] = std::chrono::duration_cast<std::chrono::milliseconds>(issue.ptime().time_since_epoch()).count();

  //     message["qualifiers"] = issue.qualifiers();
  //     message["params"] = nlohmann::json::array({});
  //     for (auto p : issue.parameters())
  //     {
  //       message["params"].push_back(p.first + ": " + p.second);
  //     }
  //     message["cwd"] = issue.context().cwd();
  //     message["file_name"] = issue.context().file_name();
  //     message["function_name"] = issue.context().function_name();
  //     message["host_name"] = issue.context().host_name();
  //     message["package_name"] = issue.context().package_name();
  //     message["user_name"] = issue.context().user_name();
  //     message["application_name"] = issue.context().application_name();
  //     message["user_id"] = issue.context().user_id();
  //     message["process_id"] = issue.context().process_id();
  //     message["thread_id"] = issue.context().thread_id();
  //     message["line_number"] = issue.context().line_number();
  //     message["chain"] = chain;

  //     if (issue.cause())
  //     {
  //       ers_to_json(*issue.cause(), 2, j_objs); 
  //     }
  //     j_objs.push_back(message);
  //   }
  //   catch(const std::exception& e)
  //   {
  //     std::cout << "Conversion from json error : " << e.what() << '\n';
  //   }
  // }

  // void erskafka::ERSStream::kafka_exporter(std::string input, std::string topic)
  // {
  //   try
  //   {
  //     // RdKafka::Producer::RK_MSG_COPY to be investigated
  //     RdKafka::ErrorCode err = m_producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(input.c_str()), input.size(), nullptr, 0, 0, nullptr, nullptr);
  //     if (err != RdKafka::ERR_NO_ERROR) { std::cout << "% Failed to produce to topic " << topic << ": " << RdKafka::err2str(err) << std::endl;}
  //   }
  //   catch(const std::exception& e)
  //   {
  //     std::cout << "Producer error : " << e.what() << '\n';
  //   }
  // }

  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::ERSStream::write(const ers::Issue &issue)
  {
    try {
      
      m_publisher -> publish(ers_to_schema( issue ));

    }
    catch(const std::exception& e)
    {
      std::cout << "Producer error : " << e.what() << '\n';
    }
  }
} // namespace erskafka
