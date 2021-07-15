/**
* @file ESStream.cpp ESStream class implementation
* 
* This is part of the DUNE DAQ software, copyright 2020.
* Licensing/copyright details are in the COPYING file that you should have
* received with this code.
*/

#include "ESStream.hpp"
#include <ers/StreamFactory.hpp>
#include <string>
#include <iostream>
#include <chrono>
#include <boost/crc.hpp>

ERS_REGISTER_OUTPUT_STREAM(erskafka::ESStream, "erskafka", param)

/** Constructor that creates a new instance of the erskafka stream with the given configuration.
  * \param format elastic search connection string.
  */
erskafka::ESStream::ESStream(const std::string &param)
{

  if(const char* env_p = std::getenv("DUNEDAQ_PARTITION")) 
     m_partition = env_p;
 
  //Kafka server settings
  std::string brokers = param;
  std::string errstr;

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  conf->set("bootstrap.servers", brokers, errstr);
  conf->set("client.id", "erskafkaprod", errstr);
  //Create producer instance
  m_producer = RdKafka::Producer::create(conf, errstr);
}

void erskafka::ESStream::ers_to_json(const ers::Issue &issue, size_t chain, std::vector<nlohmann::json> &j_objs)
{

  nlohmann::json message;
  message["partition"] = m_partition.c_str();
  message["issue_name"] = issue.get_class_name();
  message["message"] = issue.message().c_str();
  message["severity"] = ers::to_string(issue.severity());
  message["usecs_since_epoch"] = std::chrono::duration_cast<std::chrono::microseconds>(issue.ptime().time_since_epoch()).count();
  message["time"] = std::chrono::duration_cast<std::chrono::milliseconds>(issue.ptime().time_since_epoch()).count();
  //message["time"] = issue.time_t() ;

  message["qualifiers"] = issue.qualifiers();
  message["params"] = nlohmann::json::array({});
  for (auto p : issue.parameters())
  {
    message["params"].push_back(p.first + ": " + p.second);
  }
  message["cwd"] = issue.context().cwd();
  message["file_name"] = issue.context().file_name();
  message["function_name"] = issue.context().function_name();
  message["host_name"] = issue.context().host_name();
  message["package_name"] = issue.context().package_name();
  message["user_name"] = issue.context().user_name();
  message["application_name"] = issue.context().application_name();
  message["user_id"] = issue.context().user_id();
  message["process_id"] = issue.context().process_id();
  message["thread_id"] = issue.context().thread_id();
  message["line_number"] = issue.context().line_number();
  message["chain"] = chain;

  if (issue.cause())
  {
    ers_to_json(*issue.cause(), 2, j_objs); // fill it
  }
  j_objs.push_back(message);
}

void erskafka::ESStream::kafka_exporter(std::string input, std::string topic)
{
  try
  {
    char *input_bytes = const_cast<char *>(input.c_str());
    m_producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, input_bytes, input.size(), nullptr, 0, 0, nullptr, nullptr);
    m_producer->flush(10 * 1000);
    if (m_producer->outq_len() > 0)
      std::cout << "% " << m_producer->outq_len() << " message(s) were not delivered" << std::endl;
  }
  catch(const std::exception& e)
  {
    std::cout << e.what() << '\n';
  }
}

/** Write method 
  * \param issue issue to be sent.
  */
void erskafka::ESStream::write(const ers::Issue &issue)
{

  std::vector<nlohmann::json> j_objs;
  if (issue.cause() == nullptr)
  {
    ers_to_json(issue, 0, j_objs);
  }
  else
  {
    ers_to_json(issue, 1, j_objs);
  }

  // build a unique hash for a group of nested issues
  std::ostringstream tmpstream(issue.message());
  tmpstream << issue.context().process_id() << issue.time_t() << issue.context().application_name() << issue.context().host_name() << rand();
  std::string tmp = tmpstream.str();
  boost::crc_32_type crc32;
  crc32.process_bytes(tmp.c_str(), tmp.length());

  for (auto j : j_objs)
  {
    j["group_hash"] = crc32.checksum();
    erskafka::ESStream::kafka_exporter(j.dump(), "erskafka-reporting");
  }
  chained().write(issue);
}
