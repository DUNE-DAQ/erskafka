/**
* @file KafkaStream.cpp KafkaStream class implementation
* 
* This is part of the DUNE DAQ software, copyright 2020.
* Licensing/copyright details are in the COPYING file that you should have
* received with this code.
*/

#include <erskafka/KafkaStream.hpp>
#include <ers/StreamFactory.hpp>
#include <string>
#include <iostream>
#include <chrono>
#include <boost/crc.hpp>
#include <vector>

ERS_REGISTER_OUTPUT_STREAM(erskafka::KafkaStream, "erskafka", param)

/** Constructor that creates a new instance of the erskafka stream with the given configuration.
  * \param format elastic search connection string.
  */
namespace erskafka
{   
  erskafka::KafkaStream::KafkaStream(const std::string &param)
  {

    if(const char* env_p = std::getenv("DUNEDAQ_PARTITION")) 
      m_partition = env_p;
  
    //Kafka server settings
    std::string brokers = param;
    std::string errstr;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    if(errstr != ""){
      std::cout << "Bootstrap server error : " << errstr << '\n';
    }
    if(const char* env_p = std::getenv("DUNEDAQ_APPLICATION_NAME")) 
      conf->set("client.id", env_p, errstr);
    else
      conf->set("client.id", "erskafkaproducerdefault", errstr);
    if(errstr != ""){
      std::cout << "Producer configuration error : " << errstr << '\n';
    }
    //Create producer instance
    m_producer = RdKafka::Producer::create(conf, errstr);

    if(errstr != ""){
      std::cout << "Producer creation error : " << errstr << '\n';
    }

  }

  void erskafka::KafkaStream::ers_to_json(const ers::Issue &issue, size_t chain, std::vector<nlohmann::json> &j_objs)
  {
    try
    {
      nlohmann::json message;
      message["partition"] = m_partition.c_str();
      message["issue_name"] = issue.get_class_name();
      message["message"] = issue.message().c_str();
      message["severity"] = ers::to_string(issue.severity());
      message["usecs_since_epoch"] = std::chrono::duration_cast<std::chrono::microseconds>(issue.ptime().time_since_epoch()).count();
      message["time"] = std::chrono::duration_cast<std::chrono::milliseconds>(issue.ptime().time_since_epoch()).count();

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
        ers_to_json(*issue.cause(), 2, j_objs); 
      }
      j_objs.push_back(message);
    }
    catch(const std::exception& e)
    {
      std::cout << "Conversion from json error : " << e.what() << '\n';
    }
  }

  void erskafka::KafkaStream::kafka_exporter(std::string input, std::string topic)
  {
    try
    {
      // RdKafka::Producer::RK_MSG_COPY to be investigated
      RdKafka::ErrorCode err = m_producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(input.c_str()), input.size(), nullptr, 0, 0, nullptr, nullptr);
      if (err != RdKafka::ERR_NO_ERROR) { std::cout << "% Failed to produce to topic " << topic << ": " << RdKafka::err2str(err) << std::endl;}
    }
    catch(const std::exception& e)
    {
      std::cout << "Producer error : " << e.what() << '\n';
    }
  }

  /** Write method 
    * \param issue issue to be sent.
    */
  void erskafka::KafkaStream::write(const ers::Issue &issue)
  {
    try
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

        erskafka::KafkaStream::kafka_exporter(j.dump(), "erskafka-reporting");
      }
      chained().write(issue);
    }
    catch(const std::exception& e)
    {
      std::cout << "Producer error : " << e.what() << '\n';
    }
  }
} // namespace erskafka
