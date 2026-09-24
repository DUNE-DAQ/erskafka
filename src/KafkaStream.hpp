
/**
 * @file KafkaStream.hpp This file defines KafkaStream ERS stream.
 * This is part of the DUNE DAQ software, copyright 2020.
 *  Licensing/copyright details are in the COPYING file that you should have
 *  received with this code.
 *
 */

#ifndef ERSES_KAFKASTREAM_HPP
#define ERSES_KAFKASTREAM_HPP

#include <ers/OutputStream.hpp>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace erskafka {
/** This stream offers capability of publishing Issues to elastic search.
 * A stream configuration is composed of the stream name,
 * that is "erskafka".
 *
 * \brief ES stream implementation.
 */

class KafkaStream : public ers::OutputStream
{
public:
  explicit KafkaStream(const std::string& param);
  void write(const ers::Issue& issue) override;

private:
  std::string m_partition;
  RdKafka::Producer* m_producer;
  void ers_to_json(const ers::Issue& issue, size_t chain, std::vector<nlohmann::json>& j_objs);
  void kafka_exporter(std::string input, std::string topic);
};
} // namespace erskafka

#endif
