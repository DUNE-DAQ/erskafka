#
 # @file ERSPublisher.py ERSPublusher Class Implementation
 #  
 # This is part of the DUNE DAQ Software Suite, copyright 2023.
 # Licensing/copyright details are in the COPYING file that you should have
 # received with this code.
 #

from confluent_kafka import Producer
import os
from dunedaq.ersschema import IssueChain # Assuming the correct import path

class ERSPublisher:
    def __init__(self, conf):
        k_conf = {
            'bootstrap.servers': conf.get('bootstrap'),
            'client.id': conf.get('cliend_id', os.getenv('DUNEDAQ_APPLICATION_NAME', 'erskafkaproducerdefault'))
        }

        if k_conf['bootstrap.servers'] is None:
            raise RuntimeError('Missing bootstrap from json file')

        self.producer = Producer(k_conf)
        self.default_topic = conf.get('default_topic', 'ers_stream')

    def publish(self, issue: IssueChain):
        binary = issue.SerializeToString()

        # Get the topic and key
        topic = self.topic(issue)
        key = self.key(issue)

        err = self.producer.produce(topic, binary, key)

        if err is not None:
            return False

        return True

    def topic(self, issue: IssueChain):
        return self.default_topic

    def key(self, issue: IssueChain):
        return issue.session # Accessing the session field directly
