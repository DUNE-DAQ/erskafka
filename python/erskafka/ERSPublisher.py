 # @file ERSPublisher.py ERSPublusher Class Implementation
 #  
 # This is part of the DUNE DAQ Software Suite, copyright 2023.
 # Licensing/copyright details are in the COPYING file that you should have
 # received with this code.
 # 
from kafka import KafkaConsumer
import json
import threading 
import socket
import os
import re
import logging
import ers.issue_pb2 as ersissue
import google.protobuf.message as msg

from kafka import KafkaProducer

class ERSPublisher:
    def __init__(self, config):
        """Initialize the ERSPublisher with given Kafka configurations.

        Args:
            config (dict): Configuration for Kafka producer. 
                           Should contain 'bootstrap' for the Kafka bootstrap server.
        """
        self.bootstrap = config['bootstrap']

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode('utf-8')
        )

    def publish(self, issue, topic="ers_stream"):
        """Publish an ERS issue to the Kafka topic.

        Args:
            issue (ers.issue_pb2.IssueChain): The issue to be published.
            topic (str, optional): Kafka topic to publish to. Defaults to "ers_stream".

        Returns:
            FutureRecordMetadata: Result from Kafka producer send method.
        """
        binary = issue.SerializeToString()
        return self.producer.send(topic, key=issue.session, value=binary)

    def close(self, timeout=None):
        """Close the Kafka producer.

        Args:
            timeout (float, optional): The time to block during the close operation.
        """
        self.producer.close(timeout=timeout)


