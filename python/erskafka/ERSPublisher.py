# @file ERSPublisher.py ERSPublisher Class Implementation
#
# This is part of the DUNE DAQ Software Suite, copyright 2023.
# Licensing/copyright details are in the COPYING file that you should have
# received with this code.
#

from kafka import KafkaProducer
import ers.issue_pb2 as ersissue

class ERSPublisher:
    def __init__(self, config):
        """Initialize the ERSPublisher with given Kafka configurations.
        
        Args:
            config (dict): Configuration for Kafka producer. 
                           Should contain 'bootstrap' for the Kafka bootstrap server.
                           Optionally contains 'topic' for the default topic to publish to.
        """
        self.bootstrap = config['bootstrap']
        self.topic = config.get('topic', 'ers_stream')
        self.producer = None

    def __enter__(self):
        """Context manager entry method. Initializes the Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit method. Closes the Kafka producer."""
        if self.producer:
            self.producer.close()

    def publish(self, issue):
        """Publish an ERS issue to the Kafka topic.
        
        Args:
            issue (ers.issue_pb2.IssueChain): The issue to be published.
        
        Returns:
            FutureRecordMetadata: Result from Kafka producer send method.
        """
        return self.producer.send(self.topic, key=issue.session, value=issue)

# Usage example:
# with ERSPublisher(config) as publisher:
#     publisher.publish(issue)

