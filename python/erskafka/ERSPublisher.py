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
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
#del method is not determinist and not best practise, but it is the best option to clean the memory 
#without having the user to call the close method
    def __del__(self):
        """Destructor-like method to clean up resources."""
        self.close()
#the close method can be called by the user if they want to, but it will be dealt by del anyway
    def close(self):
        """Explicit method to close the Kafka producer."""
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
# publisher = ERSPublisher(config)
# publisher.publish(issue)
# No need to manually close, but you can if desired:
# publisher.close()

