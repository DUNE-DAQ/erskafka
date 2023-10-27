import os
import socket
import inspect
import ers.issue_pb2 as ersissue
from datetime import datetime
from kafka import KafkaProducer

from enum import Enum

class SeverityLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    DEBUG = "DEBUG"


class ERSPublisher:

    def __init__(self, config):
        """Initialize the ERSPublisher with given Kafka configurations."""
        self.bootstrap = config['bootstrap']
        self.topic = config.get('topic', 'ers_stream')
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode('utf-8')
        )

    def _generate_context(self):
        """Private method to generate the context for an issue."""
        return ersissue.Context(
            cwd=os.getcwd(),
            file_name=__file__,
            function_name=inspect.currentframe().f_back.f_code.co_name,  # getting the caller function name
            host_name=socket.gethostname(),
            line_number=inspect.currentframe().f_back.f_lineno,  # getting the caller's line number
            package_name="unknown",
            process_id=os.getpid(),
            thread_id=threading.get_ident(),
            user_id=os.getuid(),
            user_name=os.getlogin(),
            application_name="python"
        )

    def create_issue(self, message, name=None, severity=SeverityLevel.INFO.value):
        """Create an ERS issue with minimal user input."""
        current_time = datetime.now().timestamp()
        context = self._generate_context()
        issue = ersissue.SimpleIssue(
            context=context,
            name=name,
            message=message,
            time=current_time
        )
        if severity:
            issue.severity = severity
        return issue

    def publish_simple_message(self, message, name="GenericPythonIssue"):
        issue = self.create_issue(message, name=name)
        issue_chain = ersissue.IssueChain(
            final=issue,
            session=str(issue.time)  # using time as a session identifier
        )
        return self.publish(issue_chain)

    def publish(self, issue):
        """Publish an ERS issue to the Kafka topic."""
        return self.producer.send(self.topic, key=issue.session, value=issue)
#this can be called by the user (it is best practise), but it is dealt by del if not
    def close(self):
        """Explicit method to close the Kafka producer."""
        if self.producer:
            self.producer.close()
#del is not determinist and not best practise, but if the user does not call close, it will close and clean up resources
    def __del__(self):
        """Destructor-like method to clean up resources."""
        self.close()

class ERSException(Exception):
    """Custom exception which can also be treated as an ERS issue."""
    
    def __init__(self, message):
        super().__init__(message)
        self.message = message

# Usage example:
# publisher = ERSPublisher(config)
# publisher.publish_simple_message("This is a simple message.")
# No need to manually close, but you can if desired:
# publisher.close()


