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

def generate_context():
    """Generate the context for an issue."""
    return ersissue.Context(
        cwd=os.getcwd(),
        file_name=__file__,
        function_name=inspect.currentframe().f_back.f_code.co_name,  # getting the caller function name
        host_name=socket.gethostname(),
        line_number=inspect.currentframe().f_back.f_lineno,  # getting the caller's line number
        package_name="unknown",
        application_name="python"
    )

def exception_to_issue(exc: Exception) -> ersissue.SimpleIssue:
    """Converts an exception to a SimpleIssue."""
    context = generate_context()
    return ersissue.SimpleIssue(
        context=context,
        name=type(exc).__name__,
        message=str(exc),
        time=datetime.now().timestamp(),
        severity=SeverityLevel.ERROR.value,
        inheritance=["python_issue", type(exc).__name__]
    )

def create_issue(message, name="GenericPythonIssue", severity=SeverityLevel.INFO.value, exc=None):
    """Create an ERS issue with minimal user input."""
    current_time = datetime.now().timestamp()
    context = generate_context()
    issue = ersissue.SimpleIssue(
        context=context,
        name=name,
        message=message,
        time=current_time,
        severity=severity
    )

    # Setting the inheritance based on whether the issue comes from an exception or not
    if exc:
        issue.inheritance.extend(["python_issue", type(exc).__name__])
    else:
        issue.inheritance.extend(["python_issue", name])

    return issue


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

    def publish_simple_message(self, message):
    issue = create_issue(message, name="GenericPythonIssue")
    issue_chain = ersissue.IssueChain(
        final=issue,
        session=os.getenv('DUNEDAQ_PARTITION', 'Unknown'),
        application="python",
        module=__name__  # this sets the module to the name of the current module,it can be adjusted as necessary
    )
    return self.publish(issue_chain)

    def publish(self, issue):
        """Publish an ERS issue to the Kafka topic."""
        return self.producer.send(self.topic, key=issue.session, value=issue)


    def __del__(self):
        """Destructor-like method to clean up resources."""
        if self.producer:
            self.producer.close()

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


