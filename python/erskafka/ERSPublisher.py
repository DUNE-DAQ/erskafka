import os
import socket
import inspect
import ers.issue_pb2 as ersissue
from datetime import datetime
from kafka import KafkaProducer
import time

from enum import Enum

class SeverityLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    DEBUG = "DEBUG"

def generate_context():
    """Generate the context for an issue."""
    # Walk back up the stack and find the frame for the original caller
    frame = inspect.currentframe()
    while hasattr(frame, "f_code"):
        co = frame.f_code
        filename = os.path.normcase(co.co_filename)
        if 'ERSPublisher.py' not in filename:
            # Found the frame of the original caller
            break
        frame = frame.f_back
    
    # If no such frame is found, default to the current frame
    if frame is None:
        frame = inspect.currentframe()

    return ersissue.Context(
        cwd=os.getcwd(),
        file_name=frame.f_code.co_filename,
        function_name=frame.f_code.co_name,
        host_name=socket.gethostname(),
        line_number=frame.f_lineno,
        package_name="unknown",
        application_name="python"
    )

def exception_to_issue(exc: Exception) -> ersissue.SimpleIssue:
    """Converts an exception to a SimpleIssue."""
    context = generate_context()
    current_time = time.time_ns()  # Get current time in nanoseconds

    return ersissue.SimpleIssue(
        context=context,
        name=type(exc).__name__,
        message=str(exc),
        time=current_time,
        severity=SeverityLevel.WARNING.value,  # Assuming exceptions are always considered WARNING level
        inheritance=["PythonIssue", "IssueFromException", type(exc).__name__]
    )

def create_issue(message, name="GenericPythonIssue", severity=SeverityLevel.INFO.value, cause=None,exc=None):
    """Create an ERS IssueChain with minimal user input."""
    current_time = time.time_ns()
    context = generate_context()

    frame = inspect.currentframe().f_back

    if exc:
        # If the issue is created from an exception, set the name and inheritance
        name = type(exc).__name__  # Use the exception's type name
        inheritance_list = ["PythonIssue", "IssueFromException", name]
        issue = exception_to_issue(exc)  # Use the existing function to create an issue from the exception
    else:
        # For non-exception issues, continue as normal
        inheritance_list = ["PythonIssue", name]

    issue = ersissue.SimpleIssue(
        context=context,
        name=name,
        message=message,
        time=current_time,
        severity=severity,
        inheritance=inheritance_list
    )


    issue_chain = ersissue.IssueChain(
        final=issue,
        session=os.getenv('DUNEDAQ_PARTITION', 'Unknown'),
        application="python"
    )

    # Process the cause and add to issue_chain.causes, but not to issue.inheritance
    if cause:
        if isinstance(cause, Exception):
            # Convert exception to a SimpleIssue and append to causes of issue_chain
            cause_issue = exception_to_issue(cause)
            issue_chain.causes.extend([cause_issue])
        elif isinstance(cause, ersissue.SimpleIssue):
            # Add the cause directly to the causes of issue_chain
            issue_chain.causes.extend([cause])
        elif isinstance(cause, ersissue.IssueChain):
            # Set the final cause of the existing chain as the first cause of the new chain
            issue_chain.causes.append(cause.final)
            issue_chain.causes.extend(cause.causes)

    return issue_chain


class ERSPublisher:
    def __init__(self, config):
        # Initialize self.topic to ensure it's always set
        self.topic = None  # Default value in case the following setup fails

        # Proceed with the rest of the setup
        self.bootstrap = config['bootstrap']
        base_topic = config.get('topic', 'ers_stream')  # Default to 'ers_stream'

        # The following code ensures that 'monitoring.' is prefixed if it's missing
        if 'monitoring.' not in base_topic:
            base_topic = 'monitoring.' + base_topic

        self.topic = base_topic  # Set the topic attribute correctly

        # The rest of the KafkaProducer initialization...
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode('utf-8')
            )

    def publish_simple_message(self, message, severity=SeverityLevel.INFO.value, exc=None):
        issue_chain = create_issue(message, severity=severity, exc=exc)
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


