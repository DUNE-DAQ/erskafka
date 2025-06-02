import inspect
import os
import socket
import time
from enum import IntEnum, auto

import google.protobuf.message as msg
import kafka
from ers.issue_pb2 import Context, IssueChain, SimpleIssue


class SeverityLevel(IntEnum):
    """Map of severity levels from ERS to python."""

    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    FATAL = auto()


class ERSPublisher:
    """Publisher for messages to ERS."""

    def __init__(
        self,
        bootstrap: str = "monkafka.cern.ch:30092",
        topic: str = "ers_stream",
        application_name: str = "python",
        package_name: str = "unknown",
    ) -> None:
        """Constructor."""
        self.application_name = application_name
        self.package_name = package_name
        # Proceed with the rest of the setup
        self.bootstrap = bootstrap

        # The following code ensures that 'monitoring.' is prefixed if it's missing
        if not topic.startswith("monitoring."):
            topic = "monitoring." + topic

        self.topic = topic  # Set the topic attribute correctly

        # The rest of the KafkaProducer initialization...
        self.producer = kafka.KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: v.SerializeToString(),
            key_serializer=lambda k: str(k).encode("utf-8"),
        )

    def publish(
        self,
        message_or_exception: str | Exception,
        severity: SeverityLevel = SeverityLevel.INFO.name,
        name: str | None = None,
        cause: Exception | IssueChain | SimpleIssue | None = None,
        context_kwargs: dict | None = None,
    ) -> kafka.producer.future.FutureRecordMetadata:
        """Create and issue from text or exception and send to to the Kafka."""
        # If the name is not provided, use the name of the exception
        if name is None and isinstance(message_or_exception, Exception):
            name = type(message_or_exception).__name__

        # If the name is still not provided, use a generic name
        if name is None:
            name = "GenericPythonIssue"

        issue_chain = self._create_issue_chain(
            message_or_exception,
            name=name,
            severity=severity,
            cause=cause,
            context_kwargs=context_kwargs,
        )
        return self._publish_issue_chain(issue_chain)

    def _publish_issue_chain(
        self, issue: IssueChain
    ) -> kafka.producer.future.FutureRecordMetadata:
        """Publish an ERS issue_chain to the Kafka topic."""
        return self.producer.send(self.topic, key=issue.session, value=issue)

    def _generate_context(self, context_kwargs: dict | None = None) -> Context:
        """Generate the context for an issue."""
        # Walk back up the stack and find the frame for the original caller
        frame = inspect.currentframe()
        while hasattr(frame, "f_code"):
            co = frame.f_code
            filename = os.path.normcase(co.co_filename)
            if "ERSPublisher.py" not in filename:
                # Found the frame of the original caller
                break
            frame = frame.f_back

        # If no such frame is found, default to the current frame
        if frame is None:
            frame = inspect.currentframe()

        context = {
            "cwd": os.getcwd(),
            "file_name": frame.f_code.co_filename,
            "function_name": frame.f_code.co_name,
            "host_name": socket.gethostname(),
            "line_number": frame.f_lineno,
            "user_name": os.getlogin(),
            "package_name": self.package_name,
            "application_name": self.application_name,
        }

        if context_kwargs:
            context.update(context_kwargs)

        return Context(**context)

    def _exception_to_issue(
        self,
        exc: Exception,
        severity: SeverityLevel = SeverityLevel.WARNING.name,
        context_kwargs: dict | None = None,
    ) -> SimpleIssue:
        """Converts an exception to a SimpleIssue."""
        current_time = time.time_ns()  # Get current time in nanoseconds
        import inspect

        mro = [
            the_class.__name__
            for the_class in inspect.getmro(type(exc))
            if the_class.__name__ != "object"
        ][::-1]
        return SimpleIssue(
            context=self._generate_context(context_kwargs),
            name=type(exc).__name__,
            message=str(exc),
            time=current_time,
            severity=severity,
            inheritance=["PythonIssue", *mro],
        )

    def _create_issue_chain(
        self,
        message: Exception | str,
        name: str = "GenericPythonIssue",
        severity: SeverityLevel = SeverityLevel.INFO.name,
        cause: Exception | SimpleIssue | IssueChain | None = None,
        context_kwargs: dict | None = None,
    ) -> IssueChain:
        """Create an ERS IssueChain with minimal user input."""
        # This creates an issue chain with a given name, message, and severity
        # The message can be a Python exception;
        # in that case the name is also overwritten with the name of the exception
        # The cause can be another issue (chain or simple) or an exception

        current_time = time.time_ns()

        if isinstance(message, Exception):
            # If the issue is created from an exception, set the name and inheritance
            issue = self._exception_to_issue(
                message, severity, context_kwargs
            )  # Use the existing function to create an issue from the exception
            # The time is overwritten as this is the time of the call of the function
            issue.time = current_time
        else:
            # For non-exception issues, continue as normal
            inheritance_list = ["PythonIssue", name]
            issue = SimpleIssue(
                context=self._generate_context(context_kwargs),
                name=name,
                message=message,
                time=current_time,
                severity=str(severity),
                inheritance=inheritance_list,
            )

        issue_chain = IssueChain(
            final=issue,
            session=os.getenv("DUNEDAQ_PARTITION", "Unknown"),
            application="python",
        )

        # Process the cause and add to issue_chain.causes, but not to issue.inheritance
        if cause:
            if isinstance(cause, Exception):
                # Convert exception to a SimpleIssue and append to causes of issue_chain
                cause_issue = self._exception_to_issue(cause, severity)
                issue_chain.causes.extend([cause_issue])
            elif isinstance(cause, SimpleIssue):
                # Add the cause directly to the causes of issue_chain
                issue_chain.causes.extend([cause])
            elif isinstance(cause, IssueChain):
                # Set the last cause of the existing chain as the cause of the new chain
                issue_chain.causes.append(cause.final)
                issue_chain.causes.extend(cause.causes)

        return issue_chain

    def __del__(self) -> None:
        """Destructor-like method to clean up resources."""
        if self.producer:
            self.producer.close()


class ERSError(Exception):
    """Custom error which can also be treated as an ERS issue."""

    def __init__(self, message: msg) -> None:
        """Constructor."""
        super().__init__(message)
        self.message = message
