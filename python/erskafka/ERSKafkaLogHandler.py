from erskafka.ERSPublisher import ERSPublisher, ERSException, SeverityLevel  # import the custom exception and the SeverityLevel enum
#import erskafka.ERSPublisher as erspub
import logging
import os

class ERSKafkaLogHandlerOnRootLogger(Exception):
    pass

class ERSKafkaLogHandler(logging.Handler):
    '''
    A logging handler that sends log messages to the ERS system via Kafka.

    Note 1: You need to have Kafka to use this correctly, to see the message in Grafana, you will need to have the full ERS stack running.
    Note 2: IMPORTANT!! you MUST NOT use this handler on the root logger. Use it on a logger that you have created yourself (this is because the root logger is used by Kafka, and it creates a circular dependency).

    Example:
    ```python
    import logging
    from erskafka.ERSKafkaLogHandler import ERSKafkaLogHandler

    logger = logging.getLogger("my_logger") # NOTE, THIS IS NOT THE ROOT LOGGER!!!
    # logger = logging.getLogger() # FORBIDDEN!!! Will should raise an exception
    logger.setLevel(logging.DEBUG)
    handler = ERSKafkaLogHandler(session="test")
    logger.addHandler(handler)
    logger.debug("This is a debug message")
    ```
    '''
    def __init__(
        self,
        session:str="Unknown",
        kafka_address:str="monkafka.cern.ch:30092",
        kafka_topic:str="ers_stream",
    ):
        super().__init__()
        os.environ['DUNEDAQ_PARTITION'] = session
        self.session:str = session
        self.kafka_address:str = kafka_address
        self.kafka_topic:str = kafka_topic

        self.publisher = ERSPublisher(
            bootstrap = kafka_address,
            topic = kafka_topic,
        )

    @staticmethod
    def _convert_logging_level_to_ers_level(level:int) -> SeverityLevel:
        match level:
            case logging.DEBUG:
                return SeverityLevel.DEBUG
            case logging.INFO:
                return SeverityLevel.INFO
            case logging.WARNING:
                return SeverityLevel.WARNING
            case logging.ERROR:
                return SeverityLevel.ERROR
            case logging.CRITICAL:
                return SeverityLevel.FATAL
            case _:
                return SeverityLevel.INFO

    def emit(self, record:logging.LogRecord) -> None:
        ers_level = ERSKafkaLogHandler._convert_logging_level_to_ers_level(record.levelno)

        if record.name == 'root':
            raise ERSKafkaLogHandlerOnRootLogger('To avoid all sorts of undesired behaviours this logger cannot be use on the root logger. Use logging.getLogger("some_name").addHandler(ERSKafkaLogHandler()) instead.')

        success = self.publisher.publish(
            record.msg,
            severity = ers_level.name,
            context_kwargs = dict(
                package_name = str(record.module),
                application_name = str(record.name),
                line_number = record.lineno,
                file_name = str(record.pathname),
                function_name = str(record.funcName),
            )
        )
        if not success:
            print(f'WARNING! Failed to publish: {record.msg} to Kafka')
