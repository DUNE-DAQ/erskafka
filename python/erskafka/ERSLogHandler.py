from erskafka.ERSPublisher import ERSPublisher, ERSException, SeverityLevel  # import the custom exception and the SeverityLevel enum
#import erskafka.ERSPublisher as erspub
import logging
import os

class ERSHandler(logging.Handler):
    def __init__(self, session:str="plasorak-drunc-ers-test", kafka_address:str="monkafka.cern.ch:30092", kafka_topic:str="ers_stream"):
        super().__init__()
        os.environ['DUNEDAQ_PARTITION']=session
        self.session = session
        self.kafka_address = kafka_address
        self.kafka_topic = kafka_topic
        self.publisher = ERSPublisher({"bootstrap": self.kafka_address, "topic": self.kafka_topic})

    def _convert_logging_level_to_ers_level(self, level):
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
                print("CRITICAL error")
                return SeverityLevel.FATAL
            case _:
                return SeverityLevel.INFO

    def emit(self, record):
        ers_level = self._convert_logging_level_to_ers_level(record.levelno)
        print(f'level: {ers_level.name}, {ers_level}, message: {record.msg}')
        self.publisher.publish(
            message = record.msg,
            severity = ers_level.name
        )
