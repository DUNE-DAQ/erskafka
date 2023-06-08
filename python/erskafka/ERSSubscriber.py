#!/usr/bin/env python3

import issue_pb2
from kafka import KafkaConsumer
import json
from threading import Thread

class  ERSSubscriber:
    def __init__(self, config) :
        bootstrap = config["bootstrap"]
        self.functions = dict()
        self.thread = Thread(target=message_loop, args=self)
        self.running = False
    #    print("From Kafka server:",bootstrap)

    
    #    consumer = KafkaConsumer(bootstrap_servers=bootstrap,
     #                            group_id=kafka_consumer_group, 
      #                           client_id=kafka_consumer_id,
       #                          consumer_timeout_ms=kafka_timeout)

       

    def default_id(self):
        
    def add_callback(self, function, name, selection):

    def clear_callbacks(self):
        self.functions.clear()

    def remove_callback(self, name):
        self.functions.pop(name)

    def start(self):
        self.running = True
        self.thread.start()

    def stop(self):
        self.running = False
        self.thread.join()


    def message_loop(self) :
        while ( self.running ) :



        
