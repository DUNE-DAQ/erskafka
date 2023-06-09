#!/usr/bin/env python3

import issue_pb2
from kafka import KafkaConsumer
import json
from threading import Thread
import socket
import os

class  ERSSubscriber:
    def __init__(self, config) :
        self.bootstrap = config["bootstrap"]
        if ( 'group_id' in config.keys() ) :
            self.group = config["group_id"]
        else:
            self.group = ""
        self.timeout = config["timeout"]
        self.running = False
        self.functions = dict()
        self.thread = Thread(target=message_loop, args=self)

                        
    #    print("From Kafka server:",bootstrap)

    def default_id(self):  
        node = socket.gethostname()
        process = os.getpid()
        thread = threading.get_ident()
        id = "{}-{}-{}".format(node, process, thread)
        return id
           
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
        if (self.group == ""): group_id = self.default_id()
        else: group_id = self.group

        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap,
                                 group_id=group_id, 
                                 client_id=self.default_id(),
                                 consumer_timeout_ms=self.timeout)
        
        while ( self.running ) :
            
            try:
                message_it = iter(consumer)
                message = next(message_it)
                
            except :
                pass
            
            

        
