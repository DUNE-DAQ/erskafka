#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
from threading import Thread
import socket
import os
import re

import ers.issue_pb2 as ersissue

class  ERSSubscriber:
    def __init__(self, config) :
        self.bootstrap = config["bootstrap"]
        if ( 'group_id' in config ) :
            self.group = config["group_id"]
        else:
            self.group = ""
        self.timeout = config["timeout"]
        self.running = False
        self.functions = dict()
        self.thread = Thread(target=message_loop, args=self)

                        
    #    print("From Kafka server:",bootstrap)

    def default_id(self) -> str:  
        node = socket.gethostname()
        process = os.getpid()
        thread = threading.get_ident()
        id = "{}-{}-{}".format(node, process, thread)
        return id
           
    def add_callback(self, function, name, selection) -> bool:
        if ( name in self.function ) : return False
       
        was_running = self.running
        if (was_running) : self.stop()
        
        prog = re.compile(selection)
        self.function[name] = [prog, function]

        if (was_running) : self.start()
        return True

    def clear_callbacks(self):
        if ( self.running ) :
            self.stop()
        self.functions.clear()

    def remove_callback(self, name) -> bool:
        if ( name not in sef.functions.keys() ) : return False

        was_running = self.running
        if (was_running) : self.stop()

        self.functions.pop(name)

        if ( was_running and len(self.functions)>0 ) : self.start()
        return True

    def start(self):
        self.running = True
        self.thread.start()

    def stop(self) :
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

                for function in self.functions.values() :
                    if ( re.match(function[0], message.key ) ) :
                        issue = issue_pb2.IssueChain()
                        issue.ParseFromString( message.value )
                        function[1](issue)
                
            except :
                pass

            
            

        
