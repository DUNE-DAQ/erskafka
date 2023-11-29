#!/usr/bin/env python3

from kafka import KafkaConsumer
import json
import threading 
import socket
import os
import re
import logging

import ers.issue_pb2 as ersissue
import google.protobuf.message as msg

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
        self.thread = threading.Thread(target=self.message_loop)

                        
    #    print("From Kafka server:",bootstrap)

    def default_id(self) -> str:  
        node = socket.gethostname()
        user = os.getlogin()
        process = os.getpid()
        thread = threading.get_ident()
        id = "{}-{}-{}-{}".format(node, user, process, thread)
        return id
           
    def add_callback(self, function, name, selection  = '.*') -> bool:
        if ( name in self.functions ) : return False
       
        was_running = self.running
        if (was_running) : self.stop()
        
        prog = re.compile(selection)
        self.functions[name] = [prog, function]

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
        print("Starting run")
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
        
        topics = ["ers_stream"]
        consumer.subscribe(["monitoring." + s for s in topics])

        print("ID:", group_id, "running with functions:", *self.functions.keys())

        while ( self.running ) :
            try:
                message_it = iter(consumer)
                message = next(message_it)
                timestamp = message.timestamp
                key = message.key.decode('ascii')
                ## The key from the message is binary
                ## In order to correctly match an ascii regex, we have to convert
                
                for function in self.functions.values() :
                    if function[0].match(key) :
                        issue = ersissue.IssueChain()
                        issue.ParseFromString( message.value )
                        function[1](issue)
                
            except msg.DecodeError :
                print("Could not parse message")
            except StopIteration :
                pass
            except Exception as e:
                print(e)

        print ("Stop")

        
