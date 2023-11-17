#!/usr/bin/env python

import erskafka.ERSSubscriber as erssub
import google.protobuf.json_format as pb_json
import ers.issue_pb2 as ersissue
import json
import click
import time

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--kafka-address', type=click.STRING, default="monkafka.cern.ch", help="address of the kafka broker")
@click.option('--kafka-port', type=click.INT, default=30092, help='port of the kafka broker')
@click.option('--running-seconds', type=click.INT, default=15, help='Number of seconds of the run')

def cli(kafka_address, kafka_port, running_seconds):

    bootstrap = f"{kafka_address}:{kafka_port}"

    conf = json.loads("{}")
    conf["bootstrap"] = bootstrap
    conf["timeout"] = 400

    sub = erssub.ERSSubscriber(conf)

    sub.add_callback(name="test",
                     function=my_test_function )
    sub.add_callback(name="throwing",
                     function=throwing_function )
    sub.add_callback(name="acceptable",
                     function=acceptable_function,
                     selection=".*")
    sub.add_callback(name="doomed",
                     function=rejected_function,
                     selection="[0-9]+")
                     
    sub.start()
    time.sleep(running_seconds)
    sub.stop()
    

def my_test_function( issue ) :
    print (pb_json.MessageToJson(issue))

def acceptable_function( issue ) :
    print ("Accepted")

def rejected_function( issue ) :
    print ("Rejected")

def throwing_function( issue ) :
    print( issue.non_existent_entry )
        
if __name__ == '__main__':
    cli(show_default=True, standalone_mode=True)
