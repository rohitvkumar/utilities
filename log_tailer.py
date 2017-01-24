#!/usr/bin/env python

import argparse
import atexit
import os
from pykafka import KafkaClient, Topic
from pykafka.common import OffsetType
import sys
import traceback
import time
import threading

verbose = False
simulated = False

def list_topics(client, name):
    if not name:
        for item in client.topics.items():
            print item
    else:
        topic = client.topics[name]
        print 'Name: ', topic.name
        print 'Partition Count: ', len(topic.partitions)
        for key in topic.partitions:
            part = topic.partitions[key]
            #print 'Id: {id}, ISR: {isr}, Leader: {ld}, Replicas: {r}'.format(id=part.id, isr=part.isr, ld=part.leader, r=part.replicas)
            
def read_topic(consumer, key_filter, message_filter, print_key, print_meta, rule):
    rules = {'all':all, 'any':any}
    for message in consumer:
        if key_filter:
            if not message.partition_key:
                continue
            if not rules[rule](x in message.partition_key for x in key_filter):
                continue
        if message_filter:
            if not message.value:
                continue
            if not rules[rule](x in message.value for x in message_filter):
                continue
        if print_meta:
            print '{p}:{o}:'.format(p=message.partition.id, o=message.offset),
        if print_key:
            print '{}:'.format(message.partition_key),
        print message.value
        
        
def wait_for_threads():
    seconds = 0
    while threading.active_count() != 0:
        print 'Waiting for threads to exit.'
        time.sleep(1)
        seconds += 1
        if seconds == 10:
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME")
    parser.add_argument("-k", "--key-filter", help="Filter term for key", metavar="TERM", action="append")
    parser.add_argument("-m", "--message-filter", help="Filter term for message", metavar="TERM", action="append")
    parser.add_argument("-L", "--list", help="List topic(s)", action="store_true")
    parser.add_argument("-o", "--offset", help="Offset to read from", choices=[None, 'beginning', 'end'], default=None)
    parser.add_argument("-M", "--Metadata", help="Include metadata about the message", action="store_true")
    parser.add_argument("-K", "--Key", help="Include message key", action="store_true")
    parser.add_argument("-r", "--rule", help="Match all or any", choices=['all', 'any'], default='all')
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print args
    
    bootstrap = '{0}:{1}'.format(args.broker, args.port)
    client = KafkaClient(hosts=bootstrap)
    
    try:
        
        if args.list:
            list_topics(client, args.topic)
            sys.exit(0)
        
        if args.topic:
            topic = client.topics[args.topic]
            if not args.offset or args.offset == 'end':
                consumer = topic.get_simple_consumer(reset_offset_on_start=True, auto_offset_reset=OffsetType.LATEST)
            elif args.offset == 'beginning':
                consumer = topic.get_simple_consumer(reset_offset_on_start=True, auto_offset_reset=OffsetType.EARLIEST)
            read_topic(consumer, args.key_filter, args.message_filter, args.Key, args.Metadata, args.rule)
    except KeyboardInterrupt as e:
        print "Stopped"
        consumer.stop()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()