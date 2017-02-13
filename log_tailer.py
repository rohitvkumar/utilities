#!/usr/bin/env python

import argparse
import atexit
import os
from kafka import KafkaConsumer
import sys
import traceback
import time
import threading

verbose = False
simulated = False

def list_topics(client, name):
    if not name:
        for item in sorted(client.topics()):
            print item
    else:
        partitions = client.partitions_for_topic(name)
        print 'Name: ', name
        print 'Partition Count: ', len(partitions)
            
def read_topic(consumer, key_filter, message_filter, print_key, print_meta, rule):
    rules = {'all':all, 'any':any}
    for message in consumer:
        if key_filter:
            if not message.key:
                continue
            if not rules[rule](x in message.key for x in key_filter):
                continue
        if message_filter:
            if not message.value:
                continue
            if not rules[rule](x in message.value for x in message_filter):
                continue
        if print_meta:
            print '{p}:{o}:'.format(p=message.partition, o=message.offset),
        if print_key:
            print '{}:'.format(message.key),
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
    parser.add_argument("-o", "--offset", help="Offset to read from", choices=['beginning', 'end'], default='end')
    parser.add_argument("-M", "--Metadata", help="Include metadata about the message", action="store_true")
    parser.add_argument("-K", "--Key", help="Include message key", action="store_true")
    parser.add_argument("-e", "--exit-at-end", help="Quit when no new messages read in 5 seconds.", action="store_true")
    parser.add_argument("-r", "--rule", help="Match all or any", choices=['all', 'any'], default='all')
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print args
    
    bootstrap = ['{0}:{1}'.format(args.broker, args.port)]
    
    try:
        client = KafkaConsumer(args.topic,
                         enable_auto_commit=False,
                         auto_offset_reset='earliest',
                         bootstrap_servers=bootstrap)
        if args.list:
            list_topics(client, args.topic)
            sys.exit(0)
        
        if args.topic:
            timeout_ms = 5000 if args.exit_at_end else float('inf')
            if args.offset == 'end':
                client = KafkaConsumer(args.topic,
                         enable_auto_commit=False,
                         auto_offset_reset='latest',
                         bootstrap_servers=bootstrap,
                         consumer_timeout_ms=timeout_ms)
            else:
                client = KafkaConsumer(args.topic,
                         enable_auto_commit=False,
                         auto_offset_reset='earliest',
                         bootstrap_servers=bootstrap,
                         consumer_timeout_ms=timeout_ms)
            read_topic(client, args.key_filter, args.message_filter, args.Key, args.Metadata, args.rule)
    except KeyboardInterrupt as e:
        print "Stopped"
        client.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()