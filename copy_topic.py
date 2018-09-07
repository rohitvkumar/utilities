#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import os
from kafka import KafkaProducer, KafkaConsumer
import sys
import traceback
import time
import threading

verbose = False
simulated = False

def wait_for_threads():
    seconds = 0
    while threading.active_count() != 0:
        print ('Waiting for threads to exit.')
        time.sleep(1)
        seconds += 1
        if seconds == 10:
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-f", "--from-topic", help="Topic name", metavar="NAME", required=True)
    parser.add_argument("-F", "--from-broker", help="Kafka bootstrap broker source", metavar="hostname", required=True)
    parser.add_argument("-P", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-S", "--offset", help="Copy all data or most recent", choices=['beginning', 'end'], default = 'beginning')
    parser.add_argument("-t", "--to-topic", help="Topic name", metavar="NAME", required=True)
    parser.add_argument("-T", "--to-broker", help="Kafka bootstrap broker destination", metavar="hostname")
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print (args)

    if (not args.to_broker):
        args.to_broker = args.from_broker
    bootstrap_src = '{0}:{1}'.format(args.from_broker, args.port)
    bootstrap_dst = '{0}:{1}'.format(args.to_broker, args.port)
        
    try:
        consumer = KafkaConsumer(args.from_topic,
                                 bootstrap_servers=bootstrap_src,
                                 auto_offset_reset='earliest' if args.offset=='beginning' else 'latest',
                                 enable_auto_commit=False,
                                 consumer_timeout_ms=5*60*1000,
                                 api_version=(0,10))         
        producer = KafkaProducer(bootstrap_servers=bootstrap_dst)
        
        if verbose:
            print ("Source topic:")
            print (consumer.partitions_for_topic(args.from_topic))
            print ("Destination topic:")
            print (producer.partitions_for(args.to_topic))
        
        counter = 0
        for message in consumer:
            counter += 1
            print ("Read {0} messages.".format(counter), end='\r')
            if verbose:
                print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
            if not simulated:
                #dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp/1000))
                #print (dateStr)
                producer.send(topic=args.to_topic,
                              value=message.value,
                              partition=message.partition,
                              key=message.key,
                              timestamp_ms=message.timestamp)
                
        
        producer.flush()
        
    except KeyboardInterrupt as e:
        print ("Stopped")
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
