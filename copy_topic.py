#!/usr/bin/env python

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
        print 'Waiting for threads to exit.'
        time.sleep(1)
        seconds += 1
        if seconds == 10:
            sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-S", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-s", "--src-topic", help="Topic name", metavar="NAME", required=True)
    parser.add_argument("-d", "--dst-topic", help="Topic name", metavar="NAME", required=True)
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print args
    
    bootstrap = '{0}:{1}'.format(args.broker, args.port)
        
    try:
        consumer = KafkaConsumer(args.src_topic, bootstrap_servers=bootstrap,auto_offset_reset='earliest', enable_auto_commit=False,consumer_timeout_ms=5000)         
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        
        if verbose:
            print "Source topic:"
            print consumer.partitions_for_topic(args.src_topic)
            print "Destination topic:"
            print producer.partitions_for(args.dst_topic)
        
        for message in consumer:
            if verbose:
                print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
            if not simulated:
                producer.send(args.dst_topic, value=message.value, key=message.key)
        
        producer.flush()
        
    except KeyboardInterrupt as e:
        print "Stopped"
        producer.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()