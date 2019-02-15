#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import os
from kafka import KafkaProducer, KafkaConsumer
from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer
import sys
import traceback
import time
import threading
from xmlrpclib import _datetime_type

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
        consumer_conf = {'bootstrap.servers': bootstrap_src,
                'enable.auto.commit': 'false',
                'session.timeout.ms': 6000,
                'group.id': 'log_tailer',
                'api.version.request': True,
                'default.topic.config': {'auto.offset.reset': 'earliest' if args.offset=='beginning' else 'latest'}}
        
        consumer = Consumer(consumer_conf)
        partitions = consumer.list_topics(args.from_topic).topics.get(args.from_topic).partitions.keys()
        tps = [TopicPartition(args.from_topic, p, 0) for p in partitions]
        consumer.assign(tps)
        
        producer = Producer({'bootstrap.servers': bootstrap_dst})
        
        if verbose:
            print ("Source topic:")
            print (consumer.list_topics(args.from_topic))
            print ("Destination topic:")
            print (producer.list_topics(args.to_topic))
        
        counter = 0
        keep_running = True
        while keep_running:
            messages = consumer.consume(500, 5)
            for message in messages:
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        keep_running = False
                        print(msg.error())
                        break
                counter += 1
                print ("Read {0} messages.".format(counter), end='\r')
                if verbose:
                    print ("%s:%d:%d: key=%s value=%s" % (message.topic(), message.partition(),
                                                  message.offset(), message.key(),
                                                  message.value()))
                    dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp()[1]/1000))
                    print (dateStr)
                if not simulated:
                    time_type = message.timestamp()[0]
                    time_stamp = message.timestamp()[1]
                    producer.produce(topic=args.to_topic,
                                  value=message.value(),
                                  partition=message.partition(),
                                  key=message.key(),
                                  timestamp=time_stamp)
                    
            
            producer.flush()
        
    except KeyboardInterrupt as e:
        print ("Stopped")
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
