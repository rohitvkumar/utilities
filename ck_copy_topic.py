#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import os
from confluent_kafka import Consumer, KafkaError, TopicPartition, Producer, OFFSET_BEGINNING, OFFSET_END
import sys
import traceback
import time
import threading
from xmlrpclib import _datetime_type

verbose = False
simulated = False

def get_offsets_for_timestamps(client, name, list_partitions, rec_time):
    #print(list_partitions)
    tps = [TopicPartition(name, p, rec_time) for p in list_partitions]
    offset_metadata = client.offsets_for_times(tps)
    offsets = {}
    for met in offset_metadata:
        offsets[met.partition] = met.offset
    return offsets

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
    parser.add_argument("-k", "--key-filter", help="Only copy messages with these terms in the key", metavar="TERM", action="append")
    parser.add_argument("--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-p", "--partition", help="Topic partitions to consume from", metavar="partition", type=int, action="append")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-o", "--offset", help="Copy all data or most recent", choices=['beginning', 'end'], default='beginning')
    parser.add_argument("-O", "--abs-offset", help="Absolute offset", type=int, default=0)
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
                'default.topic.config': {'auto.offset.reset': 'earliest' if args.offset == 'beginning' else 'latest'}}
        
        consumer = Consumer(consumer_conf)
        partitions = args.partition if args.partition else consumer.list_topics(args.from_topic).topics.get(args.from_topic).partitions.keys()
        
        if args.abs_offset == 0:
            tps = [TopicPartition(args.from_topic, p, OFFSET_BEGINNING if args.offset == 'beginning' else OFFSET_END) for p in partitions]
        elif args.abs_offset > 0:
            tps = [TopicPartition(args.from_topic, p, args.abs_offset) for p in partitions]
        else:
            end_offs = get_offsets_for_timestamps(consumer, args.from_topic, args.partition, OFFSET_END)
            tps = [TopicPartition(args.from_topic, p, end_offs[p] + args.abs_offset) for p in partitions]
        print(tps)
        
        consumer.assign(tps)
        
        producer = Producer({'bootstrap.servers': bootstrap_dst})
        
        if verbose:
            print ("Source topic: {0}".format(consumer.list_topics(args.from_topic).topics))
            print ("Destination topic: {0}".format(producer.list_topics(args.to_topic).topics))
        
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
                if args.key_filter and message.key():
                    if not any(x in message.key() for x in args.key_filter):
                        continue
                if verbose:
                    print ("%s:%d:%d: key=%s value=%s" % (message.topic(), message.partition(),
                                                  message.offset(), message.key(),
                                                  message.value()))
                    dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp()[1] / 1000))
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
