#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import os
from kafka import KafkaConsumer, TopicPartition
import sys
import traceback
import time
import threading
from pprint import pprint
from operator import delslice

verbose = False
simulated = False

trace_records = {}

def list_topics(client, name):
    if not name:
        for item in sorted(client.topics()):
            print(item)
    else:
        partitions = client.partitions_for_topic(name)
        tps = [TopicPartition(name, p) for p in partitions]
        tpts = client.end_offsets(tps)
        print(tpts)
        print ('Name: ', name)
        print ('Partition Count: ', len(partitions))
            
def read_topic(consumer,
               key_filter, 
               message_filter, 
               print_key, 
               print_meta, 
               print_ts, 
               suppress, 
               date_filter, 
               rule,
               total=None):
    rules = {'all':all, 'any':any}
    if verbose:
        print("Launching loop to read the messages.")
    counter = 0
    for message in consumer:
        counter += 1
        if counter % 1000000 == 0:
            print ("Read {0} messages.".format(counter).rjust(200), end='\r', file=sys.stderr)
        if total and counter > total:
            return
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
        if date_filter:
            dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp/1000))
            if dateStr not in date_filter:
                continue
        if print_meta:
            print ('{p}+{o}+'.format(p=message.partition, o=message.offset), end="")
        if print_key:
            print ('{}+'.format(message.key), end="")
        if print_ts:
            print ('{}+'.format(time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(message.timestamp/1000))), end="")
        if message.key:
            count_tup = trace_records.get(message.key, (0,0))
            adds = count_tup[0]
            dels = count_tup[1]
            if message.value:
                adds = adds + 1
            else:
                dels = dels + 1
            trace_records[message.key] = (adds,dels)
        
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
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME")
    parser.add_argument("-k", "--key-filter", help="Filter term for key", metavar="TERM", action="append")
    parser.add_argument("-m", "--message-filter", help="Filter term for message", metavar="TERM", action="append")
    parser.add_argument("-L", "--list", help="List topic(s)", action="store_true")
    parser.add_argument("-o", "--offset", help="Offset to read from", choices=['beginning', 'end'], default='end')
    parser.add_argument("-O", "--time-offset", help="How back in time to read from", choices=['1d', '2d', '4d', '1w', '2w', '1m'], default='1w')
    parser.add_argument("-M", "--Metadata", help="Include metadata about the message", action="store_true")
    parser.add_argument("-K", "--Key", help="Include message key", action="store_true")
    parser.add_argument("-e", "--exit-at-end", help="Quit when no new messages read in 5 seconds.", action="store_true")
    parser.add_argument("-r", "--rule", help="Match all or any", choices=['all', 'any'], default='all')
    parser.add_argument("-T", "--Timestamp", help="Print the message timestamp", action="store_true")
    parser.add_argument("-D", "--Date-filter", help="Filter message based on time", metavar="YYYY-MM-DD", action="append")
    parser.add_argument("-S", "--Suppress", help="Print only metadata", action="store_true")
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print (args)
    
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
            partitions = client.partitions_for_topic(args.topic)
            tps = [TopicPartition(args.topic, p) for p in partitions]
            
            timeout_ms = 30000 if args.exit_at_end else float('inf')
            if args.offset == 'end':
                client = KafkaConsumer(
                         enable_auto_commit=False,
                         auto_offset_reset='latest',
                         bootstrap_servers=bootstrap,
                         consumer_timeout_ms=timeout_ms)
            else:
                client = KafkaConsumer(
                         enable_auto_commit=False,
                         auto_offset_reset='earliest',
                         bootstrap_servers=bootstrap,
                         consumer_timeout_ms=timeout_ms)
            
            client.assign(tps)
            client.seek_to_beginning()
            
            if args.time_offset:
                off = args.time_offset
                offtime_ms = 24 * 3600 * 1000
                if off == '2d':
                    offtime_ms *= 2
                if off == '4d':
                    offtime_ms *= 4
                if off == '1w':
                    offtime_ms *= 7
                if off == '2w':
                    offtime_ms *= 14
                if off == '1m':
                    offtime_ms *= 30
                
                print (offtime_ms)
                
                currtime_ms = int(time.time() * 1000)
                timestamps = {}
                for tp in tps:
                    timestamps[tp] = currtime_ms - offtime_ms
                tpts = client.offsets_for_times(timestamps)
                end_tpts = client.end_offsets(tps)
                total = 0
                for tp in tpts:
                    total += end_tpts[tp] - tpts[tp].offset
                    client.seek(tp, tpts[tp].offset)
                    
                print('Total: ', total)
                   
            read_topic(client,
                       args.key_filter, 
                       args.message_filter, 
                       args.Key, 
                       args.Metadata, 
                       args.Timestamp, 
                       args.Suppress,
                       args.Date_filter, 
                       args.rule,
                       total)
            tot_dels = 0
            tot_adds = 0
            morethanonedels = 0
            for key in trace_records.keys():
                count_tup = trace_records[key]
                adds = count_tup[0]
                dels = count_tup[1]
                tot_adds += adds
                tot_dels += dels
                if dels > 1:
                    morethanonedels += 1
                if count_tup[0] < count_tup[1]:
                    print("{0} - adds:{1}, dels:{2}".format(key, count_tup[0], count_tup[1]))
            
            print("Total creates: {0} deletes: {1}".format(tot_adds, tot_dels))
            print("Num records with multiple deletes: {0}".format(morethanonedels))
                    
    except KeyboardInterrupt as e:
        print ("Stopped")
        client.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
