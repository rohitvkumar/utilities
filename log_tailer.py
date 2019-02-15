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

verbose = False
simulated = False

def list_topics(client, name):
    if not name:
        for item in sorted(client.topics()):
            print(item)
    else:
        partitions = client.partitions_for_topic(name)
        print ('Name: ', name)
        print ('Partition Count: ', len(partitions))
        
        tps = [TopicPartition(name, p) for p in partitions]
        start_tpts = client.beginning_offsets(tps)
        end_tpts = client.end_offsets(tps)
        total = 0
        for tp in tps:
            total += end_tpts[tp] - start_tpts[tp]
            print("Partition:{0} - Start:{1} End:{2}".format(tp.partition, start_tpts[tp], end_tpts[tp]))
        print("Total messages: {}".format(total))
                        
def read_topic(consumer,
               key_filter,
               message_filter,
               message_filter_exclusion,
               print_key,
               print_meta,
               print_ts,
               suppress,
               date_filter,
               rule,
               exit_at_end,
               end_offsets,
               count_only,
               partition):    
    rules = {'all':all, 'any':any}
    if verbose:
        print("Launching loop to read the messages.")
        print(end_offsets)
    counter = 0
    tot_msgs = 0
    msg_offset = -1
    for message in consumer:
        if exit_at_end:
            if message.partition in end_offsets:
                if (message.offset > end_offsets[message.partition]):
                    print("Done with partition", message.partition)
                    end_offsets.pop(message.partition, None)
                if not end_offsets:
                    break
            else:
                continue
        counter += 1
        if verbose:
            print ("Read {:,} messages.".format(counter).rjust(200), end='\r', file=sys.stderr)
        if key_filter:
            if not message.key:
                continue
            if not rules[rule](x in message.key for x in key_filter):
                continue
        if message_filter_exclusion:
            if not message.value:
                continue
            if rules[rule](x in message.value for x in message_filter_exclusion):
                continue
        if message_filter:
            if not message.value:
                continue
            if not rules[rule](x in message.value for x in message_filter):
                continue
        if date_filter:
            dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp / 1000))
            if dateStr not in date_filter:
                continue
        if print_meta:
            print ('{p}+{o}+'.format(p=message.partition, o=message.offset), end="")
        if print_key:
            print ('{}+'.format(message.key), end="")
        if print_ts:
            print ('{}:{}+'.format(message.timestamp, time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(message.timestamp / 1000))), end="")
        if suppress:
            print ("")
        else:
            if count_only:
                tot_msgs += 1
            else:
                if message.value:
                    print (message.value)
            # print ("")
    if count_only:
        print("Total message read: {0}".format(tot_msgs))
        
        
def wait_for_threads():
    seconds = 0
    while threading.active_count() != 0:
        print ('Waiting for threads to exit.', file=sys.stderr)
        time.sleep(1)
        seconds += 1
        if seconds == 10:
            sys.exit(0)

def time_tag_to_ms(off):
    offtime_ms = 3600 * 1000  # Set it to 1h by default
    if off == '1h':
        offtime_ms *= 1
    if off == '4h':
        offtime_ms *= 4
    if off == '12h':
        offtime_ms *= 12
    if off == '1d':
        offtime_ms *= (1 * 24)
    if off == '2d':
        offtime_ms *= (2 * 24)
    if off == '4d':
        offtime_ms *= (4 * 24)
    if off == '1w':
        offtime_ms *= (7 * 24)
    if off == '2w':
        offtime_ms *= (14 * 24)
    if off == '1m':
        offtime_ms *= (30 * 24)
    return offtime_ms

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-c", "--num-to-read", help="Only read this many messages.", type=int)
    parser.add_argument("-D", "--Date-filter", help="Filter message based on time", metavar="YYYY-MM-DD", action="append")
    parser.add_argument("-e", "--exit-at-end", help="Quit when no new messages read in 5 seconds.", action="store_true")
    parser.add_argument("-H", "--time-offset-hrs", help="How back in time in hours to read from", type=int)
    parser.add_argument("-i", "--time-offset-mins", help="How back in time in mins to read from", type=int)
    parser.add_argument("-j", "--Items-count", help="Only print the count of items.", action="store_true")
    parser.add_argument("-k", "--key-filter", help="Filter term for key", metavar="TERM", action="append")
    parser.add_argument("-K", "--Key", help="Include message key", action="store_true")
    parser.add_argument("-L", "--list", help="List topic(s)", action="store_true")
    parser.add_argument("-m", "--message-filter", help="Filter term for message", metavar="TERM", action="append")
    parser.add_argument("-M", "--Metadata", help="Include metadata about the message", action="store_true")
    parser.add_argument("-N", "--count-only", help="Only count the number of messages.", action="store_true")
    parser.add_argument("-o", "--offset", help="Offset to read from", choices=['beginning', 'end'], default='end')
    parser.add_argument("-O", "--time-offset", help="How back in time to read from", choices=['1h', '4h', '12h', '1d', '2d', '4d', '1w', '2w', '1m'])
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-P", "--partition", help="Topic partition to consume from", metavar="partition", type=int)
    parser.add_argument("-r", "--rule", help="Match all or any", choices=['all', 'any'], default='all')
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-S", "--Suppress", help="Print only metadata", action="store_true")
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME")
    parser.add_argument("-T", "--Timestamp", help="Print the message timestamp", action="store_true")
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-x", "--message-filter-exclude", help="Filter term for message exclusion", metavar="TERM", action="append")
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print (args)
    
    bootstrap = ['{0}:{1}'.format(args.broker, args.port)]
    
    try:
        timeout_ms = 30*1000
        client = KafkaConsumer(
                         enable_auto_commit=False,
                         auto_offset_reset='earliest',
                         bootstrap_servers=bootstrap,
                         consumer_timeout_ms=timeout_ms)
        
        if args.list:
            list_topics(client, args.topic)
            sys.exit(0)
        
        if not args.topic:
            parser.error("A topic name is required.")
        
        if args.partition:
            tps = [TopicPartition(args.topic, args.partition)]
        else:
            partitions = client.partitions_for_topic(args.topic)
            tps = [TopicPartition(args.topic, p) for p in partitions]
        client.assign(tps)
        beg_tpts = client.beginning_offsets(tps)
        end_tpts = client.end_offsets(tps)
        end_offsets = {}
        for tp in tps:
            end_offsets[tp.partition] = end_tpts[tp]
        total = None
        if args.time_offset or args.time_offset_hrs or args.time_offset_mins:
            if args.time_offset:
                offtime_ms = time_tag_to_ms(args.time_offset)
            if args.time_offset_hrs:
                offtime_ms = args.time_offset_hrs * 3600 * 1000
            if args.time_offset_mins:
                offtime_ms = args.time_offset_mins * 60 * 1000
            currtime_ms = int(time.time() * 1000)
            timestamps = {}
            for tp in tps:
                timestamps[tp] = currtime_ms - offtime_ms
            time_tpts = client.offsets_for_times(timestamps)
            total = 0
            for tp in time_tpts:
                start_offset = time_tpts[tp].offset if time_tpts[tp] and time_tpts[tp].offset else beg_tpts[tp]
                total += end_tpts[tp] - start_offset
                client.seek(tp, start_offset)
        elif args.offset == 'end':
            client.seek_to_end()
            total = 0
        else:
            client.seek_to_beginning()
            total = 0
            for tp in tps:
                start_offset = beg_tpts[tp]
                total += end_tpts[tp] - start_offset
                client.seek(tp, start_offset)
                
        if args.num_to_read:
            total = args.num_to_read
                
        print('Difference between start and end offsets: {:,}'.format(total), file=sys.stderr)
            
        if (args.Items_count):
            sys.exit(0)
        
        while end_offsets:
            read_topic(client,
                       args.key_filter,
                       args.message_filter,
                       args.message_filter_exclude,
                       args.Key,
                       args.Metadata,
                       args.Timestamp,
                       args.Suppress,
                       args.Date_filter,
                       args.rule,
                       args.exit_at_end,
                       end_offsets,
                       args.count_only,
                       args.partition)
    except KeyboardInterrupt as e:
        print ("Stopped", file=sys.stderr)
        client.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
