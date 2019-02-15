#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import os
from confluent_kafka import Consumer, KafkaError, TopicPartition
import sys
import traceback
import time
import threading
from pprint import pprint

verbose = False
simulated = False
outfile = None

'''
Returns a dictionary with key = partition id and value = offset.
'''
def get_offsets_for_timestamps(client, name, list_partitions, rec_time):
    #print(list_partitions)
    tps = [TopicPartition(name, p, rec_time) for p in list_partitions]
    #print(tps)
    offset_metadata = client.offsets_for_times(tps)
    #print(offset_metadata)
    offsets = {}
    for met in offset_metadata:
        offsets[met.partition] = met.offset
    #print(offsets)
    return offsets

'''
Print the list of topic or topic metadata to output.
'''
def list_topics(client, name):
    if not name:
        metadata = client.list_topics()
        topics = metadata.topics
        for item in sorted(topics.keys()):
            print(item)
    else:
        metadata = client.list_topics(name).topics.get(name)
        print ('Name: ', name)
        print ('Partition Count: ', len(metadata.partitions))
        beg_offs = get_offsets_for_timestamps(client, name, metadata.partitions.keys(), 0)
        end_offs = get_offsets_for_timestamps(client, name, metadata.partitions.keys(), -1)
#         print(beg_offs)
#         print(end_offs)
        total = 0
        for p in beg_offs.keys():
            total += end_offs[p] - beg_offs[p]
            print("Partition:{0} - Start:{1} End:{2}".format(p, beg_offs[p], end_offs[p]))
        print("Total messages: {}".format(total))
        
def redirect_to_file(text):
    if outfile:
        original = sys.stdout
        sys.stdout = open(outfile, 'a+')
        print(text)
        sys.stdout = original
    print(text)

                        
def read_topic(client,
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
               tps,
               num_to_read):    
    rules = {'all':all, 'any':any}
    counter = 0
    tot_msgs = 0
    msg_offset = -1
    msg_rem = {}
    msg_del = []
    while end_offsets:
        messages = client.consume(500, 5)
        for message in messages:
            if message is None:
    #             print(end_offsets)
    #             currs = client.position(tps)
    #             posn = {}
    #             for curr in currs:
    #                 posn[curr.partition] = curr.offset
    #             print(posn)
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            if exit_at_end:
                if message.partition() in end_offsets:
                    if ((message.offset() + 1) >= end_offsets[message.partition()]):
                        print("part: {} end: {}, curr {}".format(message.partition(), end_offsets[message.partition()], message.offset()))
                        end_offsets.pop(message.partition(), None)
                else:
                    continue
            counter += 1
            if verbose:
                print ("Read {:,} messages.".format(counter).rjust(200), end='\r', file=sys.stderr)
            
            if num_to_read:
                if counter > num_to_read:
                    end_offsets = {}
                    break
            if key_filter:
                if not message.key():
                    continue
                if not rules[rule](x in message.key() for x in key_filter):
                    continue
            if message_filter_exclusion:
                if not message.value():
                    continue
                if rules[rule](x in message.value() for x in message_filter_exclusion):
                    continue
            if message_filter:
                if not message.value():
                    continue
                if not rules[rule](x in message.value() for x in message_filter):
                    continue
            if date_filter:
                dateStr = time.strftime("%Y-%m-%d", time.gmtime(message.timestamp() / 1000))
                if dateStr not in date_filter:
                    continue
            
            tot_msgs = tot_msgs + 1
            if count_only:
                print ("Read {:,} messages.".format(tot_msgs).rjust(200), end='\r', file=sys.stderr)
                continue
            
            outstr = []
            if print_meta:
                outstr.append ('{p}+{o}+'.format(p=message.partition(), o=message.offset()))
            if print_key:
                outstr.append ('{}+'.format(message.key()))
            if print_ts:
                outstr.append ('{}:{}+'.format(message.timestamp(), time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(message.timestamp()[1] / 1000))))
            if message.value():
                outstr.append (message.value())
            else:
                outstr.append ("NULL")
                
            if message.value():
                msg_rem[message.key()] = message.value()
            else:
                if msg_rem.has_key(message.key()):
                    msg_rem.pop(message.key())
                    msg_del.append(message.key())
                    
    redirect_to_file("The compacted message count: {}".format(len(msg_rem.keys())))
    redirect_to_file("The deleted message count: {}".format(len(msg_del)))
    redirect_to_file("")
    redirect_to_file("Messages in topic after compaction:")
    for key, value in msg_rem.items():
        redirect_to_file ("{0} - {1}".format(key, value))
            
    redirect_to_file("")
    redirect_to_file("")
    redirect_to_file("Keys of deleted messages:")
    redirect_to_file('\n'.join(msg_del))

        
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
    parser.add_argument("-f", "--out-file-path", help="File to store messages.")
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
    parser.add_argument("-P", "--partition", help="Topic partition to consume from", metavar="partition", type=int, action="append")
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
    global outfile
    
    verbose = args.verbose
    simulated = args.simulated
    outfile = args.out_file_path
    
    if verbose:
        print (args)
    
    bootstrap = '{0}:{1}'.format(args.broker, args.port)
    
    try:
        timeout_ms = 30 * 1000
        conf = {'bootstrap.servers': bootstrap,
                'enable.auto.commit': 'false',
                'session.timeout.ms': 6000,
                'group.id': 'log_tailer',
                'default.topic.config': {'auto.offset.reset': 'smallest'}}
        
        client = Consumer(conf)
        
        if args.list:
            list_topics(client, args.topic)
            sys.exit(0)
        
        if not args.topic:
            parser.error("A topic name is required.")
            
        if args.time_offset or args.time_offset_hrs or args.time_offset_mins:
            if args.time_offset:
                offtime_ms = time_tag_to_ms(args.time_offset)
            if args.time_offset_hrs:
                offtime_ms = args.time_offset_hrs * 3600 * 1000
            if args.time_offset_mins:
                offtime_ms = args.time_offset_mins * 60 * 1000
            currtime_ms = int(time.time() * 1000)
            initial_offset = currtime_ms - offtime_ms
        elif args.offset == 'end':
            initial_offset = -1
        else:
            initial_offset = 1104611152000 # Use Jan 1 2005 epoch as beginning offset
        
        if args.partition is not None:
            partitions = args.partition
        else:
            partitions = client.list_topics(args.topic).topics.get(args.topic).partitions.keys()
        
        tps = [TopicPartition(args.topic, p, initial_offset) for p in partitions]
        
        beg_offs = get_offsets_for_timestamps(client, args.topic, partitions, initial_offset)
        end_offs = get_offsets_for_timestamps(client, args.topic, partitions, -1)
        
        total = 0
        for p in tps:
            if beg_offs[p.partition] > -1:
                p.offset = beg_offs[p.partition]
                total += end_offs[p.partition] - beg_offs[p.partition]
        
        if verbose:
            print("Starting offsets: ", beg_offs)
            print("  Ending offsets: ", end_offs)
            
        client.assign(tps)
        
        print('Difference between start and end offsets: {:,}'.format(total), file=sys.stderr)
            
        if (args.Items_count):
            sys.exit(0)
        
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
                   end_offs,
                   args.count_only,
                   tps,
                   args.num_to_read)
    except KeyboardInterrupt as e:
        print ("Stopped", file=sys.stderr)
    finally:
        client.close()


if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
