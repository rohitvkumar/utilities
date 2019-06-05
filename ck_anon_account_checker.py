#!/usr/bin/env python

from __future__ import print_function
import argparse
import atexit
import json
import os
from confluent_kafka import Consumer, KafkaError, TopicPartition,  OFFSET_BEGINNING,  OFFSET_END
import sys
import traceback
import time
import threading
from tivo_envelope_parser import parse_domain_object
from translateId import internal_to_external 
from pprint import pprint
import requests

verbose = False
simulated = False
outfile = None
fe_host = None
mismatch_ids = {}

def fe_mso_pid_search(bodyId):
    url = "http://{host}.tivo.com:8085/mind/mind22?type=feMsoPartnerIdGet&bodyId={bId}".format(host=fe_host, bId=bodyId)
    headers = {'accept': 'application/json'}
    result = requests.get(url, headers=headers)
    result.raise_for_status()
    return result.json()["partnerId"]

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
        
def verify_mso_pid(text):
    global mismatch_ids
    if text is 'NULL':
        return
    try:
        accInfo = parse_domain_object(text)
        if accInfo['headers']['ObjectType'] == 'anonAccountInfo':
            id = accInfo['headers']['ObjectId']
            tsn = internal_to_external(id)
            fe_mso_pid = fe_mso_pid_search(tsn)
            mso_pid = json.loads(accInfo['payload'])['msoPartnerId']
            if fe_mso_pid != mso_pid:
                mismatch_ids[id] = dict(fe_pid=fe_mso_pid, t_pid=mso_pid)
    except Exception as e:
        print(e)
        pass
        
def redirect_to_file(text):
    if outfile:
        original = sys.stdout
        sys.stdout = open(outfile, 'a+')
        print(text)
        sys.stdout = original
    #print(text)

                        
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
               tps):    
    rules = {'all':all, 'any':any}
    counter = 0
    tot_msgs = 0
    msg_offset = -1
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
                        #print("part: {} end: {}, curr {}".format(message.partition(), end_offsets[message.partition()], message.offset()))
                        end_offsets.pop(message.partition(), None)
                else:
                    continue
            counter += 1
            if verbose:
                print ("Read {:,} messages.".format(counter).rjust(200), end='\r', file=sys.stderr)
            
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
            verify_mso_pid (' '.join(outstr))

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

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("--count-only", help="Only count the number of messages.", action="store_true")
    parser.add_argument("-d", "--Date-filter", help="Filter message based on time", metavar="YYYY-MM-DD", action="append")
    parser.add_argument("-e", "--exit-at-end", help="Quit when no new messages read in 5 seconds.", action="store_true")
    parser.add_argument("-f", "--out-file-path", help="File to store messages.")
    parser.add_argument("-i", "--time-offset-mins", help="Time offset from end in minutes", type=int)
    parser.add_argument("-I", "--time-offset-hrs", help="Time offset from end in hours", type=int)
    parser.add_argument("-j", "--print-offset-delta", help="Only print the count of items.", action="store_true")
    parser.add_argument("-k", "--key-filter", help="Only show messages with this term in the key", metavar="TERM", action="append")
    parser.add_argument("-K", "--Key", help="Print the message key", action="store_true")
    parser.add_argument("-L", "--list", help="List topic(s)", action="store_true")
    parser.add_argument("-m", "--message-filter", help="Only show messages with this term in the body", metavar="TERM", action="append")
    parser.add_argument("-M", "--Metadata", help="Print message metadata", action="store_true")
    parser.add_argument("-o", "--offset", help="Offset to read from", choices=['beginning', 'end'], default='end')
    parser.add_argument("-O", "--abs-offset", help="Absolute offset", type=int, default=0)
    parser.add_argument("--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-p", "--partition", help="Topic partition to consume from", metavar="partition", type=int, action="append")
    parser.add_argument("--rule", help="Match all or any", choices=['all', 'any'], default='all')
    parser.add_argument("--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-s", "--Suppress", help="Don't print the message body", action="store_true")
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME")
    parser.add_argument("-T", "--Timestamp", help="Print the message timestamp", action="store_true")
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-y", "--y-param", help="host for fe search.", default="pdk15.sj")
    parser.add_argument("-x", "--message-filter-exclude", help="Exclude messages with this term in the body", metavar="TERM", action="append")
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    global outfile
    global fe_host
    
    verbose = args.verbose
    simulated = args.simulated
    outfile = args.out_file_path
    fe_host = args.y_param
    
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
        
        env = args.topic.split('.')[1]
        if env == 'staging':
            fe_host = 'pdk01.st'
        elif env == 'production':
            fe_host = 'pdk15.sj'
        elif env == 'tp1':
            fe_host = 'pdk15.tp1'
        
        if args.list:
            list_topics(client, args.topic)
            sys.exit(0)
        
        if not args.topic:
            parser.error("A topic name is required.")
            
        if args.time_offset_hrs or args.time_offset_mins:
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
        end_offs = get_offsets_for_timestamps(client, args.topic, partitions, OFFSET_END)
        
        total = 0
        diff = {}
        for p in tps:
            if args.abs_offset < 0:
                beg_offs[p.partition] = end_offs[p.partition] + args.abs_offset
            elif args.abs_offset > 0:
                if args.abs_offset > beg_offs[p.partition]:
                    beg_offs[p.partition] = args.abs_offset
            if beg_offs[p.partition] == -2:
                p.offset = OFFSET_BEGINNING
            elif beg_offs[p.partition] == -1:
                p.offset = OFFSET_END
                beg_offs[p.partition] = end_offs[p.partition]
            elif beg_offs[p.partition] > -1:
                p.offset = beg_offs[p.partition]
                diff[p.partition] = end_offs[p.partition] - beg_offs[p.partition]
                total += diff[p.partition]
            else:
                print("Unable to process offsets.")
                sys.exit(1)
                
        
        if verbose:
            print(" Starting offsets: ", beg_offs)
            print("   Ending offsets: ", end_offs)
            print("Offset difference: ", diff)
        
        print('Difference between start and end offsets: {:,}'.format(total), file=sys.stderr)
            
        if (args.print_offset_delta):
            sys.exit(0)
            
        client.assign(tps)
        
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
                   tps)
        for key,val in mismatch_ids.items():
            redirect_to_file("{0} fe {1} topic {2}".format(key, val['fe_pid'], val['t_pid']))
    except KeyboardInterrupt as e:
        print ("Stopped", file=sys.stderr)
    finally:
        client.close()


if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()
