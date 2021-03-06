#!/usr/bin/env python
'''
This tool posts one or more messages to a given topic. The messages can be entered from
a file, as a parameter to the script or through stdin.

Usage: 
Enter messages through stdin, same key for all messages.
~/tools/scripts/produce.py -b <broker> -t <topic> -k <key> <-|ENTER
DO/1....
....
{....}<-|ENTER
eom<-|ENTER
DO/1....
....
{....}<-|ENTER
eom<-|ENTER
DO/1....
....
{....}<-|ENTER
eom<-|ENTER
DO/1....
....
{....}<-|ENTER
eom<-|ENTER

Enter messages through stdin, different key for each messages.
~/tools/scripts/produce.py -b <broker> -t <topic> <-|ENTER
DO/1....
....
{....}<-|ENTER
key<-|ENTER
int:123456<-|ENTER
DO/1....
....
{....}<-|ENTER
key<-|ENTER
int:123456<-|ENTER
DO/1....
....
{....}<-|ENTER
key<-|ENTER
int:123456<-|ENTER
DO/1....
....
{....}<-|ENTER
key<-|ENTER
int:123456<-|ENTER


~/tools/scripts/produce.py -b <broker> -t <topic> -k <key> -f <file> <-|ENTER       -* only one message per file
~/tools/scripts/produce.py -b <broker> -t <topic> -k <key> -m <msg> <-|ENTER      -* only one message param

Author: rvalsakumar
'''

import argparse
import atexit
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
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
            
def djb2a_hash(Key):
    hash = 5381
    mask = 0xffffffff
    for c in Key:
        hash = (((hash * 33) & mask) + ord(c)) & mask
    return hash

def marshal_to_wire(lines):
    protocol = None
    str = ""
    len_headers = None
    for line in lines:                
        if not protocol and (line.startswith("DO") or line.startswith("MRPC")):
            protocol = line.split()[0]
        elif not len_headers:
            if not line.endswith('\r\n'):
                line = line.strip() + '\r\n'
            str = str + line
            if line == '\r\n':
                len_headers = len(str)
        else:
            str = str + line
        
    len_msg = len(str) - len_headers
    return "{0} {1} {2}\r\n{3}".format(protocol, len_headers, len_msg, str)

def read_msg_stdin(key):
    print ""
    in_key = raw_input("Message key <{}> (Enter new to change): ".format(key))
    if in_key:
        key = in_key
    print "Message (to end message type 'eom' on new line): "
    lines = []
    while True:
        line = sys.stdin.readline().strip()
        if line in ['EOM', 'eom']:
            break
        lines.append(line)
    return key, lines

def read_msg_file(name):
    with file(name) as f:
        lines = []
        while True:
            line = file.readline()
            if not line:
                break
            line = line.strip()
            if line in ['EOM', 'eom']:
                break
            lines.append(line)
        return lines
            
            
def main():
    parser = argparse.ArgumentParser(description="Produce one or more messages to a topic.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME", required=True)
    parser.add_argument("-k", "--key", help="Message key", metavar="TERM", default=None)
    parser.add_argument("-d", "--domain", help="Marshal as TivoEnvelope", action="store_true")
    parser.add_argument("-M", "--middlemind-hash", help="Use middlemind hash for partition", action="store_true")
    parser.add_argument("-m", "--message", help="Message", metavar="TERM")
    parser.add_argument("-f", "--message-file", help="Message payload")
    parser.add_argument("-P", "--partition", help="Partition to post message", default=None)
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print args
    
    bootstrap = '{0}:{1}'.format(args.broker, args.port)
        
    try:
        one_shot = args.message_file or args.message
        if args.message_file:
            msg_lines = read_msg_file(args.message_file)
        elif args.message:
            msg_lines = args.message.splitlines()
        
        print "Initializing producer, please wait..."
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        partition = args.partition
        if args.middlemind_hash:
            partition_count = len(producer.partitions_for(args.topic))
            partition = djb2a_hash(args.key) % partition_count
            
        msg_key = args.key
        while True:
            if not one_shot:
                msg_key, msg_lines = read_msg_stdin(msg_key)
                
            if msg_lines[0].startswith('DO/') or msg_lines[0].startswith('MRPC/'):
                args.domain = True
                
            if args.domain:
                msg = marshal_to_wire(msg_lines)
                
            if verbose:
                print "Key: {0} Msg: {1}".format(msg_key, msg)
                    
            if simulated:
                print "Producing to topic: {}, key: {}, partition: {}".format(args.topic, msg_key, partition)
            else:
                future = producer.send(args.topic, value=msg, key=msg_key, partition=partition)
                producer.flush()
                record_metadata = None
                try:
                    record_metadata = future.get(timeout=10)
                except KafkaError:
                    print "Error: {}".format(record_metadata)
                    break
                else:
                    print "Message produced to p:{} o:{}".format(record_metadata.partition, record_metadata.offset)
            if one_shot:
                break
        
    except KeyboardInterrupt as e:
        print "Stopped"
        producer.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()