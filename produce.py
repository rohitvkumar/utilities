#!/usr/bin/env python

import argparse
import atexit
import os
from kafka import KafkaProducer
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
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-b", "--broker", help="Kafka bootstrap broker", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Kafka bootstrap broker port", metavar="port", type=int, default=9092)
    parser.add_argument("-t", "--topic", help="Topic name", metavar="NAME", required=True)
    parser.add_argument("-k", "--key", help="Message key", metavar="TERM", default=None)
    parser.add_argument("-d", "--domain", help="Key delimiter", action="store_true")
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
        if args.message_file:
            with file(args.message_file) as f:
                msg = f.read()
        else:
            msg = args.message
        
        if args.domain:
            str = ""
            for line in msg.splitlines():
                str = str + line + '\r\n'
            if not str.endswith('\r\n\r\n'):
                str = str.strip('\r\n')
            msg = str
            if verbose:
                print "Msg: ", msg
            
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        if verbose:
            print producer.partitions_for(args.topic)
        producer.send(args.topic, value=msg, key=args.key, partition=args.partition)
        producer.flush()
        
    except KeyboardInterrupt as e:
        print "Stopped"
        producer.close()

if __name__ == "__main__":
    atexit.register(wait_for_threads)
    main()