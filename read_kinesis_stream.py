#!/usr/bin/env python

import argparse
from boto import kinesis
import os
import time

verbose = False
simulated = False

def main():
    parser = argparse.ArgumentParser(description="Read the latest messages from a kinesis stream shard")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-r", "--region", help="AWS region", metavar="REGION E.g. us-west-1", default="us-west-1")
    parser.add_argument("-n", "--stream-name", help="Name of the stream to consume", metavar="NAME", required=True)
    parser.add_argument("-i", "--shard-id", help="Shard Id", metavar="ID", default="shardId-000000000000")
    parser.add_argument("-D", "--describe", help="Describe the stream", action="store_true")    
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    if verbose:
        print args
        
    conn = kinesis.connect_to_region(args.region)
    shard_id = args.shard_id #we only have one shard!
    shard_it = conn.get_shard_iterator(args.stream_name, shard_id, "LATEST")["ShardIterator"]
    
    while 1==1:
        out = conn.get_records(shard_it, limit=2)
        shard_it = out["NextShardIterator"]
        print out;
        time.sleep(0.2)

if __name__ == "__main__":
    main()