#!/usr/bin/env python
import argparse
import os
import subprocess32 as subprocess

verbose = True
simulated = False

def remove_topic(path, zookeeper, topic, force):
    
    if topic.startswith('mirror.') or topic.startswith('global.'):
        raise RuntimeError("Won't delete global or mirrored topics.")
    
    if '.test.' not in topic and not force:
        raise RuntimeError("Topic name does not have a '.test.' in it - won't delete.")
    
    command = []
    command.append(os.path.join(path, 'kafka-topics.sh'))
    command.append('--zookeeper')
    command.append(zookeeper)
    command.append('--delete')
    command.append('--topic')
    command.append(topic)
    
    if verbose:
        print "Running command: ",' '.join(command)
    if simulated:
        print ' '.join(command)
        return 0
    p = subprocess.check_output(' '.join(command), shell=True)
    print p
    

def main():
    parser = argparse.ArgumentParser(description="Delete topics.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("--bin-path", help="Path to kafka binaries.", default="/Users/rvalsakumar/tools/kafka-install/bin")
    
    parser.add_argument("--zookeeper", help="zookeeper address.", metavar='host:port', required=True)
    parser.add_argument("--topic", help="Topic name.", metavar='<topic>', action='append')
    parser.add_argument("--force", help="Force.", action='store_true')
    args = parser.parse_args()
    
    if args.verbose:
        global verbose
        verbose = args.verbose
    
    if args.simulated:
        global simulated
        simulated = args.simulated
     
    for instance in args.topic:
        remove_topic(args.bin_path, args.zookeeper, instance, args.force)

if __name__ == "__main__":
    main()