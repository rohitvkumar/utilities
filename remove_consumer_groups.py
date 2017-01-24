#!/usr/bin/env python
import argparse
import os
import subprocess32 as subprocess

verbose = True
simulated = False

def list_consumer_groups(path, broker):
    '''
    Experimental: cleanup the mirrormaker group from broker.
    Try using the new consumer instead??
    '''
    command = []
    command.append(os.path.join(path, 'kafka-consumer-groups.sh'))
    command.append('--bootstrap-server')
    command.append(broker)
    command.append('--new-consumer')
    command.append('--list')
    if verbose:
        print "Running command: ",' '.join(command)
    if simulated:
        print ' '.join(command)
        return 0
    p = subprocess.check_output(' '.join(command), shell=True)
    print p

def remove_consumer_group(path, broker, topic, group):
    '''
    Experimental: cleanup the mirrormaker group from broker.
    Try using the new consumer instead??
    '''
    command = []
    command.append(os.path.join(path, 'kafka-consumer-groups.sh'))
    command.append('--broker')
    command.append(broker)
    command.append('--new-consumer')
    command.append('--delete')
    command.append('--topic')
    command.append(topic)
    if group:
        command.append('--group')
        command.append(group)
    
    if verbose:
        print "Running command: ",' '.join(command)
    if simulated:
        print ' '.join(command)
        return 0
    p = subprocess.check_output(' '.join(command), shell=True)
    print p
    

def main():
    parser = argparse.ArgumentParser(description="Remove consumer groups from a topic.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    
    parser.add_argument("--broker", help="Broker address.", metavar='host:port', required=True)
    parser.add_argument("--topic", help="Topic name.", metavar='<topic>')
    parser.add_argument("--clean", help="Clean up the consumer groups.", action="store_true")
    parser.add_argument("--group", help="Group name.", metavar='<group name>')
    parser.add_argument("--bin-path", help="Path to kafka binaries.", default="/Users/rvalsakumar/tools/kafka-install/bin")
    args = parser.parse_args()
    
    if args.verbose:
        global verbose
        verbose = args.verbose
    
    if args.simulated:
        global simulated
        simulated = args.simulated
     
    if not args.clean:
        list_consumer_groups(args.bin_path, args.broker)
    else:
        remove_consumer_group(args.bin_path, args.broker, args.topic, args.group)
        


if __name__ == "__main__":
    main()