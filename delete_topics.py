#!/usr/bin/env python
"""
Delete topics from the kafka cluster and also remove the dynconfigTopic entity
from dynconfig so that the topic is not re-generated. The script will look for
the token 'test' in the topic name before deleting it. To override this and 
delete the topic regardless use the 'force' option.

Usage: 
delete_topics.py --zookeeper zookeeper.tec1.tivo.com:2181 --dynconfig dynconfig.tec1.tivo.com:50000 \
                    --force --topic topic1 --topic topic2
"""
import argparse
import json
import os
import re
import requests
import subprocess32 as subprocess

verbose = True
simulated = False

def get_registered_topics(address):
    req = {
        "type": "dynconfigTopicSearch"
    }
    
    url = "http://{addr}/dynconfigTopicSearch".format(addr=address)
    
    if verbose:
        print url
        print json.dumps(req)
        
    result = requests.post(url, json=req)
    result.raise_for_status()
    
    return result.json()['dynconfigTopic']

def remove_topic_cluster(path, zookeeper, topic, force):
    
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
        return
    
    p = subprocess.check_output(' '.join(command), shell=True)
    print p
    
def remove_topic_dynconfig(address, topic, force):
    
    if topic.startswith('mirror.') or topic.startswith('global.'):
        raise RuntimeError("Won't delete global or mirrored topics.")
    
    if '.test.' not in topic and not force:
        raise RuntimeError("Topic name does not have a '.test.' in it - won't delete.")
    
    req = {
        "type": "dynconfigTopicRemove",
        "name": topic
    }
    
    url = "http://{addr}/dynconfigTopicRemove".format(addr=address)
    
    if simulated:
        print 'POST:', url
        print req
        return
    
    result = requests.post(url, json=req)
    result.raise_for_status()
    
    if verbose:
        print json.dumps(result.json())
    
    return result.json()
    

def main():
    parser = argparse.ArgumentParser(description="Delete topics.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("--bin-path", help="Path to kafka binaries.", default="/Users/rvalsakumar/tools/kafka-install/bin")
    
    parser.add_argument("--zookeeper", help="zookeeper service address.", metavar='host:port', required=True)
    parser.add_argument("--dynconfig", help="Dynconfig service address.", metavar='host:port', required=True)
    parser.add_argument("--list", help="List the topcis matching regex.", metavar='Regex for name')
    parser.add_argument("--topic", help="Name of topic to be deleted.", metavar='<topic_name>', action='append')
    parser.add_argument("--force", help="Force.", action='store_true')
    args = parser.parse_args()
    
    if args.verbose:
        global verbose
        verbose = args.verbose
        print args
    
    if args.simulated:
        global simulated
        simulated = args.simulated
        
    if args.list:
        pr = re.compile(args.list)
        topics = get_registered_topics(args.dynconfig)
        for topic in topics:
            if pr.match(topic['name']):
                print topic['name']
        return
     
    for topic in args.topic:
        remove_topic_dynconfig(args.dynconfig, topic, args.force)
        remove_topic_cluster(args.bin_path, args.zookeeper, topic, args.force)

if __name__ == "__main__":
    main()