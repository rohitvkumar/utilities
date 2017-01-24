#!/usr/bin/env python
import os
import sys
import csv

def parseconf(filePath):
    topics = []
    topic = dict(name='', replication='1', partitions='1')
    new_topic = False
    keys = ['name', 'replication', 'partitions']
    keys += ['cleanup_policy', 'retention_ms', 'retention_bytes', 'delete_retention_ms']
    keys += ['min_cleanable_dirty_ratio','min_insync_replicas']

    with open(filePath) as conf:
        for line in conf:
            line = line.strip(' \n\r\t')
            # skip comments
            if line.startswith('#'):
                continue
            if len(line) == 0:
                continue
            if line.startswith('['):
                new_topic = line == '[topic]'
                # Store the previous topic
                if len(topic['name']) > 0:
                    topics.append(topic)
                    topic = dict(name='', replication='1', partitions='1')
            elif new_topic:
                parts = line.partition('=')
                name = parts[0].strip(' \n\r\t')
                value = parts[2].strip(' \n\r\t')
                if name in keys:
                    if len(value) > 0:
                        topic[name] = value

        # Handle the last section
        if len(topic['name']) > 0:
            topics.append(topic)
    return topics


def main():
    topics = parseconf(sys.argv[1])
    with open(sys.argv[2], 'wb') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'replication', 'partitions', 'cleanup_policy'])
        for topic in topics:
            writer.writerow([topic['name'], topic['replication'], topic['partitions'], topic['cleanup_policy']])

if __name__ == "__main__":
    main()