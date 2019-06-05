#!/usr/bin/env python

from __future__ import print_function
import argparse
import json
import os
import requests
import sys
from confluent_kafka import Producer, KafkaError, TopicPartition,  OFFSET_BEGINNING,  OFFSET_END, KafkaException
from kafka.vendor import six

# https://github.com/apache/kafka/blob/0.8.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L244
def murmur2(data):
    """Pure-python Murmur2 implementation.
    Based on java client, see org.apache.kafka.common.utils.Utils.murmur2
    Args:
        data (bytes): opaque bytes
    Returns: MurmurHash2 of data
    """
    # Python2 bytes is really a str, causing the bitwise operations below to fail
    # so convert to bytearray.
    if six.PY2:
        data = bytearray(bytes(data))

    length = len(data)
    seed = 0x9747b28c
    # 'm' and 'r' are mixing constants generated offline.
    # They're not really 'magic', they just happen to work well.
    m = 0x5bd1e995
    r = 24

    # Initialize the hash to a random value
    h = seed ^ length
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = ((data[i4 + 0] & 0xff) + 
            ((data[i4 + 1] & 0xff) << 8) + 
            ((data[i4 + 2] & 0xff) << 16) + 
            ((data[i4 + 3] & 0xff) << 24))
        k &= 0xffffffff
        k *= m
        k &= 0xffffffff
        k ^= (k % 0x100000000) >> r  # k ^= k >>> r
        k &= 0xffffffff
        k *= m
        k &= 0xffffffff

        h *= m
        h &= 0xffffffff
        h ^= k
        h &= 0xffffffff

    # Handle the last few bytes of the input array
    extra_bytes = length % 4
    if extra_bytes >= 3:
        h ^= (data[(length & ~3) + 2] & 0xff) << 16
        h &= 0xffffffff
    if extra_bytes >= 2:
        h ^= (data[(length & ~3) + 1] & 0xff) << 8
        h &= 0xffffffff
    if extra_bytes >= 1:
        h ^= (data[length & ~3] & 0xff)
        h &= 0xffffffff
        h *= m
        h &= 0xffffffff

    h ^= (h % 0x100000000) >> 13  # h >>> 13;
    h &= 0xffffffff
    h *= m
    h &= 0xffffffff
    h ^= (h % 0x100000000) >> 15  # h >>> 15;
    h &= 0xffffffff
    
    return h
        

def main():
    parser = argparse.ArgumentParser(description="Fix invalid records.")
    
    parser.add_argument("--dc", help="Inception DC.", required=True)
    parser.add_argument("--env", help="Inception Env.", required=True)
#     parser.add_argument("--id", help="Anon body id.", required=True)
#     parser.add_argument("--body", help="Payload.", required=True)
    parser.add_argument("--file", help="file.", required=True)
    
    args = parser.parse_args()
    
    print("Args: {}".format(args))
    
    dc = args.dc
    env = args.env
#     id = args.id.strip()
#     body = args.body
    file = args.file
    
#     if not id.startswith('int:'):
#         print('Only anonymized body ids allowed')
#         sys.exit(1)
    
    conf = {'bootstrap.servers': "kafka.{0}.tivo.com:9092".format(dc),
            'enable.auto.commit': 'false',
            'acks': 'all',
            'session.timeout.ms': 6000,
            'group.id': '{}.{}.llc-accountinfo-update.dead-letter-consumer'.format(dc, env),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    
    ai_topic = "{0}.{1}.llc-accountinfo-update.anonAccountInfo".format(dc, env)
        
    client = Producer(conf)
    
    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
    
    try:
        with open(file) as fp:
            for line in fp:
                toks = line.split(None, 1)
                id = toks[0].strip()
                body = toks[1].strip()
                hash = (murmur2(id) & 0x7fffffff)
                partition = hash % 12
                hdrs = "SchemaVersion: 33\r\nObjectId: {}\r\nObjectType: anonAccountInfo\r\n\r\n".format(id)
                first = "DO/1 {} {}\r\n".format(len(hdrs), len(body))
                msg = first + hdrs + body
                print(id + " " + msg)
                client.produce(ai_topic, value=msg, key=id, partition=partition, callback=delivery_callback)
                client.poll(1)
                
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        client.flush()
    
if __name__ == "__main__":
    main()