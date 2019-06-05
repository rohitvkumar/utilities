#!/usr/bin/env python

from __future__ import print_function
import argparse
import json
import os
import requests
import sys
from confluent_kafka import Consumer, KafkaError, TopicPartition,  OFFSET_BEGINNING,  OFFSET_END, KafkaException


def get_anon_tcid(dc, env, anonFeId, defPid):
    url = "http://anonymizer.{0}.tivo.com/anonymizerPartnerInternalIdTranslate".format(dc)
    payload =   {
                    "type": "anonymizerPartnerInternalIdTranslate",
                    "idType": "partnerCustomerId:production",
                    "partnerId": "tivo:pt.{0}".format(defPid),
                    "internalId": anonFeId
                }
    response = requests.post(url, json=payload)
    
    if (response.status_code == 200):
        partnerExternalId = response.json()['partnerExternalId']
        url = "http://anonymizer.{0}.tivo.com/anonymizerExternalIdTranslate".format(dc)
        payload =   {
                        "type": "anonymizerExternalIdTranslate",
                        "externalId": partnerExternalId
                    }
        response = requests.post(url, json=payload)
        
        if (response.status_code == 200):
            return response.json()['internalId']
    else:
        return "NoPartnerMapInGA"
        
    return None

def get_anon_accountinfo_tcid(dc, env, bodyId):
    url = "http://llc-accountinfo-update-{}.{}.tivo.com:50225/anonAccountInfoGet".format(env, dc)
    payload =   {
                    "type":"anonAccountInfoGet",
                    "bodyId":bodyId
                }
    response = requests.post(url, json=payload)
    
    if (response.status_code == 200):
        anonTcid = response.json()['anonAccountId']
        if anonTcid.startswith('int:'):
            print ("Found tcid {} for {}".format(anonTcid, bodyId), file=sys.stderr)
            return (anonTcid, response.json())
        else:
            return (None, response.json())
    
    return (None, None)

def main():
    parser = argparse.ArgumentParser(description="Fix invalid records.")
    
    parser.add_argument("--dc", help="Inception DC.", required=True)
    parser.add_argument("--env", help="Inception Env.", required=True)
    parser.add_argument("--off", help="Offset to start from.", type=int, default=OFFSET_BEGINNING)
    parser.add_argument("--file", help="File to send output.")
    
    args = parser.parse_args()
    
    print("Args: {}".format(args))
    
    dc = args.dc
    env = args.env
    
    conf = {'bootstrap.servers': "kafka.{0}.tivo.com:9092".format(dc),
            'enable.auto.commit': 'false',
            'session.timeout.ms': 6000,
            'group.id': '{}.{}.llc-accountinfo-update.dead-letter-consumer'.format(dc, env),
            'default.topic.config': {'auto.offset.reset': 'smallest'}}
    
    dl_topic = "{0}.{1}.llc-accountinfo-update.dead-letter".format(dc, env)
    ai_topic = "{0}.{1}.llc-accountinfo-update.anonAccountInfo".format(dc, env)
        
    client = Consumer(conf)
    
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
#     client.subscribe([dl_topic], on_assign=print_assignment)
    
    metadata = client.list_topics(dl_topic).topics.get(dl_topic)
    
    parts = metadata.partitions.keys()
    
    tps = [TopicPartition(dl_topic, p, args.off) for p in parts]
    
    client.assign(tps)
    
    try:
        more = True
        while more:
            messages = client.consume(500, 5)
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        more = False
                        break
                    else:
                        raise KafkaException(msg.error())
                if msg.value() is None:
                    continue
                if msg.key() is None:
                    continue
                # Proper message
    #             sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
    #                              (msg.topic(), msg.partition(), msg.offset(),
    #                               str(msg.key())))
                print ("Read {:,} messages.".format(msg.offset()).rjust(200), end='\r', file=sys.stderr)
                if 'GA failed to return tcid' not in msg.value():
                    continue
                
                anonTcid_svc = get_anon_accountinfo_tcid(dc, env, msg.key())
                if anonTcid_svc[0]:
                    continue
                    
                dl_msg = json.loads(msg.value())
                if 'details' in dl_msg:
                    dts = dl_msg['details']
                    tokens = dts.split()
                    anonFeid = tokens[-1].strip(" .")
                    anonTcid = get_anon_tcid(dc, env, anonFeid, 3095)
                    if anonTcid and anonTcid.startswith("int:"):
                        msopid = "tivo:pt.3095"
                        cvReady = True
                        if anonTcid_svc[1]:
                            msopid = anonTcid_svc[1]['msoPartnerId']
                            cvReady = anonTcid_svc[1]['cutoverReady']
                        json_val = {"anonAccountId":anonTcid,"anonFeAccountId":anonFeid,"cutoverReady":cvReady,"msoPartnerId":msopid,"type":"anonAccountInfo"}
                        if anonTcid_svc[1]:
                            msg = '{0} {1}'.format(msg.key(), json.dumps(json_val).replace(" ", ""))
                        else:
                            msg = '{0}    {1}'.format(msg.key(), json.dumps(json_val).replace(" ", ""))
                        if args.file:
                            outfile = open(args.file, "a+")
                            print(msg, file = outfile)
                            outfile.close()
                        print(msg)
                
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets.
        client.close()
    
if __name__ == "__main__":
    main()