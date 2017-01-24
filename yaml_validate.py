#!/usr/bin/env python

import argparse
import json
import os
import sys
import voluptuous
from voluptuous import Schema, Required, Any, All, Length, Exclusive
import yaml

# The topic registry schema
check_schema = Schema({
        'topics' : [{
            'topic' : All(str, Length(min=1)),
            'name' : All(str, Length(min=1)),
            'archivePolicy' : Any('none', 'original'),
            'backupPolicy' : Any('none', 'original'),
            'cleanupPolicy' : {
                               Exclusive('dataRetentionTime', 'duration') : Any('oneHour', 'twoHours', 'fourHours',
                                    'eightHours', 'twelveHours', 'oneDay',
                                    'oneWeek', 'twoWeeks', 'fourWeeks'),
                               Exclusive('tombstoneRetentionTime', 'duration') : Any('oneHour', 'twoHours', 'fourHours',
                                    'eightHours', 'twelveHours', 'oneDay',
                                    'oneWeek', 'twoWeeks', 'fourWeeks')
                               },
            'configurationPolicy' : Any('debugLogs', 'eventData', 'objectData', 'sourceOfTruth'),
            'partitionPolicy' : Any('single', 'low', 'normal', 'high'),
            'privacyPolicy' : Any('none', 'pii', 'anonymized'),
            'visibilityPolicy' : Any('private', 'public')
            }]
        },
        required=True)

def main():
    with open(sys.argv[1]) as yaml_stream:
        test = yaml.safe_load(yaml_stream)
        try:
            conf = check_schema(test)
        except Exception as e:
            print str(e)
            sys.exit(1)
        for topic in conf['topics']:
            # expand variables of the form {FOO} using the variable_dict
            #topic['topic'] = topic['topic'].format(**variable_dict)
            # fix the cleanupPolicy objects
            topic['type'] = 'dynconfigTopic'
            confPol = topic['configurationPolicy']
            if confPol in ['objectData', 'sourceOfTruth']:
                cleanupTime = topic['cleanupPolicy'].get('tombstoneRetentionTime')
                topic['cleanupPolicy'] = {
                                          'type': 'dynconfigTopicTombstoneRetentionPolicy',
                                          'duration': cleanupTime
                                          }
            else:
                cleanupTime = topic['cleanupPolicy'].get('dataRetentionTime')
                topic['cleanupPolicy'] = {
                                          'type': 'dynconfigTopicDataRetentionPolicy',
                                          'duration': cleanupTime
                                          }
            request = {
                        'type' : 'dynconfigTopicStore',
                        'topic' : topic
                        }
            print json.dumps(request)
            print json.dumps(request, indent=2)

if __name__ == '__main__':
    main()