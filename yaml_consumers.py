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
            'name' : All(str, Length(min=1))
            }]
        },
        required=True)

def construct_variable_dict():
    '''
    Construct a variable dict
    '''
    return {
        'CONTAINER'   : os.getenv('TANDEM_CONTAINER', 'UnknownContainer'),
        'ENVIRONMENT' : os.getenv('TANDEM_ENVIRONMENT', 'UnknownEnvironment'),
        'DATACENTER'  : os.getenv('TANDEM_DATACENTER', 'UnknownDatacenter'),
        'SERVICE'     : os.getenv('TANDEM_SERVICE', 'UnknownService'),
        'HOSTNAME'    : os.getenv('TANDEM_HOSTNAME', 'UnknownHostname')
        }

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
            topic['type'] = 'dynconfigTopicConsumer'
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