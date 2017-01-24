#!/usr/bin/env python
import argparse
import collections
import time
import json
import jprops
import subprocess32 as subprocess
import sys
import tempfile

verbose = False
cache_file = '/TivoData/metadata_cache.json'
kafka_conf_file = '/TivoData/etc/kafka.conf'
current_time_in_ms = lambda: int(round(time.time() * 1000))

class KafkaMetadataError(Exception):
    """Base class for Kafka Metadata related exceptions"""
    pass

def print_ts(msg):
    print "%s %s " %(time.strftime('%Y%m%d %H:%M:%S'), msg)

def get_metadata(path, broker, id):
    """
    Driver function. Obtains broker.id of the node. Runs kafkacat to get broker metadata.
    Parses the output and writes it to a file.
    :param broker: broker host:port
    :returns: Metadata dict if successful. Else raises exception
    """
    print_ts('Start fetching metadata')
    kafka_topics = None
    broker_id = None
    try:
        if not id:
            broker_id = get_broker_id()
        else:
            broker_id = id
    except Exception as e:
        raise KafkaMetadataError('Unable to get broker_id: %s' % repr(e))
    if verbose:
        print_ts('Fetching metadata for Broker Id %s' %(broker_id))

    try:
        # Using temporary files as the metadata returned can be potentially very large
         with tempfile.TemporaryFile() as fout, tempfile.TemporaryFile() as ferr:
            cmd = [path, '-b', broker, '-L', '-J']
            if verbose:
                print ' '.join(cmd)
            proc = subprocess.Popen(cmd, stdout=fout, stderr=ferr)
            proc.communicate(timeout=30)
            
            fout.seek(0)
            kafka_topics = json.load(fout)

    except subprocess.CalledProcessError as se:
        raise KafkaMetadataError('Unable to execute kafkacat using subprocess: %s, %s' % (se.output, se.returncode))
    except Exception as e:
        raise KafkaMetadataError('Unable to execute kafkacat: %s' % repr(e))

    if verbose:
        print_ts(kafka_topics)
    if kafka_topics:
        metadata = parse_metadata(kafka_topics, broker_id)
        return persist_metadata(metadata)
    else:
        raise KafkaMetadataError('Unable to fetch kafka topics')


def get_broker_id(conf_file=kafka_conf_file):
    """
    Get broker.id of the current node.
    :param conf_file: kafka.conf file path
    :return: int (broker id)
    """
    if verbose:
        print_ts('Fetching Broker Id' %())

    with open(conf_file) as config_file:
        kafka_conf = jprops.load_properties(config_file, collections.OrderedDict)
        return int(kafka_conf['broker.id'])

def parse_metadata(data, broker_id):
    """
    Parse kafka-topics.sh output. Obtain broker health.
    Write metadata to a json file.
    :param data: string
    :param broker_id: int
    :return: bool. True if successful. Else raises exception
    """
    if verbose:
        print_ts('Parsing metadata for Broker Id %s' %(broker_id))
    metadata = {'ts_MS': current_time_in_ms()}
    topics = collections.defaultdict(list)
    number_under_replicated_partitions = 0
    number_offline_partitions = 0
    names = []
    try:
        for topic in data["topics"]:
            for partition in topic["partitions"]:
                replicas = [x["id"] for x in partition["replicas"]]
                isr = [x["id"] for x in partition["isrs"]]
                leader = partition['leader']
                
                partitions = {}
                partitions['partition'] = partition['partition']
                partitions['leader'] = partition['leader']
                partitions['replicas'] = replicas
                partitions['isr'] = isr
                #topics[topic['topic']].append(partitions)

                if broker_id in replicas and broker_id not in isr:
                    number_under_replicated_partitions += 1
                    topics[topic['topic']].append(partitions)
                    names.append(topic['topic'])
                elif int(leader) < 0:
                    number_offline_partitions += 1
                    topics[topic['topic']].append(partitions)
                    names.append(topic['topic'])
                    
        metadata['under_replicated_partitions'] = number_under_replicated_partitions
        metadata['offline_partitions'] = number_offline_partitions
        metadata['names'] = names
        metadata['raw_metadata'] = topics
        if verbose:
            print_ts(metadata)
            
    except Exception as e:
        raise KafkaMetadataError('Unable to process kafkacat json output: %s' % repr(e))

    return metadata

def persist_metadata(metadata):
    try:
        with open(cache_file, 'w') as outfile:
            json.dump(metadata, outfile, indent=2)
    except IOError as e:
        raise KafkaMetadataError('Unable to write cache file [%s]: %s' % (output_file, repr(e)))
    print_ts('Metadata fetch complete')
    return True


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--broker", default="127.0.0.1:9092",
                        help="broker host and port")
    parser.add_argument("-v", "--verbose", default=False, action='store_true',
                        help="Verbose mode (for debugging)")
    parser.add_argument("-i", "--id", required=True, type=int,
                        help="Broker Id")
    parser.add_argument("-p", "--path", default='/home/tivo/bin/kafkacat',
                        help="Broker Id")
    args = parser.parse_args(argv)
    global verbose
    verbose = args.verbose
    try:
        if get_metadata(args.path, args.broker, args.id):
            return 0
    except KafkaMetadataError as e:
        print_ts('Unable to execute/parse kafkacat json: %s' % repr(e))
        raise e
    except Exception as ge:
        print_ts('Error updating metadata cache: %s' % repr(ge))
    return 1


if __name__ == '__main__':
    try:
        ret = main(sys.argv[1:])
        sys.exit(ret)
    except Exception as ex:
        print_ts('Exception fetching metadata : ' + str(ex.args[0]))
        sys.exit(1)
