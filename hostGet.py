#!/usr/bin/env python
import argparse
import json
import requests

disconnected=0
error=0
routeNotFound=0
noDefaultHost=0
total=0

def get_default_host(tsn):
    global disconnected, error, total, routeNotFound, noDefaultHost
    bodyId = 'tsn:{0}'.format(tsn)
    payload = {
               'type': 'defaultHostBodyGet',
               'bodyId': bodyId
               }
    headers = {
               'Accept': 'application/json',
               'BodyId': bodyId
               }
    url = 'http://middlemind-int.sj.tivo.com:2197/mind/mind20?type=defaultHostBodyGet'
    total += 1
    i = 0
    while True:
        try:
            r = requests.post(url, json=payload, headers=headers)
            response = r.json()
            break
        except Exception as ex:
            pass
        i += 1
        if i > 3:
            print "tsn:{0} - ErrorGettingStatus".format(tsn)
            error += 1
            return
    try:
        if 'type' in response and response['type'] == 'error':
            if 'code' in response:
                if response['code'] == 'middlemindError':
                    routeNotFound += 1
                    print "tsn:{0} - DisconnectedMM".format(tsn)
                if response['code'] == 'internalError':
                    noDefaultHost += 1
                    print "tsn:{0} - Standalone".format(tsn)
        if 'isDefaultHost' in response and 'isEndpointConnected' in response:
            if response.get('isDefaultHost') and not response.get('isEndpointConnected'):
                print "tsn:{0} - DisconnectedHost".format(tsn)
                disconnected += 1
    except Exception as ex:
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="File with list of TSNs", required=True)
    parser.add_argument("-l", "--line", help="Line to start from", default=1, type=int)
    
    args = parser.parse_args()
    
    with open(args.file) as in_file:
        line = 0
        for tsn in in_file:
            line += 1
            if line >= args.line:
                get_default_host(tsn.strip())

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        pass
    
    print "Disconnected from host: {0}".format(disconnected)
    print "Disconnected from MM: {0}".format(routeNotFound)
    print "No default host (stand alone): {0}".format(noDefaultHost)
    print "TSNs processed: {0}".format(total)