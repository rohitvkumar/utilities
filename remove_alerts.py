#!/usr/bin/env python

import argparse
import json
import requests


def get_registered_alerts(address, datacenter, environment, container):
    req = {
        "type": "dynconfigAlertSearch",
        "environment": environment,
        "datacenter": datacenter
    }
    if container:
        req["container"] = container
        
    result = requests.post("http://{addr}/dynconfigAlertSearch".format(addr=address), json=req)
    result.raise_for_status()
    
    if verbose:
        print json.dumps(result.json())
    
    return result.json()['dynconfigAlert']

def remove_alert(address, alert):
    req = {
        "type": "dynconfigAlertRemove",
        "environment": alert.get('environment'),
        "datacenter": alert.get('datacenter'),
        "container": alert.get('container'),
        "name": alert.get('name')
    }
    
    if simulated:
        print "http://{addr}/dynconfigAlertRemove".format(addr=address)
        print json.dumps(req)
        return
    
    resp = requests.post("http://{addr}/dynconfigAlertRemove".format(addr=address), json=req)
    resp.raise_for_status()

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-D", "--host", help="Dynconfig host", metavar="hostname", required=True)
    parser.add_argument("-P", "--port", help="Dynconfig port", metavar="port", type=int, default=50000)
    parser.add_argument("-c", "--container", help="Container name", metavar="NAME")
    parser.add_argument("-d", "--datacenter", help="Datacenter name", metavar="NAME", required=True)
    parser.add_argument("-e", "--environment", help="Environment name", metavar="NAME", required=True)
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    service_url = "{0}:{1}".format(args.host, args.port)
    
    alerts = get_registered_alerts( service_url,
                                    args.datacenter,
                                    args.environment,
                                    args.container)
    
    for alert in alerts:
        if verbose:
            print "Removing {0}".format(alert)
        remove_alert(service_url, alert)

if __name__ == "__main__":
    main()