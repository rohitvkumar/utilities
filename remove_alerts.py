#!/usr/bin/env python
"""
Delete alerts from the dynconfig for a given service.

Usage: 
delete_topics.py --dc tec1 --container connected-bodies-webservice --environment bclab1
"""
import argparse
import json
import os
import re
import requests
import subprocess32 as subprocess

verbose = True
simulated = False

def get_registered_alerts(dc, contr, env):
    
    req = {
        "type": "dynconfigAlertSearch"
    }
    
    if (contr):
        req["container"] = contr
        
    if (env):
        req["environment"] = env
    
    url = "http://dynconfig.{dc}.tivo.com:50000/dynconfigAlertSearch".format(dc=dc)
    
    if verbose:
        print url
        print json.dumps(req)
        
    result = requests.post(url, json=req)
    result.raise_for_status()
    
    return result.json()['dynconfigAlert']

def remove_registered_alert(alert):
    req = {
        "type": "dynconfigAlertRemove",
        "container": alert["container"],
        "datacenter": alert["datacenter"],
        "environment": alert["environment"],
        "name": alert["name"]
    }
    
    url = "http://dynconfig.{dc}.tivo.com:50000/dynconfigAlertRemove".format(dc=alert["datacenter"])
    
    if verbose:
        print url
        print json.dumps(req)
        
    if simulated:
        return
        
    result = requests.post(url, json=req)
    result.raise_for_status()

def main():
    parser = argparse.ArgumentParser(description="Delete topics.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-d", "--dc", help="Inception DC tag.", default="tec1")
    parser.add_argument("-c", "--container", help="Inception service/container name.")
    parser.add_argument("-e", "--environment", help="Inception environment name.", required=True)
    args = parser.parse_args()
    
    if args.verbose:
        global verbose
        verbose = args.verbose
        print args
    
    if args.simulated:
        global simulated
        simulated = args.simulated
     
    for alert in get_registered_alerts(args.dc, args.container, args.environment):
        remove_registered_alert(alert)

if __name__ == "__main__":
    main()