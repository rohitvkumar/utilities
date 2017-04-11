#!/usr/bin/env python

import argparse
import filecmp
import itertools
import json
import os
import requests

def get_stored_servers(address):
    req = {
        "type": "dynconfigServerSearch"
    }
        
    result = requests.post("http://{addr}/dynconfigServerSearch".format(addr=address), json=req)
    result.raise_for_status()
    
    if verbose:
        print json.dumps(result.json())
    
    return result.json().get('dynconfigServer')

def probe_service(host, port):
    url = 'http://{0}:{1}/metrics'.format(host,port)
    print url
    try:
        r = requests.head(url, timeout=2)
        print r.status_code
        return True
    except Exception as e:
        return None

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-D", "--host", help="Dynconfig host", metavar="hostname", required=True)
    parser.add_argument("-P", "--port", help="Dynconfig port", metavar="port", type=int, default=50000)
    parser.add_argument("-f", "--folder-path", help="Path to store target files", metavar="PATH", default="/TivoData/etc/targets/")
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    service_url = "{0}:{1}".format(args.host, args.port)
    
    if not os.path.exists(args.folder_path):
        os.makedirs(args.folder_path)
    
    
    
    servers = get_stored_servers(service_url)
    servers = sorted(servers, key=lambda srv: srv.get('environment'))
    
    for k, g in itertools.groupby(servers, key=lambda srv: srv.get('environment')):
        filename = str(k) + '.json'
        file_path_old = os.path.join(args.folder_path, filename)
        file_path_new = os.path.join(args.folder_path, filename + '.new')
        srvs = list(g)
        
        targets = []
        hosts = ['{0}:9080'.format(srv.get('name')) for srv in srvs if probe_service(srv.get('name'), 9080)]
        if hosts:
            targets.append({
                        "targets": hosts,
                        "labels": dict(job='cadvisor')
                      })
        hosts = ['{0}:9100'.format(srv.get('name')) for srv in srvs if probe_service(srv.get('name'), 9100)]
        if hosts:
            targets.append({
                        "targets": hosts,
                        "labels": dict(job='nodeexporter')
                      })
            
        if not targets:
            if os.path.exists(file_path_old):
                os.remove(file_path_old)
            continue
                   
        with open(file_path_new, 'w') as file:
            json.dump(targets, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        if  not os.path.exists(file_path_old) or not filecmp.cmp(file_path_new, file_path_old):
            print "No match"            
            os.rename(file_path_new, file_path_old)
        else:
            print "Match"
            os.remove(file_path_new)

if __name__ == "__main__":
    main()