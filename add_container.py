#!/usr/bin/env python
import argparse
import os
import requests
import subprocess32 as subprocess

verbose = False
simulated = False


def get_current_container_list(dynconfig, env, name):
    

def main():
    parser = argparse.ArgumentParser(description="Add/remove containers to server.")
    
    parser.add_argument("-v", "--verbose", help="Verbose output.", action="store_true")
    parser.add_argument("-s", "--simulated", help="Debugging only - no changes will be made to cluster.", action="store_true")
    parser.add_argument("-d", "--dynconfig", help="Dynconfig host", metavar="hostname", required=True)
    parser.add_argument("-p", "--port", help="Dynconfig port", metavar="port", type=int, default=50000)
    parser.add_argument("-s", "--server", help="Server name", metavar="HOST_NAME", required=True)
    parser.add_argument("-e", "--environment", help="Environment name", metavar="ENV_NAME", required=True)
    parser.add_argument("-c", "--container", help="Container name", metavar="CONTAINER", required=True, action='append')
    parser.add_argument("-r", "--remove", help="Remove containers", action='store_true')
    
    args = parser.parse_args()
    
    global verbose
    global simulated
    
    verbose = args.verbose
    simulated = args.simulated
    
    dynconfig = args.dynconfig + ':' + args.port
    

if __name__ == "__main__":
    main()