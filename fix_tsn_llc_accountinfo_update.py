#!/usr/bin/env python

import argparse
import json
import os
import requests
import sys


def get_anonAccountInfo(url, id):
    response = requests.post(url, json=dict(type="anonAccountInfoGet",bodyId=id))
    return response

def get_internalId(url, id):
    response = requests.post(url, json=dict(type="anonAccountInfoGet",bodyId=id))
    return response

def main():
    parser = argparse.ArgumentParser(description="Fix invalid records.")
    parser.add_argument("-p", "--port", help="LLC Accountinfo Update service VIP.", type=int, default=50225)
    parser.add_argument("--tsn", help="TSN to fix.")
    
    args = parser.parse_args()
    
    host_anon = "http://anonymizer.tpc1.tivo.com/anonymizerExternalIdTranslate"
    host_prod = "http://llc-accountinfo-update-{0}:{1}/anonAccountInfoGet".format("production", args.port)
    host_tp1 = "http://llc-accountinfo-update-{0}:{1}/anonAccountInfoGet".format("tp1", args.port)
    
    for line in sys.stdin.readlines():
        res = get_anonAccountInfo(host, line.strip())
        print "{} - {}".format(line.strip(), res.json())
    
    
    
if __name__ == "__main__":
    main()