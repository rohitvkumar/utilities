#!/usr/bin/env python

import argparse
import os
import json
import requests
import sys


def anonymize_id(tsn, address):
    tsn = tsn.strip()
    tsn = ''.join([ch.upper() for ch in tsn if ch not in ('-')])
    if not tsn.startswith('tsn:'):
        tsn = 'tsn:' + tsn
    req = {
        "type": "anonymizerExternalIdTranslate",
        "externalId": tsn
    }
    resp = requests.post("http://{addr}/anonymizerExternalIdTranslate".format(addr=address), json=req)
    resp.raise_for_status()
    json_resp = resp.json()
    return "{0} - {1}".format(json_resp["externalId"], json_resp["internalId"])

def main():
    parser = argparse.ArgumentParser(description="Anonymize the tsns.")
    
    parser.add_argument("-a", "--anonymizer", help="Anonymizer address.", default="anonymizer.tec1.tivo.com")
    parser.add_argument("-e", "--external-id", help="External Id to be anonymized.", required=True, action="append")
    parser.add_argument("-f", "--file-ids", help="One external id per line.")
    
    args = parser.parse_args()
    
    #with file(args.file_ids) as f:
    #    msg = f.read()
    
    if args.external_id:
        for id in args.external_id:
            print anonymize_id(id, args.anonymizer)
    
if __name__ == "__main__":
    main()