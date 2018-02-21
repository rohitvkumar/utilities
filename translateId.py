#!/usr/bin/env python

import argparse
import json
import requests
import sys

def external_to_internal(id):
    req = {
        "type": "anonymizerExternalIdTranslate",
        "externalId": id
    }
    
    url = "http://anonymizer.tec1.tivo.com/anonymizerExternalIdTranslate"
        
    result = requests.post(url, json=req)
    result.raise_for_status()
    
    return result.json()['internalId']

def internal_to_external(id):
    req = {
        "type": "anonymizerInternalIdTranslate",
        "internalId": id
    }
    
    url = "http://anonymizer.tec1.tivo.com/anonymizerInternalIdTranslate"
        
    result = requests.post(url, json=req)
    result.raise_for_status()
    
    return result.json()['externalId']

def fe_account_search(pid, pcid):
    url = "http://pdk01.st.tivo.com:8085/mind/mind22?type=feAccountFeDeviceSearch&partnerId={pid}&partnerCustomerId={pcid}".format(pid=pid, pcid=pcid)
    print url
    headers = {'accept': 'application/json'}
    result = requests.get(url, headers=headers)
    result.raise_for_status()
    
    return result.json()

def fe_mso_search(bodyId):
    url = "http://pdk15.sj.tivo.com:8085/mind/mind22?type=feDeviceMsoServiceIdGet&bodyId={bId}".format(bId=bodyId)
    print url
    headers = {'accept': 'application/json'}
    result = requests.get(url, headers=headers)
    result.raise_for_status()
    
    return result.json()


def main():
    parser = argparse.ArgumentParser(description="Translate Ids.")
    
    parser.add_argument("-e", "--external", help="External Id.")
    parser.add_argument("-i", "--internal", help="Internal Id.")
    parser.add_argument("-p", "--partner-id", help="Partner Id.")
    parser.add_argument("-f", "--internal-ids", help="Internal Ids.")
    
    args = parser.parse_args()
    
    print args
    
    if args.external:
        print external_to_internal(args.external.strip('",'))
        
    if args.internal:
        externalId = internal_to_external(args.internal.strip('",\n'))
        print json.dumps(fe_mso_search(externalId), indent=2)
        print ""
        
    if args.internal_ids:
        with open(args.internal_ids) as inp:
            for line in inp:
                externalId = internal_to_external(line.strip())
                print json.dumps(fe_mso_search(externalId), indent=2)
                print ""
        
if __name__ == "__main__":
    main()