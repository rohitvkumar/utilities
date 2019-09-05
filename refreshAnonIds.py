import argparse
import datetime
import json
import os
import re
import requests
import sys
import subprocess32 as subprocess


def getAnonAccountInfo(s, url, id, cnt):
    trials = 0
    while True:
        try:
            req = {
                "type": "anonAccountInfoGet",
                "bodyId": id
            }
            result = s.post(url, json=req)
            if not result.status_code in [200, 404]:
                print json.dumps(result.json())
            break
        except KeyboardInterrupt:
            raise
        except:
            trials = trials + 1
            if trials > 5:
                print "Exception while getting id: {} @ {}".format(id, cnt)
                sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Delete topics.")
    
    parser.add_argument("-f", "--file", help="File containing ids to refresh.", required=True)
    parser.add_argument("-H", "--host", help="Host name.", required=True)
    parser.add_argument("-i", "--index", help="Start index.", type=int, default=0)
    args = parser.parse_args()
    
    url = "http://{host}:50225/anonAccountInfoGet".format(host=args.host)
    print(url)
    
    s = requests.Session()
    
    with open(args.file) as fp:
        for cnt, line in enumerate(fp):
            if cnt < args.index:
                continue
            id = line.strip('"\r\n')
            if id.startswith('int:'):
                getAnonAccountInfo(s, url, id, cnt)
            if cnt % 10000 == 0:
                currentDT = datetime.datetime.now()
                print "Progress@{}: {}".format(currentDT, cnt)
        currentDT = datetime.datetime.now()
        print "Progress@{}: {}".format(currentDT, cnt)

if __name__ == "__main__":
    main()