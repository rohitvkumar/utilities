#!/usr/bin/env python
from __future__ import print_function
import argparse
import sys


def parse_domain_object(msg):
    newline = '\r\n'
    protocol = ""
    headers = {}
    lines = msg.splitlines()
    status = lines[0].split(' ',3)
    protocol = status[0]
    payload = ""
    if not protocol.startswith("DO/1"):
        raise NotImplementedError("Only supports DomainObjects for now.")
    end_headers = False
    for line in lines[1:]:
        if not line:
            end_headers = True
            continue
        if not end_headers:
            header = line.split(':', 1)
            headers[header[0].strip()] = header[1].strip()
        else:
            payload = payload + line
#     if len(payload) != int(status[2]):
#         print("ERR: The size of the payload does not match the size in the status line", file=sys.stderr)
    return dict(protocol=protocol, headers=headers, payload=payload)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file",
                        help="File containing message to be parsed.", required=True)
    args = parser.parse_args()
    
    with file(args.file) as f:
        print(parse_tivo_envelope(f.read()))

if __name__ == '__main__':
    main()