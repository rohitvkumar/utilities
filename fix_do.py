#!/usr/bin/env python

import argparse
import os
import sys


def main():
    parser = argparse.ArgumentParser(description="Fix the header line of a DO.")
    
    parser.add_argument("-f", "--payload-file", help="Payload text file.", required=True)
    
    args = parser.parse_args()
    
    with file(args.payload_file) as f:
        msg = f.read()
        
    print msg
    print ""
    
    protocol = "DO/1"
    str = ""
    len_headers = None
    for line in msg.splitlines():                
        if not str and (line.startswith("DO") or line.startswith("MRPC")):
            protocol = line.split()[0]
            continue
        if not len_headers:
            if not line.endswith('\r\n'):
                line = line.strip() + '\r\n'
            str = str + line
            if line == '\r\n':
                len_headers = len(str)
        else:
            str = str + line.strip()
            
    len_msg = len(str) - len_headers
    msg = "{0} {1} {2}\r\n{3}".format(protocol, len_headers, len_msg, str)
    
    print msg
    
if __name__ == "__main__":
    main()