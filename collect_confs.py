#!/usr/bin/env python
import os
import subprocess32
import sys

def main():
    folder = sys.argv[1]
    print "PARENT FOLDER: ", folder
    command = "grep -rl 'cleanup_policy' {0}/* | grep -v py$ | grep -v bck$".format(folder)
    files = subprocess32.check_output(command, shell=True)
    
    for line in files.splitlines():
        file = os.path.join(folder, line)
        
        print "\n\n{0}\n\n".format(line.replace(folder, ''))
        
        with open(file, 'r') as conf:
            print conf.read()
            
if __name__ == "__main__":
    main()