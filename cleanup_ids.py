#!/usr/bin/env python

import argparse
import os
import sys


def main():
    parser = argparse.ArgumentParser(description="Clean up ids created by old partitioner.")
    
    parser.add_argument("-b", "--broker", help="Kafka broker address.", required=True)
    
    args = parser.parse_args()
    
    dc = args.broker.split(".")[1]
    
    print dc
    
if __name__ == "__main__":
    main()