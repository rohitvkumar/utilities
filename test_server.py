#!/usr/bin/env python
from bottle import route, run, HTTPResponse, request
import glob
import jprops
import json
import os
import requests
import subprocess

@route('/health')
def health():
    return "Job is running\n"

@route('/iftttRegistrationTokenGet', method='POST')
def iftttRegistrationTokenGet():
    return "Job is running\n"

def main():
    run(host='0.0.0.0', port=5000, server='paste')

if __name__ == '__main__':
    main()
