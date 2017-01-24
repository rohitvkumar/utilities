#!/usr/bin/env python

import requests


def get_token(user, password, service, scope, realm):
    data = {"scope":scope, "service":service, "account":user}
    r = requests.post(realm, auth=HTTPBasicAuth(user, password), data=data)
    token=find_between( (str(r.content)), 'token":"', '"' )
    return ( token )


def main():
    token = get_token("rvalsakumar", "1HackerWay", "repo-vip.tivo.com:8081", "repository:dockerv2-local:pull,push", "http://repo-vip.tivo.com:8081/artifactory/api/docker/dockerv2-local/v2/token")
    print token

if __name__ == "__main__":
    main()
