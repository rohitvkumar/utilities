#!/bin/bash
for i in $(seq -f "%02g" 1 4); do ssh llc-cassandra-tp1-$i.tpc2.tivo.com 'docker stop llc-linear-contentid-lookup'; done
for i in $(seq -f "%02g" 1 4); do ssh llc-cassandra-production-$i.tpc1.tivo.com 'docker stop llc-linear-contentid-lookup'; done
