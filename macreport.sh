#!/bin/bash

emailList="dvalkenaar@cornerstonetechnologies.com"
tmp=/tmp/macreport.$$
hostname=`hostname`

printf "Host: \n" >> $tmp
host -a `hostname` >> $tmp

printf "ifconfig: \n" >> $tmp
ifconfig >> $tmp

printf "\nOS Information\n" >> $tmp
sw_vers >> $tmp

printf "\nID: " >> $tmp
(id -u; id -g) >> $tmp

printf "\nUsers\n" >> $tmp
dscl . list /Users UniqueID | grep -v "^_" >> $tmp

printf "\nDomain Configuration\n" >> $tmp
dsconfigad -show >> $tmp

mail -s "$hostname report" $emailList < $tmp

printf "Thank you! Report sent to $emailList.\n"
 
rm $tmp

sleep 3
