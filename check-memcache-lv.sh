#!/bin/sh

TSN_FILENAME=$1

tsnArray=$(cat $TSN_FILENAME)

declare -a tsnArray
tsnArray=(`cat "$TSN_FILENAME"`)

toDay=$(date +"%m-%d-%Y")
currDir=$PWD
logFileName=`basename $0 .sh`
logDir="$currDir/logs/$toDay"
mkdir -p $logDir
logPath=$logDir/$TSN_FILENAME-$logFileName-$(date +"%I_%M_%S_%p").out

echo "*********************$logPath**********************"

declare -a arr=("10.160.30.102" "10.160.30.103" "10.160.30.104" "10.160.30.105" "10.160.30.106" "10.160.30.107" "10.160.30.108" "10.160.30.109" "10.160.30.110" "10.160.30.111" "10.160.30.112" "10.160.30.121" "10.160.30.122" "10.160.30.123" "10.160.30.124" "10.160.30.125" "10.160.30.127" "10.160.30.128" "10.160.30.129" "10.160.30.130" "10.160.30.131" "10.160.30.132" "10.160.30.133" "10.160.30.134" "10.160.30.135" "10.160.30.136" "10.160.30.137" "10.160.30.138" "10.160.30.139" "10.160.30.140" "10.160.30.141" "10.160.30.142" "10.160.30.143" "10.160.30.144" "10.160.30.84" "10.160.30.85" "10.160.30.86")

noOfTsns=${#tsnArray[@]}
echo "no of tsns $noOfTsns"
#echo $batchSize


for i in ${arr[@]}
do
   for TSN in "${tsnArray[@]}"
   do 
    bodyInfoKey="dm%3ASentBodyEntitlementsToSDToken-tsn%3A$TSN"
    excludedPartnerIds="dm%3AexcludedPartnerIds-tsn%3A:$TSN"

    echo "retrieving cache key $bodyInfoKey for tsn: $TSN in server $i" | tee -a $logPath
    echo "get $bodyInfoKey" | nc $i 3307 | tee -a $logPath

    echo "retrieving cache key $excludedPartnerIds for tsn: $TSN in server $i" | tee -a $logPath
    echo "get $excludedPartnerIds" | nc $i 3307 | tee -a $logPath
    
   done
done

