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


declare -a arr=("192.168.141.7" "192.168.141.8" "192.168.141.14" "192.168.141.50" "192.168.141.57" "192.168.141.58")

noOfTsns=${#tsnArray[@]}
echo "no of tsns $noOfTsns"
#echo $batchSize


for i in ${arr[@]}
do
   for TSN in "${tsnArray[@]}"
   do 
    anonAccountId="dm%3AAnonAccountId-tsn%3A$TSN"
    bodyInfoKey2="dm%3ASentBodyEntitlementsToSDToken-tsn%3A$TSN"
    excludedPartnerIds="dm%3AexcludedPartnerIds-tsn%3A:$TSN"

    echo "retrieving cache key $bodyInfoKey2 for tsn: $TSN in server $i" | tee -a $logPath
    echo "get $bodyInfoKey2" | nc $i 3307 | tee -a $logPath
    
    #echo "retrieving cache key $excludedPartnerIds for tsn: $TSN in server $i" | tee -a $logPath
    #echo "get $excludedPartnerIds" | nc $i 3307 | tee -a $logPath

    echo "retrieving cache key $anonAccountId for tsn: $TSN in server $i" | tee -a $logPath
    echo "get $anonAccountId" | nc $i 3307 | tee -a $logPath

   done
done

