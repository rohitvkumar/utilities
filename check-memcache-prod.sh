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

declare -a arr=("10.10.30.11" "10.10.30.12" "10.10.30.13" "10.10.30.14" "10.10.30.15" "10.10.30.16" "10.10.30.17" "10.10.30.18" "10.10.30.181" "10.10.30.182" "10.10.30.183" "10.10.30.184" "10.10.30.185" "10.10.30.186" "10.10.30.187" "10.10.30.188" "10.10.30.189" "10.10.30.19" "10.10.30.190" "10.10.30.191" "10.10.30.192" "10.10.30.193" "10.10.30.194" "10.10.30.195" "10.10.30.196" "10.10.30.197" "10.10.30.198" "10.10.30.199" "10.10.30.203" "10.10.30.204" "10.10.30.210" "10.10.30.29" "10.10.30.52" "10.10.30.59" "10.10.30.71" "10.10.30.72")

noOfTsns=${#tsnArray[@]}
echo "no of tsns $noOfTsns"
#echo $batchSize


for i in ${arr[@]}
do
   for TSN in "${tsnArray[@]}"
   do 
    bodyInfoKey="dm%3ASentBodyEntitlementsToSDToken-tsn%3A$TSN"
    
    echo "retrieving cache key $bodyInfoKey for tsn: $TSN in server $i" | tee -a $logPath
    echo "get $bodyInfoKey" | nc $i 3307 | tee -a $logPath
    
   done
done

