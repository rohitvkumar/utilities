if [ $# -ne 1 ]; then
    echo "Usage: command <tsn>"
    exit 0
fi

set -x
anonId=$(curl 'http://anonymizer.tec1.tivo.com/anonymizerExternalIdTranslate' -d '{"type": "anonymizerExternalIdTranslate","externalId": "'$1'"}' -H content-type:application/json -s | jq '.internalId')
set +x
echo $anonId

set -x
anonFeAccountId=$(curl 'http://llc-accountinfo-update-staging.tpc1.tivo.com:50225/anonAccountInfoGet' -d '{"type":"anonAccountInfoGet","bodyId":'$anonId'}' -H content-type:application/json -s)
set +x
echo $anonFeAccountId
