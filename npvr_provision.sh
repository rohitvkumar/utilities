if [ $# -ne 2 ]; then
    echo "Usage: command <tsn> <mor_env>"
    exit 0
fi

set -x
resp=$(curl "http://$2.tivo.com:8085/mind/mind22?type=feMsoPartnerIdGet&bodyId=$1" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerId=$(echo $resp | jq '.partnerId' | sed -e 's/^"//' -e 's/"$//')

set -x
resp=$(curl "http://$2.tivo.com:8085/mind/mind22?type=feDevicePartnerCustomerIdGet&bodyId=$1&partnerId=$partnerId" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerCustomerId=$(echo $resp | jq '.partnerCustomerId' | sed -e 's/^"//' -e 's/"$//')

devices=$(curl "http://$2.tivo.com:8085/mind/mind22?type=feAccountFeDeviceSearch&partnerId=$partnerId&partnerCustomerId=$partnerCustomerId" -H "Accept:application/json" -s)

echo ""
echo "Device types"
echo $devices | jq '.feDevice[] | {bodyId,deviceType}'

echo ""
echo "Device anon ids:"
for tsn in $(echo $devices | jq '.feDevice[].bodyId')
do
   echo $tsn
   anonId=$(curl 'http://anonymizer.tec1.tivo.com/anonymizerExternalIdTranslate' \
   -d '{"type": "anonymizerExternalIdTranslate","externalId": '$tsn'}' -H content-type:application/json -s | jq '.internalId')
   echo "AnonId: $anonId"
   anonFeAccountId=$(curl 'http://llc-accountinfo-update-staging.tpc1.tivo.com:50225/anonAccountInfoGet' \
   -d '{"type":"anonAccountInfoGet","bodyId":'$anonId'}' -H content-type:application/json -s | jq '.anonFeAccountId')
   echo "FeAccountIdAnon: $anonFeAccountId"
done

echo ""
echo "AppDb cluster id:"
curl "http://$2.tivo.com:8085/mind/mind37?type=infoGet&bodyId=$1" -H "Accept:application/json" -s | jq '.clusterId'

echo ""
echo ""
echo "All device fields:"
echo $devices | jq '.'
