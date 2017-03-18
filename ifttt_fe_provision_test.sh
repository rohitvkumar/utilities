if [ $# -ne 2 ]; then
    echo "Usage: command <tsn> <mor_env>"
    exit 0
fi

set -x
resp=$(curl "http://pdk01.$2.tivo.com:8085/mind/mind22?type=feMsoPartnerIdGet&bodyId=$1" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerId=$(echo $resp | jq '.partnerId' | sed -e 's/^"//' -e 's/"$//')

set -x
resp=$(curl "http://pdk01.$2.tivo.com:8085/mind/mind22?type=feDevicePartnerCustomerIdGet&bodyId=$1&partnerId=$partnerId" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerCustomerId=$(echo $resp | jq '.partnerCustomerId' | sed -e 's/^"//' -e 's/"$//')

set -x
curl "http://pdk01.$2.tivo.com:8085/mind/mind22?type=feAccountFeDeviceSearch&partnerId=$partnerId&partnerCustomerId=$partnerCustomerId" -H "Accept:application/json" -s | jq '.'
set +x
