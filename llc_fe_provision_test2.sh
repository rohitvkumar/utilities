if [ $# -ne 2 ]; then
    echo "Usage: command <anon-bodyId> <mor_env>"
    exit 0
fi

set -x
resp=$(curl "http://anonymizer.qec1.tivo.com/anonymizerInternalIdTranslate" -H "content-type:application/json"\
 -H "Accept:application/json" -d '{"type":"anonymizerInternalIdTranslate","internalId":"'$1'"}' -s)
set +x
echo $resp | jq '.'
TSN=$(echo $resp | jq '.externalId' | sed -e 's/^"//' -e 's/"$//')

set -x
resp=$(curl "http://$2.tivo.com:8085/mind/mind22?type=feMsoPartnerIdGet&bodyId=$TSN" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerId=$(echo $resp | jq '.partnerId' | sed -e 's/^"//' -e 's/"$//')

set -x
resp=$(curl "http://$2.tivo.com:8085/mind/mind22?type=feDevicePartnerCustomerIdGet&bodyId=$TSN&partnerId=tivo:pt.3095" -H "Accept:application/json" -s)
set +x
echo $resp | jq '.'
partnerCustomerId=$(echo $resp | jq '.partnerCustomerId' | sed -e 's/^"//' -e 's/"$//')

set -x
curl "http://$2.tivo.com:8085/mind/mind22?type=feAccountFeDeviceSearch&partnerId=$partnerId&partnerCustomerId=$partnerCustomerId" -H "Accept:application/json" -s | jq '.'
set +x
