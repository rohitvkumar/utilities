for i in $(seq -f "%02g" 1 23); do curl "http://as$i.tp1.tivo.com:8081/femind/v22/do?type=feDevicePartnerCustomerIdGet&bodyId=tsn:8492001901E45AA&partnerId=tivo:pt.3095" -H Accept:application/json -sv; done