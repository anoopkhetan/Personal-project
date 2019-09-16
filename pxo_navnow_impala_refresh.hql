#!/bin/sh

echo '>>>>>>>>>>>>>>> Impala refresh started <<<<<<<<<<<<<<<<<<<<'

impala-shell -k -i $1 -q "refresh $2.PXO_NAVNOW_FUND_CLIENT_MAPPING_1; refresh $3.pxo_navnow_mtmi_predicted; ; refresh $3.pxo_navnow_pricing_predicted;  refresh $3.pxo_navnow_nds_predicted; refresh $3.pxo_navnow_mtmi_clientlevel; refresh $3.pxo_navnow_pricing_clientlevel; refresh $3.pxo_navnow_nds_clientlevel; refresh $3.pxo_navnow_mtmi_fundlevel; refresh $3.pxo_navnow_pricing_fundlevel; refresh $3.pxo_navnow_nds_fundlevel; 
refresh $3.pxo_navnow_mtmi_funds_predicted; refresh $3.pxo_navnow_pricing_funds_predicted; refresh $3.pxo_navnow_ticker_prediction_quantiles1; refresh $3.pxo_navnow_mtmi_funds_predicted_1; refresh $3.pxo_navnow_pricing_funds_predicted_1; refresh $3.pxo_navnow_nds_ticker_prediction_1; exit;"

echo '>>>>>>>>>>>>>>> Impala refresh done for all tables <<<<<<<<<<<<<<<<<<<<'


while [ `date +"%H%M%S"` -le '190228' ]
do
  if [ `date +"%H%M%S"` -ge '155928' ]
  then
  	START=$(date +%s)
	echo '---------> Every min impala refresh started at' `date +"%H:%M:%S.%s"` ' - 1st time'

	impala-shell -k -i $1 -q "refresh $3.pxo_navnow_mtmi_clientlevel; refresh $3.pxo_navnow_pricing_clientlevel; refresh $3.pxo_navnow_nds_clientlevel; refresh $3.pxo_navnow_mtmi_fundlevel; refresh $3.pxo_navnow_pricing_fundlevel; refresh $3.pxo_navnow_nds_fundlevel; exit;"

	echo '---------> Impala refresh ended at' `date +"%H:%M:%S.%s"`

	sleep 5

	echo '---------> Impala refresh started at' `date +"%H:%M:%S.%s"` ' - 2nd time'

	impala-shell -k -i $1 -q "refresh $3.pxo_navnow_mtmi_clientlevel; refresh $3.pxo_navnow_pricing_clientlevel; refresh $3.pxo_navnow_nds_clientlevel; refresh $3.pxo_navnow_mtmi_fundlevel; refresh $3.pxo_navnow_pricing_fundlevel; refresh $3.pxo_navnow_nds_fundlevel; exit;"

	echo '---------> Impala refresh ended at' `date +"%H:%M:%S.%s"`

	END=$(date +%s)

	sleep `expr 60 - $(( $END - $START ))`
  fi
done
echo '>>>>>>>>>>>>>>> Impala refresh done for the day <<<<<<<<<<<<<<<<<<<<'