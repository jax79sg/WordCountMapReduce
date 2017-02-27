#!/bin/bash
rm -r ./output/pairs/results
aws s3 sync s3://emr-cc/emr-output/pairs ./output/pairs
rm -r ./output2/pairs/results
aws s3 sync s3://emr-cc/emr-output2/pairs ./output2/pairs

rm -r ./output/stripes/results
aws s3 sync s3://emr-cc/emr-output/stripes ./output/stripes
rm -r ./output2/stripes/results
aws s3 sync s3://emr-cc/emr-output2/stripes ./output2/stripes
cd output2/pairs/results
echo PAIR RESULTS 
grep '^for[**]' *
cd ../../../output2/stripes/results
echo STRIPES RESULTS
grep '^for[**]' *
