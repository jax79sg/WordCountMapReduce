#!/bin/bash
cd bin
rm ../MapReduceStripe.jar
jar cvfm ../MapReduceStripe.jar ../sManifest.txt *
aws s3 sync ../codes s3://emr-cc/emr-code-stripe --delete
aws s3 cp ../MapReduceStripe.jar s3://emr-cc/emr-code-stripe/MapReduceStripe.jar
#aws s3 sync ../input s3://emr-cc/emr-input-words --delete
rm -r ../output/stripes/*
aws s3 sync ../output/stripes s3://emr-cc/emr-output/stripes --delete
rm -r ../output2/stripes/*
aws s3 sync ../output2/stripes s3://emr-cc/emr-output2/stripes --delete
aws emr create-cluster --release-label emr-4.0.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m3.2xlarge --steps Type=CUSTOM_JAR,Name="Custom JAR Step",ActionOnFailure=CONTINUE,Jar=s3://emr-cc/emr-code-stripe/MapReduceStripe.jar,Args=["s3://emr-cc/emr-input-words","s3://emr-cc/emr-output/stripes/results","s3://emr-cc/emr-output/stripes/results","s3://emr-cc/emr-output2/stripes/results"] --auto-terminate --log-uri s3://emr-cc/emr-log/stripes --enable-debugging
open https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#cluster-list:
