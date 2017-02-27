#!/bin/bash
cd bin
rm ../MapReducePairs.jar
jar cvfm ../MapReducePairs.jar ../pManifest.txt *
aws s3 sync ../codes s3://emr-cc/emr-code-pairs --delete
aws s3 cp ../MapReducePairs.jar s3://emr-cc/emr-code-pairs/MapReducePairs.jar
#aws s3 sync ../input s3://emr-cc/emr-input-words/ --delete
rm -r ../output/pairs/*
aws s3 sync ../output/pairs s3://emr-cc/emr-output/pairs --delete
rm -r ../output2/pairs/*
aws s3 sync ../output2/pairs s3://emr-cc/emr-output2/pairs --delete
aws emr create-cluster --release-label emr-4.0.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m3.2xlarge --steps Type=CUSTOM_JAR,Name="Custom JAR Step",ActionOnFailure=CONTINUE,Jar=s3://emr-cc/emr-code-pairs/MapReducePairs.jar,Args=["s3://emr-cc/emr-input-words","s3://emr-cc/emr-output/pairs/results","s3://emr-cc/emr-output/pairs/results","s3://emr-cc/emr-output2/pairs/results"] --auto-terminate --log-uri s3://emr-cc/emr-log/pairs --enable-debugging
open https://eu-west-1.console.aws.amazon.com/elasticmapreduce/home?region=eu-west-1#cluster-list:
