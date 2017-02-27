#!/bin/bash
cd bin
rm ../MapReduceStripe.jar
jar cvfm ../MapReduceStripe.jar ../sManifest.txt *
rm -r ../localoutput/stripes/*
rm -r ../localoutput2/stripes/*
cd ..
java -cp hadoop26lib/*:MapReduceStripe.jar stripes.MapReduce input localoutput/stripes/results	localoutput/stripes/results localoutput2/stripes/results
