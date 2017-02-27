#!/bin/bash
cd bin
rm ../MapReducePairs.jar
jar cvfm ../MapReducePairs.jar ../pManifest.txt *
rm -r ../localoutput/pairs/*
rm -r ../localoutput2/pairs/*
cd ..
java -cp hadoop26lib/*:MapReducePairs.jar pairs.MapReduce input localoutput/pairs/results	localoutput/pairs/results localoutput2/pairs/results
