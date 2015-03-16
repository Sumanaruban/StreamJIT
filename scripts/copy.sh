#!/bin/bash
#Author - Sumanan
#Mar 22, 2015
#Create a new streamjit app dir from an old streamjit app dir to run an experiment.
#This script is useful when doing experiments in a cluster.
#----------------------------------------------------------------------------------

if [ "$#" -ne 5 ]; then
	echo "Illegal number of parameters. 5 arguments must be passed"
	echo "copy.sh <app> <fromID> <toID> <mainClass> <noOfnodes>"
	exit
fi

args=("$@")
app=${args[0]}
fromID=${args[1]}
toID=${args[2]}
mainClass=${args[3]}
nodes=${args[4]}

fromDir=$app$fromID
toDir=$app$toID

if [ ! -d $fromDir ]; then
	echo "No $fromDir exists...Exiting..."
 	exit
fi

if [ -d $toDir ]; then
	echo "$toDir exists...Exiting..."
 	exit
fi

mkdir $toDir

cp -r $fromDir/* $toDir/
cd $toDir
mv $fromDir.jar $toDir.jar
rm *.sh
cd ..
./setup.sh $toDir $mainClass $nodes
