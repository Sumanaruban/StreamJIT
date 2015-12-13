#!/bin/bash
#Author - Sumanan
#10-12-2015
if [ "$#" -lt 2 ]; then
	echo "Illegal number of parameters. At least 2 arguments must be passed"
	echo "clean.sh <app> <mainClass> [tune]"
	exit
fi

args=("$@")
appDir=${args[0]}
mainClass=${args[1]}
tune=${args[2]}

if [ ! -d $appDir ]; then
	echo "No $appDir exists...Exiting..."
	exit
fi

rm $appDir/slurm-*

if [ "$tune" = "tune" ];then
	echo "Cleaning for new tune run"
	rm -r $appDir/$mainClass
else
	echo "Cleaning for new verification run"
	rm -r $appDir/$mainClass/summary
	rm $appDir/$mainClass/*.db
	rm $appDir/$mainClass/*.db-journal
	rm $appDir/$mainClass/*.log
	mv $appDir/$mainClass/verify.txt $appDir/$mainClass/verify.dd
	rm $appDir/$mainClass/*.txt
	mv $appDir/$mainClass/verify.dd $appDir/$mainClass/verify.txt
	rm $appDir/$mainClass/*.dot
fi

