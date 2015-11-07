#!/bin/bash
#Author - Sumanan
#This script downloads the output files of a StreamJIT app run
#from Lanka cluster. The Ant script 'jarapp.xml' automatically
#generates this script.
args=("$@")
all=${args[0]}
app=FMRadio71
mainClass=FMRadioCore

#Reads cfgs from verify.txt. Verify.txt contains best cfgs.
function cfgList(){
	while read line
	do
	#echo $line
	IFS='=' read -a array <<< "$line"
	list=("${list[@]}" "${array[0]}")
	done < $mainClass/verify.txt
	#echo ${list[@]}
	kk=final_${mainClass}.cfg,final_blobgraph.svg
	for i in "${list[@]}"
	do
		cfg=${i}_${mainClass}.cfg
		svg=${i}_blobgraph.svg
		kk="$kk,$cfg,$svg"
	done
	#echo $kk
	}

if [ -d $app ]; then
	echo "$app exists. No downloads..."
 	exit
fi
mkdir -p $app
cd $app
mkdir -p $mainClass
db=""
jar=""
if [ "$all" = "all" ];then
	db=,$mainClass.db
	jar=,*.jar
fi
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/\{summary,tune,*.txt,*.orig,streamgraph.dot$db\} $mainClass/
cfgList
cd $mainClass
mkdir -p configurations
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/configurations/\{_streamgraphWtNames.svg,_streamgraph.svg,$kk\} configurations/
cd ..
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/\{*.sh,slurm-*,options.properties\} .
#to download everything.
#rsync -avh --progress sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app .
	
