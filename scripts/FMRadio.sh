#!/bin/bash
#Author - Sumanan
# A sample file to download tuning files from Lanka cluster.
args=("$@")
all=${args[0]}
app=FMRadio
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
if [ "$all" = "all" ];then
	echo "Going to download all files, best configurations and tuning DB..."
	scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/\{summary,tune,*.txt,*.orig,streamgraph.dot,$mainClass,*.jar\} $mainClass/
	cfgList
	cd $mainClass
	mkdir -p configurations
	scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/configurations/\{$kk\} configurations/
	cd ..
else
	echo "Going to download essential files only..."
	scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/$mainClass/\{summary,tune,*.txt,*.orig,streamgraph.dot\} $mainClass/
fi
scp -r sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app/\{*.sh,slurm-*,options.properties\} .
#to download everything.
#rsync -avh --progress sumanan@lanka.csail.mit.edu:/data/scratch/sumanan/$app .
	
