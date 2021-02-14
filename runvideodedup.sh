#!/bin/bash
#set -e

clear

date

cp /mnt/mp400/img02/* /zpool/zdata/nassys/img.1-not-saved/VideoDedup.3.2

tr=60
max=20
  
if [ "$1" == "-step=3" ] || [ "$2" == "-step=3" ] || [ "$3" == "-step=3" ]
then
  rm -r /mnt/mp400/img02/rs/*
  max=15
fi

if [ "$3" == "backup" ] || [ "$4" == "backup" ]
then
  /zpool/zdata/nassys/pgBackup.sh
fi

  
#rm /mnt/mp400/img02/log/*
rm -r /mnt/mp600/tmp/*

date




src=/zpool/zdata/dvgrab
img=/mnt/mp400/img02/db

echo "/mnt/mp400/img02/VideoDedup.py -src=$src -img=$img -tmp=/mnt/mp600/tmp -sqldb=VideoDedup02 -rs=/mnt/mp400/img02/rs -uw=/mnt/mp400/img02/0uw -v=1 -fps=0.2 -limffmpeg=80 -maxdiff=$max -maxdiffuw=7 -minimg=5 $1 $2 $3 -delparam" 

/mnt/mp400/img02/VideoDedup.py -src=$src -img=$img -tmp=/mnt/mp600/tmp -sqldb=VideoDedup02 -rs=/mnt/mp400/img02/rs -uw=/mnt/mp400/img02/0uw -v=1 -fps=0.2 -limffmpeg=75 -maxdiff=$max -maxdiffuw=7 -minimg=5 $1 $2 $3 -delparam

#  -sameresult -delparam -noffmpeg -nosrccp





