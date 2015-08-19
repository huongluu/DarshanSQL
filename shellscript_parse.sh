#!/bin/bash

FILES=$(find /Users/huongluu/Research/data/mira/2015/1/5/yeluo_qmcapp_id391143_1-5-82432-11076375904968815475_1.darshan.gz -type f)
dbhost="localhost"
dbuser="huong"
dbpass="123456"
dbname="mira_final"
for f in $FILES
do
    filename=$(basename $f)
    shortname="${filename%.darshan.gz}"
    result=`mysql -h $dbhost -u $dbuser -p$dbpass --database $dbname -e "show tables like 'jobs_info'"`
    if [  -z "$result" ]; then      # table doesn't exist --> insert first record
        INPUT="$f.input.txt"
        darshan-parser --perf --file --file-list-detailed $f > $INPUT
        python ./darshan_parse_final.py -i $INPUT -m 0 # per-job info
        rm $INPUT
    else   # table exists, check if log is already processed
        str="select logfilename  from jobs_info where logfilename='"$shortname"' "
        result=`mysql -h $dbhost -u $dbuser -p$dbpass --database $dbname -e "$str"`
   
        if [  -z "$result" ]; then
            INPUT="$f.input.txt"
            darshan-parser --perf --file --file-list-detailed $f > $INPUT
            python ./darshan_parse_final.py -i $INPUT -m 0 # per-job info
            rm $INPUT
        else
            echo "$filename is already processed \n"
        fi
    fi
done
