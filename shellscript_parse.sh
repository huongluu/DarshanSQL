#!/bin/bash

#FILES=$(find /projects/monitoring_data/darshan/ -type f)
FILES=$(find /projects/monitoring_data/darshan/2015-06/1893253/kjin_flow_id1893253_6-22-48474-7398650372849410191_1.darshan.gz -type f)
dbhost="128.174.236.151"
dbuser="bbehza2"
dbpass="123456"
dbname="bluewaters"
for f in $FILES
do
    filename=$(basename $f)
    shortname="${filename%.darshan.gz}"
    result=`mysql -h $dbhost -u $dbuser -p$dbpass --database $dbname -e "show tables like 'jobs_info'"`
    if [  -z "$result" ]; then      # table doesn't exist --> insert first record
        INPUT="$filename.input.txt"
        darshan-parser --perf --file --file-list-detailed $f > $INPUT
        python ./darshan_parse_final.py -i $INPUT -m 0 # per-job info
        rm $INPUT
    else   # table exists, check if log is already processed
        str="select logfilename  from jobs_info where logfilename='"$shortname"' "
        echo $str
        result=`mysql -h $dbhost -u $dbuser -p$dbpass --database $dbname -e "$str"`
   
        if [  -z "$result" ]; then
            INPUT="$filename.input.txt"
            darshan-parser --perf --file --file-list-detailed $f > $INPUT
            python ./darshan_parse_final.py -i $INPUT -m 0 # per-job info
            rm $INPUT
        else
            echo "$filename is already processed \n"
        fi
    fi
done
