#!/bin/bash

FILES=$(find /darshan_logs_directory -type f)
dbhost=""
dbuser=""
dbpass=""
dbname=""
for f in $FILES
do
    filename=$(basename $f)
    shortname="${filename%.darshan.gz}"
    result=`mysql -h $dbhost -u $dbuser -p$dbpass --database $dbname -e "show tables like 'jobs_info'"`
    if [  -z "$result" ]; then      # table doesn't exist --> create table/ insert first record
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
