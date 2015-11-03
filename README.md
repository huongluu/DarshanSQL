# Description
DarshanSQL is a python script to parse Darshan logs to MySQL for more data analysis.

# Prerequisites
Please make sure you have the following required software on your system before installation: 
* Darshan-util toolset: can be found here http://www.mcs.anl.gov/research/projects/darshan/docs/darshan-util.html
* MySQL server

# Usage

* Create a database in MySQL server: for example: darshan_db

$ mysql -h "hostname" -u "user" -p

mysql> create database darshan_db;

* Enter credentials in both 2 scripts: shellscript_parse.sh and darshan_parse_final.py, including database host (remote or local), database user and password, database name (e.g. darshan_db)
* Enter path to directory contain Darshan logs in shellscript_parse.sh
