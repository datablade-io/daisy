#!/usr/bin/bash

for i in `clickhouse-client --query "select concat(database,'.', name) from system.tables where database='benchmark'" --format CSV`; do 
    dbname=`echo $i | awk -F '"' '{print $2}'` 
    echo $dbname
    clickhouse-client --query "select * from $dbname" --format=CSV > /data/$dbname.data
done