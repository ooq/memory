#!/bin/bash

# Helper file to help with setting mapred.reduce.tasks in the
# query files.

for query_file in /root/impala-tpcds-kit/shark_queries/*sql
do
  #sed -i '1s/^/set mapred.reduce.tasks=80;\n/' $query_file
  head -n -1 $query_file > .tmp
  mv .tmp $query_file
  #sed '1s/80/40/'
done
