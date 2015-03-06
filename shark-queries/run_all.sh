#!/bin/bash

for query_file in /root/impala-tpcds-kit/shark_queries/*sql
do
  FILENAME=`basename $query_file`
  OUTPUT_FILENAME="/tmp/query_out_$FILENAME"
  echo "Running query in file $query_file and sending output to $OUTPUT_FILENAME"
  # Clear buffer cache before each query!
  python /root/shark/bin/dev/clear-buffer-cache.py
  /root/shark/bin/shark-withinfo -h localhost -p 4444 -i $query_file > $OUTPUT_FILENAME 2>&1
done
