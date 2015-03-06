#!/bin/bash

FILENAME="$1"
echo "Running queries in file $FILENAME"

USERNAME=`basename $FILENAME`

cat $FILENAME | while read line
do
  QUERY_FILENAME=`basename $line`
  OUTPUT_FILENAME="/tmp/query_out_$QUERY_FILENAME"

  # Make a temporary file for the query with the fair scheduler pool set.
  TMP_FILENAME="/tmp/query_$USERNAME"
  TMP_FILENAME+="_$RANDOM"

  echo "set mapred.fairscheduler.pool=$USERNAME;" >> "$TMP_FILENAME"
  cat "$line" >> "$TMP_FILENAME"

  echo "Running query in file $TMP_FILENAME and sending output to $OUTPUT_FILENAME"
  /root/shark/bin/shark-withinfo -h localhost -p 4444 -i $TMP_FILENAME > $OUTPUT_FILENAME 2>&1
  rm $TMP_FILENAME
done
