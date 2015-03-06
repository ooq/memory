#!/bin/bash

# Generates a file for each user with a random
# query order.

NUM_USERS=3

for (( i=1; i <= $NUM_USERS; i++))
do
  echo "Generating query list for user $i"
  FILENAME="/root/impala-tpcds-kit/shark_queries/user_$i"
  ls /root/impala-tpcds-kit/shark_queries/*sql | shuf > $FILENAME
done

