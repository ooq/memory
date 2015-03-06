#!/bin/bash

for filename in /root/impala-tpcds-kit/shark_queries/user_*
do
  /root/impala-tpcds-kit/shark_queries/run_user.sh $filename &
done

echo "Launched all user streams; waiting for completion"
wait
echo "All user streams completed"
