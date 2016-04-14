#!/usr/bin/env bash

cp /usr/hdp/current/spark-client/lib/{datanucleus-api-jdo-3.2.6.jar,datanucleus-core-3.2.10.jar,datanucleus-rdbms-3.2.9.jar} Microsoft/NetalyzerJobs
cp /usr/hdp/current/spark-client/conf/hive-site.xml Microsoft/NetalyzerJobs

gzip -f Microsoft/NetalyzerJobs/RawData/*.csv

hadoop fs -put -f Microsoft /

curl --insecure \
  --user 'netalyzer:Elvis15Alive!' \
  --header 'Content-Type: application/json' \
  --request POST \
  --data @payload.json \
  'https://netalyzer.azurehdinsight.net/livy/batches'

echo

