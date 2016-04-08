#!/usr/bin/env bash

gzip -f Microsoft/NetalyzerJobs/RawData/*.csv

hadoop fs -put -f Microsoft /

curl --insecure \
  --user '<username>:<password>' \
  --header 'Content-Type: application/json' \
  --request POST \
  --data '{"file": "wasb://data@netalyzerdata.blob.core.windows.net/Microsoft/NetalyzerJobs/spark1-1.0-SNAPSHOT-all.jar", "className": "com.microsoft.spark1.Main"}' \
  'https://netalyzer.azurehdinsight.net/livy/batches'

echo
