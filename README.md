# spark1

From:  https://azure.microsoft.com/en-us/documentation/articles/hdinsight-apache-spark-livy-rest-interface/

curl -k --user "user:pass" -H 'Content-Type: application/json' -X POST -d '{ "file":"wasb://data@netalyzerdata.blob.core.windows.net/tmp/spark1-1.0-SNAPSHOT-all.jar", "className":"org.bitvector.spark1.Main" }' "https://netalyzer.azurehdinsight.net/livy/batches"

curl -k --user "user:pass" -X GET "https://netalyzer.azurehdinsight.net/livy/batches/0"
