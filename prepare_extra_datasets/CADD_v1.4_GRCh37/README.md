
## Run on existing dataproc cluster
```
gcloud dataproc jobs submit pyspark \
  --cluster=em-cluster \
  --files=gs://hail-common/builds/0.2/jars/hail-0.2-$HASH-Spark-2.2.0.jar \
  --py-files=gs://hail-common/builds/0.2/python/hail-0.2-$HASH.zip \
  --properties="spark.driver.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar,spark.executor.extraClassPath=./hail-0.2-$HASH-Spark-2.2.0.jar" \
  2_make_hailtable.py
```
