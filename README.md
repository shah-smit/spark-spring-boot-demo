# Spark Demo


### Points from Book Spark The Definitive Guide
- `spark.conf.set("spark.sql.shuffle.partitions", 50)` Set to the no. of cores in the machine, to efficiently allow spark to do its job
- A Good rule of thumb is that the number of partitions should be larger than the number of executors on your cluster, 
potentially by multiple factors depending on the workload