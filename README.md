# data-engine-spark

Draft version - All business logic embedded in the same class
spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV0 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Version 1.0 - Business logic seggregation and use of traditional Phoenix JDBC to write data into database.
spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV1 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Version 2.0 - Implementation changed to use mapPartition() instead of map, parse JSON into Row object and write into database using spark-phoenix connector.
spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV2 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar
