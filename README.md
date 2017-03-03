# data-engine-spark

Draft version - All business logic embedded in the same class

spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV0 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Version 1.0 - Business logic seggregation and use of traditional Phoenix JDBC to write data into database.

spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV1 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Version 2.0 - Implementation changed to use mapPartition() instead of map, parse JSON into Row object and write into database using spark-phoenix connector.

spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV2 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Version 3.0 - Implementation change to create DStream of JSON strings instead of Row objects. Writer changed to operate on RDD of JSON string instead of Row. Ran into issue, not a complete working version.

spark-submit --conf spark.driver.extraClassPath=/usr/hdp/current/spark-client/bin --conf spark.data.engine.properties=data-engine --class com.abc.de.spark.handler.MetricsHandlerV3 --conf spark.sql.shuffle.partitions=1 --master yarn --deploy-mode client data-engine-spark-0.0.1-SNAPSHOT.jar

Points to note: This is a working code for json string with regular data type(not binary or byte)
1. Spark is unable to create a dataframe from the json string with one of the json element having binary datatype.
2. All the fields of the dataframe is populated a null.

{"APP":"jdg","RTIME":9223370548286455807,"METRIC":[B@2334de54,"MID":77,"HOST":"ln001xsjdg0001","INSTANCE":"jdg_m1","ENV":"MCOMPerf1"}
{"APP":"jdg","RTIME":9223370548286455807,"METRIC":[B@480396ed,"MID":82,"HOST":"ln001xsjdg0001","INSTANCE":"jdg_m1","ENV":"MCOMPerf1"}
{"APP":"jdg","RTIME":9223370548286455807,"METRIC":[B@3ac07363,"MID":94,"HOST":"ln001xsjdg0001","INSTANCE":"jdg_m1","ENV":"MCOMPerf1"}
{"APP":"jdg","RTIME":9223370548286455807,"METRIC":[B@60783dcd,"MID":87,"HOST":"ln001xsjdg0001","INSTANCE":"jdg_m1","ENV":"MCOMPerf1"}
{"APP":"jdg","RTIME":9223370548286455807,"METRIC":[B@2a813a80,"MID":79,"HOST":"ln001xsjdg0001","INSTANCE":"jdg_m1","ENV":"MCOMPerf1"}
...

2017-03-03 14:11:43 INFO  MetricsWriterFunctionV2:82 - Creating dataframe...with schema:{"type":"struct","fields":[{"name":"APP","type":"string","nullable":true,"metadata":{}},{"name":"RTIME","type":"long","nullable":false,"metadata":{}},{"name":"INSTANCE","type":"string","nullable":true,"metadata":{}},{"name":"METRIC","type":"byte","nullable":true,"metadata":{}},{"name":"HOST","type":"string","nullable":false,"metadata":{}},{"name":"MID","type":"integer","nullable":false,"metadata":{}},{"name":"ENV","type":"string","nullable":false,"metadata":{}}]}
+----+-----+--------+------+----+----+----+
|APP |RTIME|INSTANCE|METRIC|HOST|MID |ENV |
+----+-----+--------+------+----+----+----+
|null|null |null    |null  |null|null|null|
+----+-----+--------+------+----+----+----+

3. Spark is able to create a dataframe successfully, once the Binary type json element is omitted.

{"APP":"bagapp","RTIME":9223370548285855807,"MID":33,"HOST":"jcia5121","INSTANCE":"macys-bagapp_mcombagapp_m01","ENV":"Qa15codemacys"}
{"APP":"bagapp","RTIME":9223370548285855807,"MID":39,"HOST":"jcia5121","INSTANCE":"macys-bagapp_mcombagapp_m01","ENV":"Qa15codemacys"}
{"APP":"bagapp","RTIME":9223370548285855807,"MID":38,"HOST":"jcia5121","INSTANCE":"macys-bagapp_mcombagapp_m01","ENV":"Qa15codemacys"}
{"APP":"bagapp","RTIME":9223370548285855807,"MID":36,"HOST":"jcia5121","INSTANCE":"macys-bagapp_mcombagapp_m01","ENV":"Qa15codemacys"}
{"APP":"bagapp","RTIME":9223370548285855807,"MID":37,"HOST":"jcia5121","INSTANCE":"macys-bagapp_mcombagapp_m01","ENV":"Qa15codemacys"}
...

2017-03-03 14:21:54 INFO  MetricsWriterFunctionV2:85 - Creating dataframe...with schema:{"type":"struct","fields":[{"name":"MID","type":"integer","nullable":false,"metadata":{}},{"name":"RTIME","type":"long","nullable":false,"metadata":{}},{"name":"ENV","type":"string","nullable":false,"metadata":{}},{"name":"APP","type":"string","nullable":true,"metadata":{}},{"name":"HOST","type":"string","nullable":false,"metadata":{}},{"name":"INSTANCE","type":"string","nullable":true,"metadata":{}}]}
+---+-------------------+-------------+------+--------+---------------------------+
|MID|RTIME              |ENV          |APP   |HOST    |INSTANCE                   |
+---+-------------------+-------------+------+--------+---------------------------+
|33 |9223370548285855807|qa15test|bagapp|jcia5121|bagapp_mcombagapp_m01|
+---+-------------------+-------------+------+--------+---------------------------+



