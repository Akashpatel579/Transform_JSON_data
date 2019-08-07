# To upload the jar for deployment - local to cloudera
scp -P 2222 target/TransformJSONData-1.0-SNAPSHOT*******.jar cloudera@localhost:/home/cloudera/Clairvoyant/Project/JavaSparkApp/Transform_JSON_Data/
TransformJSONData-1.0-SNAPSHOT.jar

# Execute spark-sbumit with the jar
spark2-submit --class sparkstreaming.SparkStreamingTransformJSON --jars json-simple-1.1.jar TransformJSONData-1.0-SNAPSHOT.jar Transform_Data_App_Kafka_Spark_Streaming_config.json
