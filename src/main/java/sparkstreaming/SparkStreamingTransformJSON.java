package sparkstreaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.collection.JavaConverters;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class SparkStreamingTransformJSON {
	public static void main(String[] args) throws InterruptedException, StreamingQueryException {

		if (args.length < 1) {
			System.err.println("Usage: spark2-submit --class TransformJSON.StructuredStreamingKafkaSource --jars json-simple-1.1.jar TransformJSONData-1.0-SNAPSHOT.jar <Transform_Data_App_KSMS_config.json> ");
			System.exit(1);
		}

		try {
			JSONParser parser = new JSONParser();
			Object obj = parser.parse(new FileReader(args[0]));

			JSONObject jsonObject = (JSONObject) obj;
			String applicationName = (String) jsonObject.get("application_name");
			String kafkaBroker = (String) jsonObject.get("kafka_broker");
			String topic = (String) jsonObject.get("topic");
			String group_id = (String) jsonObject.get("group_id");
			String offset = (String) jsonObject.get("starting_offsets");

			JSONArray jsonArray = (JSONArray) jsonObject.get("table_mapping");

			SparkConf conf = new SparkConf()
					.setAppName(applicationName);

			JavaSparkContext sc = new JavaSparkContext(conf);
			JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

			Set<String> topics = Collections.singleton(topic);
			Map<String, Object> kafkaParams = new HashMap<>();
			kafkaParams.put("bootstrap.servers", kafkaBroker);
			kafkaParams.put("key.deserializer", StringDeserializer.class);
			kafkaParams.put("value.deserializer", StringDeserializer.class);
			kafkaParams.put("group.id", group_id);
			kafkaParams.put("auto.offset.reset", offset);


			JavaInputDStream<ConsumerRecord<String, String>> stream =
					KafkaUtils.createDirectStream(
							ssc,
							LocationStrategies.PreferConsistent(),
							ConsumerStrategies.Subscribe(topics, kafkaParams)
					);

			// stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

			JavaDStream<String> lines = stream.map(ConsumerRecord::value);

			//Create JavaRDD<Row>
			lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
				@Override
				public void call(JavaRDD<String> rdd) {
					JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
						@Override
						public Row call(String msg) {
							Row row = RowFactory.create(msg);
							return row;
						}
					});
					// Create Schema
					StructType schema = DataTypes.createStructType(new StructField[]{
							DataTypes.createStructField("value", DataTypes.StringType, true)});
					// Get Spark 2.0 session
					SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
					Dataset msgDataFrame = spark.createDataFrame(rowRDD, schema);
					// msgDataFrame.show();

					spark.sparkContext().setLogLevel("ERROR");


//                Dataset df = msgDataFrame.select(functions.get_json_object(new Column("value"), "$.firstName").as("FIRTSTNAME"),
//                        functions.get_json_object(new Column("value"), "$.lastName").as("LASTNAME"));
//                df.show();

					ArrayList<Column> obj_C = new ArrayList<>();

					for (Object e : jsonArray) {
//                System.out.println("Nested JSON Data " + ((JSONObject) e).get("source_name"));
//                System.out.println("Nested JSON Data " + ((JSONObject) e).get("destination_name"));

						obj_C.add(functions
								.get_json_object(new Column("value"), "$." + (((JSONObject) e).get("source_name")))
								.as((String) ((JSONObject) e).get("destination_name")));
					}

					Dataset df = msgDataFrame.select(JavaConverters.asScalaIteratorConverter(obj_C.iterator())
							.asScala().toSeq());
					df.show();

				}
			});

			ssc.start();
			ssc.awaitTermination();
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}

//        JavaDStream<String> results = stream.map(record -> record.value());

//        results.foreachRDD(rdd -> {
//            rdd.foreach(
//                    record -> System.out.println(record));
//        });
		//System.out.println(stream);


	}
}

class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession
					.builder()
					.config(sparkConf)
					.getOrCreate();
		}
		return instance;
	}
}