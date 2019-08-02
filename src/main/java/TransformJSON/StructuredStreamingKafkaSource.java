package TransformJSON;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import scala.collection.JavaConverters;


public final class StructuredStreamingKafkaSource {

    public static void main(String[] args) throws StreamingQueryException {
        if (args.length < 1) {
            System.err.println("Usage: spark2-submit --class TransformJSON.StructuredStreamingKafkaSource --jars json-simple-1.1.jar TransformJSONData-1.0-SNAPSHOT.jar <Transform_Data_App_KSMS_config.json> ");
            System.exit(1);
        }

        JSONParser parser = new JSONParser();

        try {

            Object obj = parser.parse(new FileReader(args[0]));

            JSONObject jsonObject = (JSONObject) obj;

            String applicationName = (String) jsonObject.get("application_name");
            String kafkaBroker = (String) jsonObject.get("kafka_broker");
            String topic = (String) jsonObject.get("topic");

            // getting table Properties
            JSONArray jsonArray = (JSONArray) jsonObject.get("table_mapping");

            // iterating columns properties that we wanted to change
            Iterator itr2 = jsonArray.iterator();

            while (itr2.hasNext())
            {
                Iterator itr1 = ((Map<Object, Object>) itr2.next()).entrySet().iterator();
                while (itr1.hasNext()) {
                    Map.Entry pair = (Map.Entry) itr1.next();
                    System.out.println(pair.getKey() + " : " + pair.getValue());
                }
            }

        SparkSession spark = SparkSession
                .builder()
                .appName(applicationName)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> contact_info_df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBroker)
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .alias("json_data")
                .as(Encoders.STRING());

//
//        Dataset df = contact_info_df.select(functions.get_json_object(new Column("value"), "$.firstName").as("FIRTSTNAME"),
//                                            functions.get_json_object(new Column("value"), "$.lastName").as("LASTNAME"));

            ArrayList<Column> obj_C = new ArrayList<>();

            for (Object e : jsonArray) {

            System.out.println("Nested JSON Data " + ((JSONObject) e).get("source_name"));
            System.out.println("Nested JSON Data " + ((JSONObject) e).get("destination_name"));

            obj_C.add(functions
                    .get_json_object(new Column("value"), "$."+(((JSONObject) e).get("source_name")))
                    .as((String)((JSONObject) e).get("destination_name")));
            }

        Dataset df_new = contact_info_df.select(JavaConverters.asScalaIteratorConverter(obj_C.iterator()).asScala().toSeq());

        // Dataset<Row> df = contact_info_df.select(DataType.fromJson(schema.json())).as("data").select("data.*");

        // Start running the query that prints the running selected columns to the console
        StreamingQuery query = df_new.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
