package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Properties;

public class kafkaSinker {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Set up Kafka properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "host.docker.internal:29092");
    properties.setProperty("group.id", "flink-group");

    // Create Kafka consumer
    FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
        "your-topic2",
        new SimpleStringSchema(),
        properties
    );

    // Assign Timestamps and Watermarks
    DataStream<String> input = env
        .addSource(kafkaConsumer)
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
          @Override
          public long extractAscendingTimestamp(String element) {
            return parseTimestamp(element);
          }
        });

    // Process the stream: Calculate max value every 10 seconds
    DataStream<Tuple3<Long, Double, Long>> maxValues = input
        .map(new MapFunction<String, Tuple3<Long, Double, Long>>() {
          @Override
          public Tuple3<Long, Double, Long> map(String value) throws Exception {
            return parseEvent(value);
          }
        })
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
        .maxBy(1);  // Find the record with max value in the window (index 1 is the 'value')

    // Create a Kafka producer (sink)
    FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
        "your-topic3",               // Target topic
        new SimpleStringSchema(),     // Serialization schema
        properties                    // Producer properties
    );

    // Write the max values (id, value, timestamp) to the Kafka topic
    maxValues
        .map(value -> value.f0 + "," + value.f1 + "," + value.f2)  // Convert Tuple3 to String
        .addSink(kafkaProducer);

    // Execute the job
    env.execute("Max Value Flink Job with Kafka Sink");
  }

  // Helper method to parse the JSON and extract the timestamp
  private static long parseTimestamp(String json) {
    try {
      JsonNode node = objectMapper.readTree(json);
      return node.get("timestamp").asLong();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse timestamp from JSON", e);
    }
  }

  // Helper method to parse the JSON event
  private static Tuple3<Long, Double, Long> parseEvent(String json) {
    try {
      JsonNode node = objectMapper.readTree(json);
      long id = node.get("id").asLong();
      double value = node.get("value").asDouble();
      long timestamp = node.get("timestamp").asLong();
      return new Tuple3<>(id, value, timestamp);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse event from JSON", e);
    }
  }
}
