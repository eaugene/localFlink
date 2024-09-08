package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.fs.Path;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FileSinker {

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

    // Define a StreamingFileSink to write output as CSV
    // https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/streamfile_sink/#:~:text=Part%20files%20can%20only%20be%20finalized%20on%20successful%20checkpoints.
    StreamingFileSink<String> sink = StreamingFileSink
        .forRowFormat(new Path("/opt/flink/data/max_values"), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))    // Rollover every 10 minutes
                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))   // Roll over if no data is received for 5 minutes
                .withMaxPartSize(1024 * 1024 * 1024)                    // Max file size is 1 GB
                .build())
        .build();

    // Write the max values (id, value, timestamp) to the CSV file
    maxValues
        .map(value -> value.f0 + "," + value.f1 + "," + value.f2)  // Convert Tuple3 to CSV format string
        .addSink(sink);

    // Execute the job
    env.execute("Max Value Flink Job with File Sink");
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
