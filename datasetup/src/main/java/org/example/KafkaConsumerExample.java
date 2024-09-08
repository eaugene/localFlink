package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerExample {

  public static void main(String[] args) {

    // Kafka broker address
    String bootstrapServers = "127.0.0.1:9092";
    // Kafka topic name
    String topicName = "your-topic3";
    // Consumer group ID
    String groupId = "my-consumer-group";

    // Configure the Kafka consumer
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("group.id", groupId);
    properties.put("auto.offset.reset", "latest"); // Start reading from the earliest message
    properties.put("enable.auto.commit", "true"); // Auto commit offsets

    // Create a Kafka consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Subscribe to the topic
    consumer.subscribe(Collections.singletonList(topicName));

    // ObjectMapper for JSON deserialization
    ObjectMapper objectMapper = new ObjectMapper();

    try {
      while (true) {
        // Poll for new data
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          // Deserialize the message from JSON
          //String message = objectMapper.readValue(record.value(), String.class);
          String ts = String.valueOf(record.timestamp());

          // Process the message
          System.out.println("Received message: " + record.value() + " at timestamp: " + ts);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Close the consumer
      consumer.close();
    }
  }
}
