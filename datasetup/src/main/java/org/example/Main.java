package org.example;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Math.abs;


public class Main {
  public static void main(String[] args) {
    // Kafka broker address
    String bootstrapServers = "127.0.0.1:9092";
    // Kafka topic name
    String topicName = "your-topic2";

    // Configure the Kafka producer
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrapServers);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    // Create a Kafka producer
    Producer<String, String> producer = new KafkaProducer<>(properties);

    // ObjectMapper for JSON serialization
    ObjectMapper objectMapper = new ObjectMapper();

    Random random = new Random();

    try {
      int i=1;
      for (;;) {  // Replace 10 with the number of messages you want to send
        // Create a message payload
        Map<String, Object> message = new HashMap<>();
        message.put("id", i++);
        message.put("timestamp", System.currentTimeMillis());
        message.put("value", abs(random.nextInt(100)));

        // Serialize the message to JSON
        String messageStr = objectMapper.writeValueAsString(message);

        // Send the message to Kafka
        producer.send(new ProducerRecord<>(topicName, Integer.toString(i), messageStr));

        // Log the sent message
        System.out.println("Sent message: " + messageStr);

        // Optional: Sleep for a bit before sending the next message
        TimeUnit.SECONDS.sleep(1);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Close the producer
      producer.close();
    }
  }
}