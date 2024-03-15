package com.learning.kafkagettingstarted.theusecase;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumers {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "test-group");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            // Subscribe to the topic
            consumer.subscribe(java.util.Collections.singletonList("lemmy"));

            // Continuously poll for new messages
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    // Print the received message
                    System.out.println("Received message: " + record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Error consuming messages: " + e.getMessage());
        }
    }
}
