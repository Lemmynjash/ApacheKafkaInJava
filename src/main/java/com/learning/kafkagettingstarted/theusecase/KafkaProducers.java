package com.learning.kafkagettingstarted.theusecase;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducers {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            // Topic name
            String topicName = "kafka.usecase.student";

            // Publish messages to the topic
            for (int i = 0; i < 10; i++) {
                String message = "Message " + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
                producer.send(record);
                System.out.println("Published message: " + message);
            }
        } catch (Exception e) {
            System.err.println("Error producing messages: " + e.getMessage());
        }
    }
}
