package com.learning.kafkagettingstarted.theusecase;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Properties;

public class KafkaTopicCreator {

    public static void main(String[] main){
        Properties properties=new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        // Create AdminClient
        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Specify the topic name and its properties
            String topicName = "kafka.usecase.student";
            int numPartitions = 2; // Number of partitions for the topic
            short replicationFactor = 1; // Replication factor for the topic

            // Create a NewTopic object with specified properties
            NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

            // Create the topic
            adminClient.createTopics(java.util.Collections.singleton(newTopic));

            System.out.println("Topic \"" + topicName + "\" created successfully.");
        } catch (Exception e) {
            System.err.println("Error creating topic: " + e.getMessage());
        }
    }
}
