package com.vitals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService {
    private static String KAFKA_BOOTSTRAP_SERVERS;
    private static String KAFKA_INPUT_TOPIC;
    private static String KAFKA_OUTPUT_TOPIC;
    private static String SASL_USERNAME;
    private static String SASL_PASSWORD;
    private static String CONSUMER_GROUP;
    private static String CLIENT_ID;

    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        loadConfig(); // Load configuration from file
        setupKafka();
        consumeAndProduce();

        // Graceful shutdown on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(KafkaService::shutdown));
    }

    private static void loadConfig() {
        Properties config = new Properties();
        try (FileInputStream fis = new FileInputStream("./src/main/resources/config.properties")) {
            config.load(fis);
            KAFKA_BOOTSTRAP_SERVERS = config.getProperty("KAFKA_BOOTSTRAP_SERVERS");
            KAFKA_INPUT_TOPIC = config.getProperty("KAFKA_INPUT_TOPIC");
            KAFKA_OUTPUT_TOPIC = config.getProperty("KAFKA_OUTPUT_TOPIC");
            SASL_USERNAME = config.getProperty("SASL_USERNAME");
            SASL_PASSWORD = config.getProperty("SASL_PASSWORD");

            // Generate a unique consumer group ID
            CONSUMER_GROUP = "consumer-" + System.currentTimeMillis();

            // Generate a unique client ID
            CLIENT_ID = "kafka-consumer-clientId-" + UUID.randomUUID();

            System.out.println("Configuration Loaded Successfully!");
            System.out.println("Generated Consumer Group: " + CONSUMER_GROUP);
            System.out.println("Generated Client ID: " + CLIENT_ID);
        } catch (IOException e) {
            System.err.println("Error loading config file: " + e.getMessage());
            System.exit(1); // Exit if config file is missing or incorrect
        }
    }

    private static void setupKafka() {
        // Consumer Properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP); // Auto-generated consumer group
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID); // Auto-generated client ID
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // SASL Authentication (Security)
        consumerProps.put("security.protocol", "SASL_PLAINTEXT");
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
        consumerProps.put("ssl.enabled.protocols", "TLSv1.2"); // Secure TLS versions

        // Producer Properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Apply SASL to Producer as well
        producerProps.put("security.protocol", "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
        producerProps.put("ssl.enabled.protocols", "TLSv1.2");

        consumer = new KafkaConsumer<>(consumerProps);
        producer = new KafkaProducer<>(producerProps);
    }

    private static void consumeAndProduce() {
        consumer.subscribe(Collections.singleton(KAFKA_INPUT_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                try {
                    String receivedMessage = record.value();
                    System.out.println("Received Message: " + receivedMessage);

                    // Publish the received message to the output topic
                    ProducerRecord<String, String> producerRecord =
                            new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, "ProcessedEvent", receivedMessage);
                    producer.send(producerRecord);

                    System.out.println("Message published to " + KAFKA_OUTPUT_TOPIC+": "+receivedMessage);
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                }
            });
        }
    }

    private static void shutdown() {
        System.out.println("Shutting down Kafka consumer & producer...");
        try {
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
        } catch (Exception e) {
            System.err.println("Error closing Kafka connections: " + e.getMessage());
        }
    }
}