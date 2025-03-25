package com.vitals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
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
        loadConfig();
        System.out.println("Loaded configs...");
        setupKafka();
        System.out.println("kafka setup done...");
        consumeAndProduce();

        // Graceful shutdown on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(KafkaService::shutdown));
    }

    // Method to load properties from the classpath
    private static Properties loadProperties() {
        Properties properties = new Properties();

        try (InputStream input = KafkaService.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Unable to find the config.properties file.");
                System.exit(1); // Exit if file is missing
            }
            properties.load(input);
        } catch (IOException e) {
            System.out.println("Error loading config.properties file: " + e);
            System.exit(1); // Exit on error
        }

        return properties;
    }

    // Method to load the property from environment variable or from properties file
    private static String loadConfigFile(String propertyName, Properties properties) {
        String envVarValue = System.getenv(propertyName);

        // Return environment variable value if set, otherwise return value from properties file
        return (envVarValue != null && !envVarValue.isEmpty()) ? envVarValue : properties.getProperty(propertyName);
    }

    // Updated loadConfig method using loadProperties and loadConfigFile
    private static void loadConfig() {
        Properties config = loadProperties(); // Load properties from classpath

        // Use environment variables or fall back to properties file values
        KAFKA_BOOTSTRAP_SERVERS = loadConfigFile("KAFKA_BOOTSTRAP_SERVERS", config);
        KAFKA_INPUT_TOPIC = loadConfigFile("KAFKA_INPUT_TOPIC", config);
        KAFKA_OUTPUT_TOPIC = loadConfigFile("KAFKA_OUTPUT_TOPIC", config);
        SASL_USERNAME = loadConfigFile("SASL_USERNAME", config);
        SASL_PASSWORD = loadConfigFile("SASL_PASSWORD", config);

        // Generate a unique consumer group ID
        CONSUMER_GROUP = "Kafka_consumer_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        // Generate a unique client ID
        CLIENT_ID = "kafka-consumer-clientId-" + UUID.randomUUID();

        System.out.println("Configuration Loaded Successfully!");
        System.out.println("Generated Consumer Group: " + CONSUMER_GROUP);
        System.out.println("Generated Client ID: " + CLIENT_ID);
    }

    private static void setupKafka() {
        // Consumer Properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP); // Auto-generated consumer group
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID); // Auto-generated client ID
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // SASL Authentication (Security)
        consumerProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        consumerProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
        consumerProps.put("ssl.enabled.protocols", "TLSv1.2");

        // Producer Properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Apply SASL to Producer as well
        producerProps.put("security.protocol", "SASL_PLAINTEXT");
        producerProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
        producerProps.put("ssl.enabled.protocols", "TLSv1.2");

        consumer = new KafkaConsumer<>(consumerProps);
        System.out.println("Created consumer group: "+CONSUMER_GROUP+"     consumer: "+consumer);
        producer = new KafkaProducer<>(producerProps);
        System.out.println("Created producer:  "+producer);
    }

    private static void consumeAndProduce() {

        consumer.subscribe(Collections.singleton(KAFKA_INPUT_TOPIC));
        System.out.println("Subscribed to: " + consumer.subscription());

        while (true) {
            System.out.println("polling for data....");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(record -> {
                System.out.println("data present...");
                String receivedMessage = record.value();
                System.out.println("Received Message: " + receivedMessage);

                // Publish the received message to the output topic
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, "ProcessedEvent", receivedMessage);
                producer.send(producerRecord);

                System.out.println("Message published to " + KAFKA_OUTPUT_TOPIC + ": " + receivedMessage);

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