package com.vitals;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLOutput;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class Patient_Vitals {

    public static String KAFKA_BOOTSTRAP_SERVERS;
    public static String KAFKA_OUTPUT_TOPIC;
    public static String SASL_ENABLED;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static String SASL_MECHANISM;
    public static String SASL_TLS_VERSION;
    public static String SASL_PROTOCOL;
    public static String KAFKA_INPUT_TOPIC;
    public static String KAFKA_CONSUMER_GROUP;
    static Consumer<String, String> consumer;

    public static void main(String[]args) throws Exception {
        // Set up Kafka producer properties
        Properties properties = loadProperties();


        KAFKA_OUTPUT_TOPIC = loadconfigfile("KAFKA_OUTPUT_TOPIC",properties);
        KAFKA_BOOTSTRAP_SERVERS = loadconfigfile("KAFKA_BOOTSTRAP_SERVERS", properties);
        SASL_ENABLED = loadconfigfile("SASL_ENABLED", properties);
        SASL_USERNAME = loadconfigfile("SASL_USERNAME", properties);
        SASL_PASSWORD = loadconfigfile("SASL_PASSWORD", properties);
        SASL_MECHANISM = loadconfigfile("SASL_MECHANISM", properties);
        SASL_TLS_VERSION = loadconfigfile("SASL_TLS_VERSION", properties);
        SASL_PROTOCOL = loadconfigfile("SASL_PROTOCOL", properties);

        KAFKA_CONSUMER_GROUP= loadconfigfile("KAFKA_CONSUMER_GROUP", properties);

        KafkaProduser  kafka = new KafkaProduser();

                Properties kafkaProperties = new Properties();

                String saslEnvValue = SASL_ENABLED;
                boolean	sasl_enabled = (saslEnvValue != null && !saslEnvValue.isEmpty())
                        ? Boolean.parseBoolean(saslEnvValue)
                        : false;

                if(sasl_enabled) {
                    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_BOOTSTRAP_SERVERS);
                    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP);
                    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    kafkaProperties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 60000); // Set the maximum time to try and fetch metadata before throwing an exception (60 seconds)
                    kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);
                    kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_PROTOCOL); // Use SASL over SSL for security
                    kafkaProperties.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM); // Authentication mechanism
                    kafkaProperties.put("ssl.enabled.protocols", SASL_TLS_VERSION);  // Specify allowed TLS versions
                    kafkaProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
                }else{
                    kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
                    kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP);
                    kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                    kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                }
                String clientId = "kafka-consumer-clientId-" + UUID.randomUUID();
                kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
                consumer = new KafkaConsumer<>(kafkaProperties) ;
                consumer.subscribe(Collections.singletonList(KAFKA_INPUT_TOPIC));




















                System.out.println("Connected to Kafka Broker...");
                while (true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> {

                        try {
                            System.out.println("data process from kafka to kafka"+record.value());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });

                }








    }


    private static Properties loadProperties() {

        Properties properties = new Properties();

        try (InputStream input = Patient_Vitals.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Unable to find the config.properties file.");
                System.exit(1);
            }
            properties.load(input);
        } catch (IOException e) {
           System.out.println("Error loading config.properties file."+e);
            System.exit(1);
        }

        return properties;
    }

    private static String loadconfigfile(String propertyName,Properties properties){
        String envVarValue = System.getenv(propertyName);

        return (envVarValue != null && !envVarValue.isEmpty()) ? envVarValue : properties.getProperty(propertyName);

    }

}