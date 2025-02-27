package com.vitals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class kafkaProducer {

    public static String KAFKA_BOOTSTRAP_SERVERS;
    public static String KAFKA_OUTPUT_TOPIC;
    public static String SASL_ENABLED;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static String SASL_MECHANISM;
    public static String SASL_TLS_VERSION;
    public static String SASL_PROTOCOL;

    public static void producer(String data) throws Exception {
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


        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProperties.put("acks", "all");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("topic",KAFKA_OUTPUT_TOPIC);
        kafkaProperties.put("batch.size", "5"); // Set the desired batch size in bytes
        kafkaProperties.put("linger.ms", "30000");
        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_PROTOCOL); // Use SASL over SSL for security
        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, SASL_MECHANISM); // Authentication mechanism
        kafkaProperties.put("ssl.enabled.protocols", SASL_TLS_VERSION);  // Specify allowed TLS versions
        kafkaProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");


        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);

        producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC,data));
        System.out.println("Produced to "+KAFKA_OUTPUT_TOPIC+": " + data );

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