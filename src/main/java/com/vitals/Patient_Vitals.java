package com.vitals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Patient_Vitals {

    public static String KAFKA_OUTPUT_TOPIC;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static int INTERVAL;
    public static String KAFKA_BOOTSTRAP_SERVERS;


    private static final Map<Integer, String> patientNames = new HashMap<>();

    static KafkaProducer<String, String> producer;

    public static void main(String[]args) throws Exception {
        // Set up Kafka producer properties
        Properties properties = loadProperties();


        KAFKA_OUTPUT_TOPIC = loadconfigfile("KAFKA_OUTPUT_TOPIC",properties);
        SASL_USERNAME = loadconfigfile("SASL_USERNAME", properties);
        SASL_PASSWORD = loadconfigfile("SASL_PASSWORD", properties);
        INTERVAL = Integer.parseInt(loadconfigfile("INTERVAL",properties))*1000;
        KAFKA_BOOTSTRAP_SERVERS = loadconfigfile("KAFKA_BOOTSTRAP_SERVERS",properties);

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        kafkaProperties.put("acks", "all");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("topic",KAFKA_OUTPUT_TOPIC);
        kafkaProperties.put("batch.size", "5"); // Set the desired batch size in bytes
        kafkaProperties.put("linger.ms", "30000");
//        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
//        kafkaProperties.put("ssl.enabled.protocols", "TLSv1.2");
//        kafkaProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");

        producer = new KafkaProducer<>(kafkaProperties);

        String filePath = "./vitals_data.txt";
        sendVitals(filePath, INTERVAL);

    }

    public static void sendVitals(String filePath, int intervalMillis) {
        // Set up Kafka producer properties

        ObjectMapper objectMapper = new ObjectMapper();
        while(true) {
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
                while ((line = reader.readLine()) != null) {  // Read line by line
                    try {
                        // Validate and parse JSON
                        JsonNode jsonNode = objectMapper.readTree(line);
                        String jsonString = objectMapper.writeValueAsString(jsonNode);

                        producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, jsonString));
                        System.out.println("Sent message: " + jsonString);

                        // Sleep for specified interval
                        TimeUnit.MILLISECONDS.sleep(intervalMillis);
                    } catch (Exception e) {
                        System.err.println("Invalid JSON: " + line);
                        e.printStackTrace();
                    }
                }
            }catch (IOException e) {
                e.printStackTrace();
            } finally {
                producer.close();
            }
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

    static String loadconfigfile(String propertyName, Properties properties){
        String envVarValue = System.getenv(propertyName);

        return (envVarValue != null && !envVarValue.isEmpty()) ? envVarValue : properties.getProperty(propertyName);

    }

}