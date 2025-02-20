package com.vitals;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.Random;
import java.time.Instant;

public class Patient_Vitals {

    public static String KAFKA_BOOTSTRAP_SERVERS;
    public static String KAFKA_OUTPUT_TOPIC;
    public static String SASL_ENABLED;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static String SASL_MECHANISM;
    public static String SASL_TLS_VERSION;
    public static String SASL_PROTOCOL;

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

        for (;;) {
            // Send the JSON to the Kafka topic
            String alertVitals = alertVitals();
            producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, alertVitals));
            System.out.println("Published alert vitals: " + alertVitals);
            System.out.println();
            Thread.sleep(1000);
            String normalVitals = normalVitals();
            producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC,  normalVitals));
            System.out.println("Published patient vitals: " + normalVitals);
            System.out.println();
            Thread.sleep(1000);
        }

    }

    public static String alertVitals() {
        try {
            // Create ObjectMapper for JSON handling
            ObjectMapper objectMapper = new ObjectMapper();

            // Create a JSON object for vitals
            ObjectNode vitals = objectMapper.createObjectNode();

            ZonedDateTime currentutcTime = ZonedDateTime.now(ZoneId.of("UTC"));

            vitals.put("time", currentutcTime.toEpochSecond()); // Current timestamp

            // Generate random values for vitals outside the normal range
            Random rand = new Random();
            vitals.put("Id", getRandomValue(rand, 1, 5));
            vitals.put("body_temperature(°F)", getRandomValueOutOfRange(rand, 95.0, 104.0));
            vitals.put("heart_rate(bpm)", getRandomValueOutOfRange(rand, 40, 160));
            vitals.put("systolic_pressure(mmHg)", getRandomValueOutOfRange(rand, 60, 200));
            vitals.put("diastolic_pressure(mmHg)", getRandomValueOutOfRange(rand, 40, 120));
            vitals.put("respiratory_rate(breaths/min)", getRandomValueOutOfRange(rand, 5, 30));
            vitals.put("oxygen_saturation%", getRandomValueOutOfRange(rand, 60, 110));
            vitals.put("blood_glucose_level(mg/dL)", getRandomValueOutOfRange(rand, 20, 400));
            vitals.put("etco2(mmHg)", getRandomValueOutOfRange(rand, 20, 60));
            vitals.put("skin_turgor(recoilTimeSec)", getRandomValueOutOfRange(rand, 5, 10));

            // Convert JSON object to String
            String jsonString = objectMapper.writeValueAsString(vitals);
            return jsonString;

        } catch (Exception e) {
            e.printStackTrace();
            return "Data not published...Issue in generating patient vitals";
        }
    }

    public static String normalVitals() {
        try {
            // Create ObjectMapper for JSON handling
            ObjectMapper objectMapper = new ObjectMapper();

            // Create a JSON object
            ObjectNode vitals = objectMapper.createObjectNode();

            ZonedDateTime currentutcTime = ZonedDateTime.now(ZoneId.of("UTC"));

            vitals.put("time", currentutcTime.toEpochSecond()); // Current timestamp

            // Generate random values for vitals
            Random rand = new Random();
            vitals.put("Id", getRandomValue(rand, 1, 5));
            vitals.put("body_temperature(°F)", getRandomValue(rand, 96.0, 100.0));
            vitals.put("heart_rate(bpm)", getRandomValue(rand, 60, 100));
            vitals.put("systolic_pressure(mmHg)", getRandomValue(rand, 90, 140));
            vitals.put("diastolic_pressure(mmHg)", getRandomValue(rand, 60, 90));
            vitals.put("respiratory_rate(breaths/min)", getRandomValue(rand, 12, 20));
            vitals.put("oxygen_saturation%", getRandomValue(rand, 90, 100));
            vitals.put("blood_glucose_level(mg/dL)", getRandomValue(rand, 70, 140));
            vitals.put("etco2(mmHg)", getRandomValue(rand, 35, 45));
            vitals.put("skin_turgor(recoilTimeSec)", getRandomValue(rand, 1, 3));

            // Convert to JSON string and print
            String jsonString = objectMapper.writeValueAsString(vitals);
            return jsonString;

        } catch (Exception e) {
            e.printStackTrace();
            return "Data not published...Issue in generating patient vitals";
        }
    }

    // Helper method to generate random int values outside normal range
    private static int getRandomValueOutOfRange(Random rand, int min, int max) {
        return rand.nextInt((max - min) + 1) + min; // generates a value outside of normal ranges
    }

    // Helper method to generate random double values outside normal range
    private static double getRandomValueOutOfRange(Random rand, double min, double max) {
        return Math.round((rand.nextDouble() * (max - min) + min) * 10.0) / 10.0; // Rounds to 1 decimal place
    }

    // Helper method to generate random int values within a range
    private static int getRandomValue(Random rand, int min, int max) {
        return rand.nextInt((max - min) + 1) + min;
    }

    // Helper method to generate random double values within a range
    private static double getRandomValue(Random rand, double min, double max) {
        return Math.round((rand.nextDouble() * (max - min) + min) * 10.0) / 10.0; // Rounds to 1 decimal place
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