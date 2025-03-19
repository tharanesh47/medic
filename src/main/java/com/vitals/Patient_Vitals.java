package com.vitals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class Patient_Vitals {

    public static String KAFKA_BOOTSTRAP_SERVERS;
    public static String KAFKA_OUTPUT_TOPIC;
    public static String SASL_ENABLED;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static String SASL_MECHANISM;
    public static String SASL_TLS_VERSION;
    public static String SASL_PROTOCOL;

    private static final Map<Integer, String> patientNames = new HashMap<>();

    static {
        patientNames.put(1, "Tharanesh");
        patientNames.put(2, "Akshay");
        patientNames.put(3, "Sridhar");
        patientNames.put(4, "Tanmaya");
        patientNames.put(5, "Sugam");
    }

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
            String alertVitals = generateVitals(true);
            producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, alertVitals));
            System.out.println("Alert vitals: " + alertVitals);
            System.out.println();
            Thread.sleep(1000);
            String normalVitals = generateVitals(false);
            producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC,  normalVitals));
            System.out.println("Normal vitals: " + normalVitals);
            System.out.println();
            Thread.sleep(1000);
        }

    }

    private static String generateVitals(boolean isAlert) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectNode vitals = objectMapper.createObjectNode();
            Random rand = new Random();

            ZonedDateTime currentUtcTime = ZonedDateTime.now(ZoneId.of("UTC"));
            vitals.put("time", currentUtcTime.toEpochSecond());

            int id = getRandomValue(rand, 1, 5);
            vitals.put("Id", id);
            vitals.put("Name", patientNames.getOrDefault(id, "Unknown Patient"));

            if (isAlert) {
                vitals.put("body_temperature_°F", getRandomValueOutOfRange(rand, 96.0, 100.1));
                vitals.put("heart_rate_bpm", getRandomValueOutOfRange(rand, 60, 100));
                vitals.put("systolic_pressure_mmHg", getRandomValueOutOfRange(rand, 90, 120));
                vitals.put("diastolic_pressure_mmHg", getRandomValueOutOfRange(rand, 40, 120));
                vitals.put("respiratory_rate_breaths/min", getRandomValueOutOfRange(rand, 5, 25));
                vitals.put("oxygen_saturation%", getRandomValue(rand, 70, 90));
                vitals.put("blood_glucose_level_mg/dL", getRandomValueOutOfRange(rand, 70, 120));
                vitals.put("etco2_mmHg", getRandomValueOutOfRange(rand, 20, 60));
                vitals.put("skin_turgor_recoilTimeSec", getRandomValueOutOfRange(rand, 5, 10));
                vitals.put("isAlert", true);
            } else {
                vitals.put("body_temperature_°F", getRandomValue(rand, 96.0, 100.0));
                vitals.put("heart_rate_bpm", getRandomValue(rand, 60, 100));
                vitals.put("systolic_pressure_mmHg", getRandomValue(rand, 90, 120));
                vitals.put("diastolic_pressure_mmHg", getRandomValue(rand, 40, 120));
                vitals.put("respiratory_rate_breaths/min", getRandomValue(rand, 12, 20));
                vitals.put("oxygen_saturation%", getRandomValue(rand, 90, 100));
                vitals.put("blood_glucose_level_mg/dL", getRandomValue(rand, 70, 120));
                vitals.put("etco2_mmHg", getRandomValue(rand, 35, 45));
                vitals.put("skin_turgor_recoilTimeSec", getRandomValue(rand, 1, 3));
                vitals.put("isAlert", false);
            }

            return objectMapper.writeValueAsString(vitals);
        } catch (Exception e) {
            e.printStackTrace();
            return "Data not published...Issue in generating patient vitals";
        }
    }

    // Helper method to generate values outside the normal range (for int)
    private static int getRandomValueOutOfRange(Random rand, int min, int max) {
        if (rand.nextBoolean()) { // 50% chance to generate below min
            return min - (rand.nextInt(10) + 1); // Ensures a valid negative offset
        } else { // 50% chance to generate above max
            return max + (rand.nextInt(10) + 1);
        }
    }

    // Helper method to generate values outside the normal range (for double)
    private static double getRandomValueOutOfRange(Random rand, double min, double max) {
        if (rand.nextBoolean()) { // 50% chance to go below min
            return Math.round((min - (rand.nextDouble() * 5 + 0.1)) * 10.0) / 10.0; // Prevents zero range
        } else { // 50% chance to go above max
            return Math.round((max + (rand.nextDouble() * 5 + 0.1)) * 10.0) / 10.0; // Prevents zero range
        }
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