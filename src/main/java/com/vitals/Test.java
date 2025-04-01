//package com.vitals;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.kafka.clients.CommonClientConfigs;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.config.SaslConfigs;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//import static com.vitals.Patient_Vitals.loadconfigfile;
//import static com.vitals.propertyReader.properties;
//
//public class Test {
//    public static String KAFKA_OUTPUT_TOPIC;
//    public static String SASL_USERNAME;
//    public static String SASL_PASSWORD;
//    public static int INTERVAL;
//
//    public static void main(String[] args) {
//        KAFKA_OUTPUT_TOPIC = loadconfigfile("KAFKA_OUTPUT_TOPIC",properties);
//        SASL_USERNAME = loadconfigfile("SASL_USERNAME", properties);
//        SASL_PASSWORD = loadconfigfile("SASL_PASSWORD", properties);
//        INTERVAL = Integer.parseInt(loadconfigfile("INTERVAL",properties))*1000;
//
//
//        Properties kafkaProperties = new Properties();
//        kafkaProperties.put("bootstrap.servers", "localhost:9092");
//        kafkaProperties.put("acks", "all");
//        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProperties.put("topic",KAFKA_OUTPUT_TOPIC);
//        kafkaProperties.put("batch.size", "5"); // Set the desired batch size in bytes
//        kafkaProperties.put("linger.ms", "30000");
//        kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
//        kafkaProperties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
//        kafkaProperties.put("ssl.enabled.protocols", "TLSv1.2");
//        kafkaProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SASL_USERNAME + "\" password=\"" + SASL_PASSWORD + "\";");
//
//
//        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
//        String filePath = "./vitals_data.txt"; // Path to the JSON file
//        sendVitals(filePath, INTERVAL*1000);
//    }
//
//    public static void sendVitals(String filePath, int intervalMillis) {
//        // Set up Kafka producer properties
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092"); // Change if using a remote Kafka server
//        properties.put("key.serializer", StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
//
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//        ObjectMapper objectMapper = new ObjectMapper();
//        String topic = "vitals-topic"; // Kafka topic name
//
//        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
//            String line;
//            while(true) {
//                while ((line = reader.readLine()) != null) {  // Read line by line
//                    try {
//                        // Validate and parse JSON
//                        JsonNode jsonNode = objectMapper.readTree(line);
//                        String jsonString = objectMapper.writeValueAsString(jsonNode);
//
//                        // Send JSON data to Kafka
//                        producer.send(new ProducerRecord<>(topic, jsonString));
//                        System.out.println("Sent message: " + jsonString);
//
//                        // Sleep for specified interval
//                        TimeUnit.MILLISECONDS.sleep(intervalMillis);
//                    } catch (Exception e) {
//                        System.err.println("Invalid JSON: " + line);
//                        e.printStackTrace();
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }
//    }
//}