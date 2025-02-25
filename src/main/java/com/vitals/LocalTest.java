//package com.vitals;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.serialization.StringSerializer;
//
//import java.util.Properties;
//import java.util.Random;
//import java.time.Instant;
//
//public class LocalTest {
//
//    public static void main(String[]args) throws Exception {
//        for (;;) {
//            alertVitals();
//            normalVitals();
//            Thread.sleep(1500);
//        }
//    }
//
//    public static void alertVitals() {
//        // Set up Kafka producer properties
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");  // Change if using a remote Kafka server
//        properties.put("key.serializer", StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
//
//        // Create Kafka producer
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        try {
//            // Create ObjectMapper for JSON handling
//            ObjectMapper objectMapper = new ObjectMapper();
//
//            // Create a JSON object for vitals
//            ObjectNode vitals = objectMapper.createObjectNode();
//            vitals.put("Id", 1);
//            vitals.put("time", Instant.now().getEpochSecond()); // Current timestamp
//
//            // Generate random values for vitals outside the normal range
//            Random rand = new Random();
//            vitals.put("body_temperature(°F)", getRandomValueOutOfRange(rand, 95.0, 104.0));
//            vitals.put("heart_rate(bpm)", getRandomValueOutOfRange(rand, 40, 160));
//            vitals.put("systolic_pressure(mmHg)", getRandomValueOutOfRange(rand, 60, 200));
//            vitals.put("diastolic_pressure(mmHg)", getRandomValueOutOfRange(rand, 40, 120));
//            vitals.put("respiratory_rate(breaths/min)", getRandomValueOutOfRange(rand, 5, 30));
//            vitals.put("oxygen_saturation%", getRandomValueOutOfRange(rand, 60, 110));
//            vitals.put("blood_glucose_level(mg/dL)", getRandomValueOutOfRange(rand, 20, 400));
//            vitals.put("etco2(mmHg)", getRandomValueOutOfRange(rand, 20, 60));
//            vitals.put("skin_turgor(recoilTimeSec)", getRandomValueOutOfRange(rand, 5, 10));
//
//            // Convert JSON object to String
//            String jsonString = objectMapper.writeValueAsString(vitals);
//
//            // Send the JSON to the Kafka topic
//            String topic = "vitals-topic";  // Kafka topic name
//            //producer.send(new ProducerRecord<>(topic, null, jsonString));
//
//            System.out.println("Sent message: " + jsonString);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            // Close producer
//            producer.close();
//        }
//    }
//
//    public static void normalVitals() {
//        // Set up Kafka producer properties
//        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "localhost:9092");  // Change if using a remote Kafka server
//        properties.put("key.serializer", StringSerializer.class.getName());
//        properties.put("value.serializer", StringSerializer.class.getName());
//
//        // Create Kafka producer
//        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
//
//        try {
//            // Create ObjectMapper for JSON handling
//            ObjectMapper objectMapper = new ObjectMapper();
//
//            // Create a JSON object
//            ObjectNode vitals = objectMapper.createObjectNode();
//
//            // Add fixed fields
//            vitals.put("Id", 1);
//            vitals.put("time", Instant.now().getEpochSecond()); // Current timestamp
//
//            // Generate random values for vitals
//            Random rand = new Random();
//            vitals.put("body_temperature(°F)", getRandomValue(rand, 96.0, 100.0));
//            vitals.put("heart_rate(bpm)", getRandomValue(rand, 60, 100));
//            vitals.put("systolic_pressure(mmHg)", getRandomValue(rand, 90, 140));
//            vitals.put("diastolic_pressure(mmHg)", getRandomValue(rand, 60, 90));
//            vitals.put("respiratory_rate(breaths/min)", getRandomValue(rand, 12, 20));
//            vitals.put("oxygen_saturation%", getRandomValue(rand, 90, 100));
//            vitals.put("blood_glucose_level(mg/dL)", getRandomValue(rand, 70, 140));
//            vitals.put("etco2(mmHg)", getRandomValue(rand, 35, 45));
//            vitals.put("skin_turgor(recoilTimeSec)", getRandomValue(rand, 1, 3));
//
//            // Convert to JSON string and print
//            String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(vitals);
//
//            // Send the JSON to the Kafka topic
//            String topic = "vitals-topic";  // Kafka topic name
//            //producer.send(new ProducerRecord<>(topic, null, jsonString));
//
//            System.out.println("Sent message: " + jsonString);
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }finally {
//            // Close producer
//            producer.close();
//        }
//    }
//
//    // Helper method to generate values outside the normal range (for int)
//    private static int getRandomValueOutOfRange(Random rand, int min, int max) {
//        if (rand.nextBoolean()) { // 50% chance to generate below min
//            return min - (rand.nextInt(10) + 1); // Ensures a valid negative offset
//        } else { // 50% chance to generate above max
//            return max + (rand.nextInt(10) + 1);
//        }
//    }
//
//    // Helper method to generate values outside the normal range (for double)
//    private static double getRandomValueOutOfRange(Random rand, double min, double max) {
//        if (rand.nextBoolean()) { // 50% chance to go below min
//            return Math.round((min - (rand.nextDouble() * 5 + 0.1)) * 10.0) / 10.0; // Prevents zero range
//        } else { // 50% chance to go above max
//            return Math.round((max + (rand.nextDouble() * 5 + 0.1)) * 10.0) / 10.0; // Prevents zero range
//        }
//    }
//
//    // Helper method to generate random int values within a range
//    private static int getRandomValue(Random rand, int min, int max) {
//        return rand.nextInt((max - min) + 1) + min;
//    }
//
//    // Helper method to generate random double values within a range
//    private static double getRandomValue(Random rand, double min, double max) {
//        return Math.round((rand.nextDouble() * (max - min) + min) * 10.0) / 10.0; // Rounds to 1 decimal place
//    }
//}