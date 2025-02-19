import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.Random;
import java.time.Instant;

public class Patient_Vitals {

    public static void main(String[]args) throws Exception {
        // Set up Kafka producer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", propertyReader.propertyReader("KAFKA_BOOTSTRAP_SERVERS"));
        kafkaProperties.put("acks", "all");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("topic",propertyReader.propertyReader("KAFKA_OUTPUT_TOPIC"));
        kafkaProperties.put("batch.size", "5"); // Set the desired batch size in bytes
        kafkaProperties.put("linger.ms", "1000");

        kafkaProperties.put("security.protocol", propertyReader.propertyReader("SASL_PROTOCOL"));
        kafkaProperties.put("sasl.mechanism", propertyReader.propertyReader("SASL_MECHANISM"));
        kafkaProperties.put("ssl.enabled.protocols", propertyReader.propertyReader("SASL_TLS_VERSION"));

        String username = propertyReader.propertyReader("SASL_USERNAME");
        String password = propertyReader.propertyReader("SASL_PASSWORD");

        kafkaProperties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + username + "\" " +
                        "password=\"" + password + "\";");


        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
        String topic = propertyReader.propertyReader("KAFKA_OUTPUT_TOPIC");
        for (;;) {
            // Send the JSON to the Kafka topic
            producer.send(new ProducerRecord<>(topic,  alertVitals()));
            Thread.sleep(1000);
            producer.send(new ProducerRecord<>(topic,  normalVitals()));

            System.out.println("Published patient vitals:" + alertVitals());
            System.out.println("Published alert vitals:" + normalVitals());
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

            vitals.put("time", Instant.now().getEpochSecond()); // Current timestamp

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

            vitals.put("time", Instant.now().getEpochSecond()); // Current timestamp

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
}