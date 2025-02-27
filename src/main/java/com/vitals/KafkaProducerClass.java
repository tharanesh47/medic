package com.vitals;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.Properties;

class KafkaProducerClass {
    private static KafkaProducer<String, String> producer = null;

    public KafkaProducerClass() {
        Properties kafkaProperties = new Properties();

        String saslEnvValue = Patient_Vitals.SASL_ENABLED; // Define the variable
        boolean sasl_enabled = (saslEnvValue != null && !saslEnvValue.isEmpty())
                ? Boolean.parseBoolean(saslEnvValue)
                : false;

        kafkaProperties.put("bootstrap.servers", Patient_Vitals.KAFKA_BOOTSTRAP_SERVERS);
        kafkaProperties.put("acks", "all");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (sasl_enabled) {
            kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, Patient_Vitals.SASL_PROTOCOL);
            kafkaProperties.put(SaslConfigs.SASL_MECHANISM, Patient_Vitals.SASL_MECHANISM);
            kafkaProperties.put("ssl.enabled.protocols", Patient_Vitals.SASL_TLS_VERSION);
            kafkaProperties.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                            + Patient_Vitals.SASL_USERNAME + "\" password=\"" + Patient_Vitals.SASL_PASSWORD + "\";");
        }

        kafkaProperties.put("batch.size", "5");
        kafkaProperties.put("linger.ms", "300000");

        producer = new KafkaProducer<>(kafkaProperties);
    }

    public void publishMessage(String data) {
        try {
            System.out.println("Publishing message: {}"+data);
            ProducerRecord<String, String> record = new ProducerRecord<>(Patient_Vitals.KAFKA_OUTPUT_TOPIC, data);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                } else {
                    System.out.println("Message published successfully! " + metadata);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}