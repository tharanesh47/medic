//package com.vitals;
//
//package com.condense.connectors.impl;
//
//import java.time.Duration;
//import java.util.*;
//
//import com.condense.connectors.mysql.Mysql;
//import org.apache.kafka.clients.CommonClientConfigs;
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.config.SaslConfigs;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import com.condense.connectors.kafka.KafkaConsumerInterface;
//import com.condense.connectors.kafkamysql.Main;
//
//public class KafkaConsumerImpl {
//
//    public static long offsetValue;
//    public static int partition;
//    static Consumer<String, String> consumer;
//    public static String data;
//
//    @Override
//    public void startAndProcessData() {
//        String saslEnvValue = Main.SASL_ENABLED;
//        boolean saslEnabled = (saslEnvValue != null && !saslEnvValue.isEmpty())
//                ? Boolean.parseBoolean(saslEnvValue)
//                : false;
//
//        Properties kafkaProperties = new Properties();
//
//        if (saslEnabled) {
//            logger.info("SASL ENABLED {}", saslEnabled);
//
//            kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Main.KAFKA_BOOTSTRAP_SERVERS);
//            kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, Main.KAFKA_CONSUMER_GROUP);
//            kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            .kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//            kafkaProperties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 60000); // Max metadata fetch time (60 seconds)
//            kafkaProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 100000);
//            kafkaProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, Main.SASL_PROTOCOL); // Use SASL over SSL for security
//            kafkaProperties.put(SaslConfigs.SASL_MECHANISM, Main.SASL_MECHANISM); // Authentication mechanism
//            kafkaProperties.put("ssl.enabled.protocols", Main.SASL_TLS_VERSION);  // Allowed TLS versions
//            kafkaProperties.put("sasl.jaas.config",
//                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
//                            + Main.SASL_USERNAME + "\" password=\"" + Main.SASL_PASSWORD + "\";");
//        } else {
//            logger.info("SASL NOT ENABLED {}", saslEnabled);
//
//            kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Main.KAFKA_BOOTSTRAP_SERVERS);
//            kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, Main.KAFKA_CONSUMER_GROUP);
//            kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//            kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        }
//
//        String clientId = "kafka-consumer-clientId-" + UUID.randomUUID();
//        kafkaProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
//
//        consumer = new KafkaConsumer<>(kafkaProperties);
//        consumer.subscribe(Collections.singletonList(Main.KAFKA_TOPIC));
//
//        logger.info("Connected to Kafka Broker...");
//
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//
//            records.forEach(record -> {
//                offsetValue = record.offset();
//                partition = record.partition();
//                data = record.value();
//                logger.info("Record: {}, partition value: {}, offset value: {}", data, partition, offsetValue);
//
//                try {
//                    try {
//                        mysql.processAndInsertData(data, offsetValue, partition);
//                    } catch (Exception e) {
//                        logger.error("Error processing and inserting data into Bigtable", e);
//                    }
//                } catch (Exception e) {
//                    logger.error("Error processing and inserting data into Bigtable", e);
//                }
//            });
//
//            if (MysqlImpl.acknowledgmentSent) {
//                commitOffsets(MysqlImpl.storedOffsetValues, MysqlImpl.storedPartitionValues);
//                MysqlImpl.acknowledgmentSent = false;
//            }
//        }
//    }
//
//    public static void commitOffsets(List<Long> storedOffsetValues, List<Integer> storedPartitionValues) {
//        try {
//            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
//
//            for (int i = 0; i < storedOffsetValues.size(); i++) {
//                offsetsToCommit.put(new TopicPartition(Main.KAFKA_TOPIC, storedPartitionValues.get(i)),
//                        new OffsetAndMetadata(storedOffsetValues.get(i) + 1));
//            }
//
//            consumer.commitSync(offsetsToCommit);
//            logger.info("Offsets committed to Kafka");
//
//            MysqlImpl.storedOffsetValues.clear();
//            MysqlImpl.storedPartitionValues.clear();
//        } catch (TimeoutException e) {
//            logger.error("Timeout occurred while committing offsets. The operation timed out: " + e.getMessage(), e);
//            System.exit(1);
//        } catch (RebalanceInProgressException e) {
//            logger.error("Rebalance in progress while committing offsets. The consumer is rebalancing: " + e.getMessage(), e);
//            System.exit(1);
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//        try {
//            if (consumer != null) {
//                consumer.close();
//                logger.info("KafkaConsumer closed");
//            }
//        } catch (Exception e) {
//            logger.error("Error closing KafkaConsumer", e);
//        }
//    }
//}