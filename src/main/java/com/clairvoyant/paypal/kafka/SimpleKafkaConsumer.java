package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    public static void main(String[] args) {

        Config config = ConfigFactory.load();

        String BOOTSTRAP_SERVERS = config.getString("kafka.brokers");
        String GROUP_ID = config.getString("kafka.group_id");
        String CONSUMER_TOPIC = config.getString("kafka.consumer_topic");
        String KEY_DESERIALIZER = config.getString("kafka.key_deserializer");
        String VALUE_DESERIALIZER = config.getString("kafka.value_deserializer");
        String KAFKA_OFFSET_RESET = config.getString("kafka.auto_offset_reset");

        LOGGER.info("BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
        LOGGER.info("GROUP_ID: " + GROUP_ID);
        LOGGER.info("CONSUMER_TOPIC: " + CONSUMER_TOPIC);
        LOGGER.info("KEY_DESERIALIZER: " + KEY_DESERIALIZER);
        LOGGER.info("VALUE_DESERIALIZER: " + VALUE_DESERIALIZER);

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("auto.offset.reset", KAFKA_OFFSET_RESET);
        props.put("key.deserializer", KEY_DESERIALIZER);
        props.put("value.deserializer", VALUE_DESERIALIZER);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(CONSUMER_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Record: " + record.value());
            }
        }

    }
}