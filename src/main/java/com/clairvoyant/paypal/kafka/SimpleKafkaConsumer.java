package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class SimpleKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    public static void main(String[] args) {

        Config config = ConfigFactory.load();

        String CONSUMER_TOPIC = config.getString("kafka.consumer_topic");

        LOGGER.info("CONSUMER_TOPIC: " + CONSUMER_TOPIC);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Configurations.getConsumerProperties());
        consumer.subscribe(Collections.singletonList(CONSUMER_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Record: " + record.value());
            }
        }

    }
}