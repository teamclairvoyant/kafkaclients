package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    public static void main(String[] args) throws InterruptedException {

        Config config = ConfigFactory.load();

        String BOOTSTRAP_SERVERS = config.getString("kafka.brokers");
        String GROUP_ID = config.getString("kafka.group_id");
        String PRODUCER_TOPIC = config.getString("kafka.producer_topic");
        String KEY_SERIALIZER = config.getString("kafka.key_deserializer");
        String VALUE_SERIALIZER = config.getString("kafka.value_deserializer");
        String KAFKA_OFFSET_RESET = config.getString("kafka.auto_offset_reset");

        LOGGER.info("BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
        LOGGER.info("GROUP_ID: " + GROUP_ID);
        LOGGER.info("PRODUCER_TOPIC: " + PRODUCER_TOPIC);
        LOGGER.info("KEY_SERIALIZER: " + KEY_SERIALIZER);
        LOGGER.info("VALUE_SERIALIZER: " + VALUE_SERIALIZER);

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", GROUP_ID);
        props.put("auto.offset.reset", KAFKA_OFFSET_RESET);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<>(PRODUCER_TOPIC, Integer.toString(i), "Test Message: " + i));
            LOGGER.info("Message sent successfully");
            Thread.sleep(1000);
        }

        producer.close();

    }
}