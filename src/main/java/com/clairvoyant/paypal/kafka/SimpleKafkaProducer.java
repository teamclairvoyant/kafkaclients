package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleKafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    public static void main(String[] args) throws InterruptedException {

        Config config = ConfigFactory.load();

        String PRODUCER_TOPIC = config.getString("kafka.producer_topic");

        LOGGER.info("PRODUCER_TOPIC: " + PRODUCER_TOPIC);

        Producer<String, String> producer = new KafkaProducer<>(Configurations.getProducerProperties());

        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<>(PRODUCER_TOPIC, Integer.toString(i), "Test Message: " + i));
            LOGGER.info("Message sent successfully");
            Thread.sleep(1000);
        }

        producer.close();

    }
}