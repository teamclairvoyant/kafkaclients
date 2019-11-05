package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Configurations {

	private static final Logger LOGGER = LoggerFactory.getLogger(Configurations.class);

	private static Config config = ConfigFactory.load();

	private static String BOOTSTRAP_SERVERS = config.getString("kafka.brokers");
	private static String GROUP_ID = config.getString("kafka.group_id");
	private static String KAFKA_OFFSET_RESET = config.getString("kafka.auto_offset_reset");

	public static Properties getProducerProperties(){
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		producerProperties.put("group.id", GROUP_ID);
		producerProperties.put("auto.offset.reset", KAFKA_OFFSET_RESET);
		producerProperties.put("key.serializer", StringSerializer.class.getName());
		producerProperties.put("value.serializer", StringSerializer.class.getName());

		LOGGER.info("BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
		LOGGER.info("GROUP_ID: " + GROUP_ID);

		return producerProperties;
	}

	public static Properties getConsumerProperties(){
		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		consumerProperties.put("group.id", GROUP_ID);
		consumerProperties.put("auto.offset.reset", KAFKA_OFFSET_RESET);
		consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
		consumerProperties.put("value.deserializer", StringDeserializer.class.getName());

		LOGGER.info("BOOTSTRAP_SERVERS: " + BOOTSTRAP_SERVERS);
		LOGGER.info("GROUP_ID: " + GROUP_ID);
		LOGGER.info("KAFKA_OFFSET_RESET: " + KAFKA_OFFSET_RESET);

		return consumerProperties;
	}


}
