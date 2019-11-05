package com.clairvoyant.paypal.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Configurations {

	private static Config config = ConfigFactory.load();

	private static String BOOTSTRAP_SERVERS = config.getString("kafka.brokers");
	private static String GROUP_ID = config.getString("kafka.group_id");
	private static String KEY_DESERIALIZER = config.getString("kafka.key_deserializer");
	private static String VALUE_DESERIALIZER = config.getString("kafka.value_deserializer");
	private static String KAFKA_OFFSET_RESET = config.getString("kafka.auto_offset_reset");

	public static Properties getProducerProperties(){
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		producerProperties.put("group.id", GROUP_ID);
		producerProperties.put("auto.offset.reset", KAFKA_OFFSET_RESET);
		producerProperties.put("key.serializer", StringSerializer.class.getName());
		producerProperties.put("value.serializer", StringSerializer.class.getName());

		return producerProperties;
	}

	public static Properties getConsumerProperties(){
		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		consumerProperties.put("group.id", GROUP_ID);
		consumerProperties.put("auto.offset.reset", KAFKA_OFFSET_RESET);
		consumerProperties.put("key.deserializer", KEY_DESERIALIZER);
		consumerProperties.put("value.deserializer", VALUE_DESERIALIZER);

		return consumerProperties;
	}


}
