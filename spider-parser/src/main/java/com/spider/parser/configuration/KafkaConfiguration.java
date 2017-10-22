package com.spider.parser.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConfiguration {
	
	public static Properties readProducerConfig() throws IOException  {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		try (InputStream resourceStream = loader.getResourceAsStream("producer.properties")) {
			props.load(resourceStream);
		}
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.spider.common.kafka.serializers.KafkaJsonSerializer");
		
		return props;
	}
	
	public static Properties readConsumerConfig() throws IOException  {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		try (InputStream resourceStream = loader.getResourceAsStream("consumer.properties")) {
			props.load(resourceStream);
		}
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.spider.common.kafka.serializers.KafkaJsonDeserializer");
		
		return props;
	}
	
}
