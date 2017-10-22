package com.spider.downloader.consumers;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;

public class KafkaURILoopConsumer implements Runnable {
	
	private final Logger logger = LoggerFactory.getLogger(KafkaURILoopConsumer.class);

	private final KafkaConsumer<String, LinkedHashMap<String, String>> consumer;
	private KafkaProducer<String, Page> producer;
	private List<String> topics;
	private Integer id;
	private String groupId;

	public static class Builder {
		private List<String> topics;
		private Integer id;
		private Properties properties;
		private String groupId;
		private KafkaProducer<String, Page> producer;

		public Builder topics(String... topics) {
			this.topics = Arrays.asList(topics);
			return this;
		}

		public Builder id(int id) {
			this.id = id;
			return this;
		}

		public Builder properties(Properties properties) {
			this.properties = properties;
			return this;
		}

		public Builder groupId(String groupId) {
			this.groupId = groupId;
			return this;
		}
		
		public Builder producer(KafkaProducer<String, Page> producer) {
			this.producer = producer;
			return this;
		}

		public KafkaURILoopConsumer build() throws IOException {
			return new KafkaURILoopConsumer(this);
		}
	}

	public KafkaURILoopConsumer(Builder builder) throws IOException {
		if (builder.properties == null) {
			throw new NullPointerException("Properties cannot be null");
		}

		this.topics = builder.topics;
		this.id = builder.id;
		this.groupId = builder.groupId;
		this.producer = builder.producer;

		builder.properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		if (id != null) {
			builder.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, String.valueOf(builder.id));
		}

		this.consumer = new KafkaConsumer<>(builder.properties);
	}

	@Override
	public void run() {
		try {
			consumer.subscribe(topics);
			ObjectMapper mapper = new ObjectMapper();
			while (true) {
				// Althought Consumer uses a custom JSON Deserializer it always returned a
				// LinkedHashMap
				ConsumerRecords<String, LinkedHashMap<String, String>> records = consumer.poll(Long.MAX_VALUE);
				for (ConsumerRecord<String, LinkedHashMap<String, String>> record : records) {
					logger.info("Reading kafka: offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
							record.value());
					try {
						Page page = mapper.convertValue(record.value(), new TypeReference<Page>() {});
						logger.info(page.toString());
						
						String html = IOUtils.toString(page.getLocation().toURL().openStream(), "utf-8");
						if(html == null || html.isEmpty()) {
							throw new IOException("Failed downloading HTML from " + page.getLocation().toString() + "fails");
						}
						page.setHtmlRaw(html);
						logger.info("Downloaded: " + page.toString());
						
						ProducerRecord<String, Page> producerRecord = new ProducerRecord<String, Page>(
								Topics.DOCUMENTS, page);
						producer.send(producerRecord);
						logger.info("Sending kafka: topic = %s, key = %s, value=%s%n", producerRecord.topic(),
								producerRecord.key(), producerRecord.value());
						producer.send(new ProducerRecord<String, Page>(Topics.DOCUMENTS, page));
						logger.info("Sending kafka: topic = %s, key = %s, value=%s%n", producerRecord.topic(),
								producerRecord.key(), producerRecord.value());
					} catch (Exception e) {
						logger.error("Exception", e);
					}
				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

}