package com.spider.repository.consumers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;

public class KafkaPageLoopConsumer implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(KafkaPageLoopConsumer.class);

	private final KafkaConsumer<String, LinkedHashMap<String, String>> consumer;
	private KafkaProducer<String, Page> producer;
	private List<String> topics;
	private Integer id;
	private String groupId;
	private Session session;

	public static class Builder {
		private List<String> topics;
		private Integer id;
		private Properties properties;
		private String groupId;
		private KafkaProducer<String, Page> producer;
		private Session session;

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

		public Builder driver(Driver driver) {
			this.session = driver.session();
			return this;
		}

		public KafkaPageLoopConsumer build() throws IOException {
			return new KafkaPageLoopConsumer(this);
		}
	}

	public KafkaPageLoopConsumer(Builder builder) throws IOException {
		if (builder.properties == null) {
			throw new NullPointerException("Properties cannot be null");
		}

		this.topics = builder.topics;
		this.id = builder.id;
		this.groupId = builder.groupId;
		this.producer = builder.producer;
		this.session = builder.session;

		// If no groupId is set, then consumer starts from the beginning with random
		// groupId
		if (groupId == null || groupId.isEmpty()) {
			this.groupId = UUID.randomUUID().toString();
			builder.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			builder.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
		}

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
						Page page = mapper.convertValue(record.value(), new TypeReference<Page>() {
						});
						logger.info(page.toString());

						saveLinksOnNeo4j(page);

						sendLinksToKafka(page);
					} catch (Exception e) {
						logger.error("Exception", e);
					}
				}
			}
		} catch (WakeupException e) {
			// ignore for shutdown
		} finally {
			consumer.close();
			session.close();
		}
	}

	public void shutdown() {
		consumer.wakeup();
	}

	public void saveLinksOnNeo4j(Page page) {
		// Create or update source node
		String source = page.getLocation().toString();
		session.run("MERGE (o:Page{ url: {link} }) ON CREATE SET o = {url: {link}} ON MATCH SET o += {url: {link}}",
				Values.parameters("link", source));

		for (String link : page.getLinks()) {
			if (link != null && !link.isEmpty()) {
				// Create or update destination node
				session.run("MERGE (d:Page{ url: {link} }) ON CREATE SET d = {url: {link}} ON MATCH SET d += {url: {link}}",
						Values.parameters("link", link));

				// Create relationship between source -> destination node
				session.run("MERGE (o:Page{ url: {source} }) MERGE (d:Page{ url: {destination} }) MERGE (o)-[l:LINKS]->(d)", Values.parameters("source", source, "destination", link));

				logger.info("Saving neo4j: link = %s", link);
			}
		}
	}

	public void sendLinksToKafka(Page page) {
		for (String link : page.getLinks()) {
			ProducerRecord<String, Page> producerRecord;
			try {
				producerRecord = new ProducerRecord<String, Page>(Topics.LINKS,
						(new Page.Builder()).location(link).build());
				producer.send(producerRecord);
				logger.info("Sending kafka: topic = %s, key = %s, value = %s%n", producerRecord.topic(),
						producerRecord.key(), producerRecord.value());
			} catch (URISyntaxException e) {
				logger.error("Error building URI from link: " + link, e);
			}
		}
	}

}