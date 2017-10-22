package com.spider.parser.consumers;

import java.io.IOException;
import java.util.ArrayList;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;

public class KafkaHTMLLoopConsumer implements Runnable {

	private final Logger logger = LoggerFactory.getLogger(KafkaHTMLLoopConsumer.class);

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

		public KafkaHTMLLoopConsumer build() throws IOException {
			return new KafkaHTMLLoopConsumer(this);
		}
	}

	public KafkaHTMLLoopConsumer(Builder builder) throws IOException {
		if (builder.properties == null) {
			throw new NullPointerException("Properties cannot be null");
		}

		this.topics = builder.topics;
		this.id = builder.id;
		this.groupId = builder.groupId;
		this.producer = builder.producer;

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

						String html = page.getHtmlRaw();
						if (html == null || html.isEmpty()) {
							throw new IOException("No HTML to parse on " + page.getLocation().toString());
						}
						Document doc = Jsoup.parse(html);
						logger.info("Parsed: " + doc.title());
						List<String> links = parseLinks(doc);
						logger.info("\nLinks detected: (%d)", links.size());
						page.setLinks(links);

						ProducerRecord<String, Page> producerRecord = new ProducerRecord<String, Page>(
								Topics.DOCUMENTS_PARSED, page);
						producer.send(producerRecord);
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

	/**
	 * This method only parse links(href), not imports or media hiperlinks.
	 * 
	 * @param doc
	 * @return
	 */
	private static List<String> parseLinks(Document doc) {
		Elements links = doc.select("a[href]");
		List<String> parseLinks = new ArrayList<>(links.size());
		for (Element link : links) {
			String l = link.attr("abs:href");
			if (l != null && !l.isEmpty()) {
				parseLinks.add(link.attr("abs:href"));
			}
		}

		return parseLinks;
	}

	public void shutdown() {
		consumer.wakeup();
	}

}