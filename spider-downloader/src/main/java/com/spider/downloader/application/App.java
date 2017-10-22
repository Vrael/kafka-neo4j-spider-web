package com.spider.downloader.application;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;
import com.spider.downloader.configuration.KafkaConfiguration;
import com.spider.downloader.consumers.KafkaURILoopConsumer;

public class App {

	public static void main(String[] args) throws Exception {
		Properties producerProperties = KafkaConfiguration.readProducerConfig();
		KafkaProducer<String, Page> producer = new KafkaProducer<>(producerProperties);

		ExecutorService executor = Executors.newFixedThreadPool(2);
		KafkaURILoopConsumer.Builder builder = new KafkaURILoopConsumer.Builder();
		Properties consumerProperties = KafkaConfiguration.readConsumerConfig();
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");

		builder.topics(Topics.LINKS).groupId(UUID.randomUUID().toString()).producer(producer)
				.properties(consumerProperties);
		executor.submit(builder.build());
		executor.submit(builder.build());

		stop(producer, executor);
	}

	private static void stop(KafkaProducer<?, ?> producer, ExecutorService consumers) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdown, waiting producer and consumers end their work!");
				producer.close();
				consumers.shutdown();
			}
		});
	}

}
