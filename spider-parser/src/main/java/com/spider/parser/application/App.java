package com.spider.parser.application;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;
import com.spider.parser.configuration.KafkaConfiguration;
import com.spider.parser.consumers.KafkaHTMLLoopConsumer;

public class App {

	public static void main(String[] args) throws Exception {
		Properties producerProperties = KafkaConfiguration.readProducerConfig();
		KafkaProducer<String, Page> producer = new KafkaProducer<>(producerProperties);

		ExecutorService executor = Executors.newFixedThreadPool(2);
		KafkaHTMLLoopConsumer.Builder builder = new KafkaHTMLLoopConsumer.Builder();
		builder.topics(Topics.DOCUMENTS).producer(producer).properties(KafkaConfiguration.readConsumerConfig());
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
