package com.spider.repository.application;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import com.spider.common.kafka.configuration.Topics;
import com.spider.common.models.Page;
import com.spider.repository.configuration.KafkaConfiguration;
import com.spider.repository.consumers.KafkaPageLoopConsumer;

public class App {

	public static void main(String[] args) throws Exception {
		Properties producerProperties = KafkaConfiguration.readProducerConfig();
		KafkaProducer<String, Page> producer = new KafkaProducer<>(producerProperties);

		Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "password" ) );
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
		KafkaPageLoopConsumer.Builder builder = new KafkaPageLoopConsumer.Builder();		
		builder.topics(Topics.DOCUMENTS_PARSED).producer(producer).properties(KafkaConfiguration.readConsumerConfig()).driver(driver);
		executor.submit(builder.build());
		executor.submit(builder.build());

		stop(producer, executor, driver);
	}

	private static void stop(KafkaProducer<?, ?> producer, ExecutorService consumers, Driver driver) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				System.out.println("Shutdown, waiting producer and consumers end their work!");
				producer.close();
				consumers.shutdown();
				driver.close();
			}
		});
	}

}
