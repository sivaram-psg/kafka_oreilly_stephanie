package com.company;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThread {
	public static void main(String args[]) {
		System.out.println("main");

       new ConsumerDemoWithThread().run();

}

	private ConsumerDemoWithThread(){

	}

	private  void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		String bootstrapServers = "localhost:9092";
		String groupId = "application5";
		String topic = "first_topic";
		CountDownLatch latch = new CountDownLatch(1);
		Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

		Thread thread = new Thread(myConsumerRunnable);
		thread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("caught shutdown hook");
			((ConsumerRunnable)myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.info("Application has exited");
			}
		}));
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Thread interrupted");
		}
		finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerRunnable implements Runnable{
		private CountDownLatch latch;
		private KafkaConsumer<String,String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		Properties properties = new Properties();
        public ConsumerRunnable(String bootstrapServers,String groupId, String topic,CountDownLatch latch){
			//create customer configuration
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			consumer = new KafkaConsumer<String,String>(properties);
			consumer.subscribe(Collections.singleton(topic));

			this.latch = latch;

		}
		@Override
		public void run() {
			//poll data
			try {
				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
						logger.info("Key: " + consumerRecord.key() + ", Value:" + consumerRecord.value());
						logger.info("Topic: " + consumerRecord.topic() + ", Partition:" + consumerRecord.partition());
					}
				}
			}catch (WakeupException e)
			{
				logger.info("consumer thread interrupted");
			}
			finally {
				//closes the consumer
				consumer.close();
				//tells the main code the current consumer is completed
				latch.countDown();
			}
		}

		public void shutdown(){
        	// this closes the poll throws wakeup exception
                 consumer.wakeup();
		}
	}

}
