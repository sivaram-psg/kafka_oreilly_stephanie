package com.company;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerDemo {
	public static void main(String args[]) {
		System.out.println("main");
       Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
       Properties properties = new Properties();
       String bootstrapServers = "localhost:9092";
       String groupId = "application5";
       String topic = "first_topic";
   	
       //create customer configuration
	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	
	//create consumer
	KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
	
	//subscribe to topic
	
	consumer.subscribe(Collections.singleton(topic));
	
	//poll data
	while(true) {
		ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(100));
		for(ConsumerRecord<String, String> consumerRecord:consumerRecords) {
			logger.info("Key: "+consumerRecord.key()+", Value:"+consumerRecord.value());
			logger.info("Topic: "+consumerRecord.topic()+", Partition:"+consumerRecord.partition());
		}
	}
	}

}
