package com.company;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo2WithKeys {
	
	public static void main(String args[]) throws InterruptedException, ExecutionException {
		final Logger logger= LoggerFactory.getLogger(ProducerDemo2WithKeys.class);
		System.out.println("test1");
		String kafkaServer = "localhost:9092";
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		for(int i=0;i<10;i++) {
		//create ProducerRecord
		String topic = "first_topic";
		String value = "hello world "+i;
		String key = "id_"+i;
		ProducerRecord<String,String> record = new ProducerRecord<>(topic,key,value);
		//send data
		//producer.send(record);
		logger.info("key: "+key);
	producer.send(record, new Callback() {

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			//executed each time success or failure
			if(exception == null) {
			logger.info("Received message \n"+
			"Topic: "+metadata.topic()+"\n"+
					"Partition: "+metadata.partition()+"\n"+
			"offset: "+metadata.offset()+"\n"+
					"timestamp "+metadata.timestamp());
			}
			else {
				logger.error("Error while producing "+exception);
			}
			
		}
		
	}).get();
		}
		producer.close();
	}

}
