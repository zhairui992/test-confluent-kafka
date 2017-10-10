package org.zr.kafka;

//import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import avro.shaded.com.google.common.collect.Lists;

public class App {
	public static void main(String[] args) throws UnknownHostException {
		Properties config = new Properties();
//		config.put("client.id", InetAddress.getLocalHost().getHostName());
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "zrr");
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.01:9092");
		config.put("schema.registry.url", "http://localhost:8081");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");  
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer" );
		
		BasicConsumeLoop loop = new BasicConsumeLoop( config, Lists.newArrayList( "test-accounts" ) );
		loop.run();
		
	}
}
