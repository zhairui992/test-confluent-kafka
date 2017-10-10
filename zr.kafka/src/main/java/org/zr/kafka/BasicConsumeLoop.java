package org.zr.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicConsumeLoop implements Runnable {
	
	private final Logger log = LoggerFactory.getLogger( BasicConsumeLoop.class );
	
	private final KafkaConsumer<Object, Object> consumer;
	
	private final List<String> topics;
	
	private final CountDownLatch shutdownLatch;
	
	public BasicConsumeLoop( Properties config, List<String> tipics ) {
		this.consumer = new KafkaConsumer<>( config );
		this.topics = tipics;
	    this.shutdownLatch = new CountDownLatch(1);
	}
	
	public void process( ConsumerRecord<Object, Object> record ){
		log.info( "test" );
		System.out.println( String.valueOf( record.value() ) );
	}
	
	@Override
	public void run() {
		try{
			consumer.subscribe( topics );
			while( true ) {
				ConsumerRecords<Object, Object> records = consumer.poll( Long.MAX_VALUE );
				records.forEach( record -> process( record ) );
				consumer.commitAsync();
			}
		} catch( WakeupException e ) {
			
		} catch ( Exception e ) {
			log.error( "Unexpected error", e );
		} finally {
			consumer.close();
			shutdownLatch.countDown();
		}
	}
	
	public void shutdown() throws InterruptedException {
		consumer.wakeup();
	    shutdownLatch.await();
	}
}