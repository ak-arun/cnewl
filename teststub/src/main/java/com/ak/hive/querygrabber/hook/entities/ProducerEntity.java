package com.ak.hive.querygrabber.hook.entities;

import org.apache.kafka.clients.producer.KafkaProducer;

public class ProducerEntity {

	public ProducerEntity(KafkaProducer<String, String> producer,
			long createTimeMillis) {
		super();
		this.producer = producer;
		this.createTimeMillis = createTimeMillis;
	}
	private KafkaProducer<String, String> producer;
	private long createTimeMillis;
	
	public KafkaProducer<String, String> getProducer() {
		return producer;
	}
	public void setProducer(KafkaProducer<String, String> producer) {
		this.producer = producer;
	}
	public long getCreateTimeMillis() {
		return createTimeMillis;
	}
	public void setCreateTimeMillis(long createTimeMillis) {
		this.createTimeMillis = createTimeMillis;
	}
	
}
