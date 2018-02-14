package com.mna.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kstream.annotations.KStreamProcessor;
import org.springframework.messaging.handler.annotation.SendTo;

import lombok.Data;

@EnableBinding(KStreamProcessor.class)
public class KafkaBootStreamsConfiguration {
	
	@StreamListener("input")
	@SendTo("output")
	public KStream<String, Test> process(KStream<String, Test> input) {
		
		KTable<String, Test> combinedDocuments = input
				.map(new TestKeyValueMapper())
				.groupByKey()
				.reduce(new TestReducer(), Materialized.<String, Test, KeyValueStore<Bytes, byte[]>>as("streams-json-store"))
	    		;
	        
	        return combinedDocuments.toStream();
	        
    }
	
    public static class TestKeyValueMapper implements KeyValueMapper<String, Test, KeyValue<String, Test>> {

		@Override
		public KeyValue<String, Test> apply(String key, Test value) {
			return new KeyValue<String, KafkaBootStreamsConfiguration.Test>(value.getKey(), value);
		}
		
    }
    
    public static class TestReducer implements Reducer<Test> {

		@Override
		public Test apply(Test value1, Test value2) {
			value1.getWords().addAll(value2.getWords());
			return value1;
		}
    	
    }
    
    @Data
    public static class Test {
    	
    	private String key;
    	private List<String> words;
    	
    	public Test() {
    		words = new ArrayList<>();
    	}
    	
    }
    
}
