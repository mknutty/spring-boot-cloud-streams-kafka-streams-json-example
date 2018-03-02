package com.mna.demo;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.core.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.mna.demo.KafkaBootStreamsConfiguration.Test;

import lombok.AllArgsConstructor;

/*
 * This will get the current value in the Store by key. Pass in the value that is the key field in the JSON
 */
@RestController
@AllArgsConstructor
public class TestController {
	private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
	
	@GetMapping("test")
	public Test list(String key) {
		ReadOnlyKeyValueStore<String, Test> keyValueStore = streamsBuilderFactoryBean.getKafkaStreams().store("streams-json-store", QueryableStoreTypes.keyValueStore());
		return  keyValueStore.get(key);
	}
}
