package org.study;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerService implements Closeable {
    private final KafkaConsumer<String, String> consumer;
    private final ConsumerFunction parse;

    public ConsumerService(String topic, ConsumerFunction parse, String group) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(group));
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void run(){
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()){
                for (var record : records){
                    parse.consumer(record);
                }
            }
        }
    }

    private static Properties properties(String group) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
