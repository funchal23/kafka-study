package org.study;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;

public class SendService implements Closeable {

    private final KafkaProducer<String, String> producer;

    public SendService(){
        this.producer = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, String value){
        var record = new ProducerRecord<>(topic, key, value);
        producer.send(record, getCallback());
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Success send message! TOPIC:" + data.topic() + " PARTITION:" + data.partition() + " OFFSET:" + data.offset());
        };
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
