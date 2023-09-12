package org.study;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TransferMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var email = "email@email.com";
        var value = "idReceiver:1,idConsumer:1,value:2000";
        var transferSendEmail = new ProducerRecord<>("NEW_TRANSFER_SEND_EMAIL", email, email);
        var transferValidation = new ProducerRecord<>("NEW_TRANSFER_VALIDATION", value, value);
        producer.send(transferSendEmail, getCallback()).get();
        producer.send(transferValidation, getCallback()).get();
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
}
