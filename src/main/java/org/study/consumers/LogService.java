package org.study.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.study.ConsumerService;

public class LogService {

    public static void main(String[] args) {
        LogService logService = new LogService();
        try(ConsumerService consumerService = new ConsumerService("NEW_TRANSFER.*", logService::parse, LogService.class.getSimpleName())) {
            consumerService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Consuming Log");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
