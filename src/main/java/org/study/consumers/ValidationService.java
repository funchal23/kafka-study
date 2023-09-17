package org.study.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.study.ConsumerService;

public class ValidationService {

    public static void main(String[] args) {
        ValidationService validationService = new ValidationService();
        try (ConsumerService consumerService = new ConsumerService("NEW_TRANSFER_VALIDATION", validationService::parse, ValidationService.class.getSimpleName())) {
            consumerService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Consuming Validation");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
