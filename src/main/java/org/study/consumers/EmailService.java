package org.study.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.study.ConsumerService;

public class EmailService {

    public static void main(String[] args) {
        EmailService emailService = new EmailService();
        try(ConsumerService consumerService = new ConsumerService("NEW_TRANSFER_SEND_EMAIL", emailService::parse, EmailService.class.getSimpleName())) {
            consumerService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Consuming Email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
