package org.study.publishers;

import org.study.SendService;

import java.util.UUID;

public class Transfer {

    public static void main(String[] args) {
        try(SendService sendService = new SendService()) {
            var key = UUID.randomUUID().toString();
            sendService.send("NEW_TRANSFER_SEND_EMAIL", key, "email@email.com");
            sendService.send("NEW_TRANSFER_VALIDATION", key, "idReceiver:1,idConsumer:1,value:2000");
        }
    }
}
