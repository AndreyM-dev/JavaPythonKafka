package com.epam.producer.controller;

import com.epam.producer.service.KafkaProducerService;
import com.epam.producer.service.MessageProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MessagesController {

    @Autowired
    private KafkaProducerService kafkaProducer;

    @GetMapping(path = "/message")
    public void pushMessage() {
        kafkaProducer.sendMessage();
    }
    @GetMapping(path = "/messages/{num}")
    public void pushMessages(@PathVariable int num) {
        kafkaProducer.sendListMessages(num);
    }

}
