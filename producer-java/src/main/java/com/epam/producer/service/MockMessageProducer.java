package com.epam.producer.service;

import com.epam.producer.model.Message;
import com.github.javafaker.Faker;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MockMessageProducer implements MessageProducer{

    @Override
    public Message getMessage() {
        return getMockMessage();
    }

    private Message getMockMessage() {
        Faker faker = new Faker();
       return Message.builder()
               .id(faker.random().nextLong())
               .message(faker.backToTheFuture().quote())
               .build();
    }

    @Override
    public List<Message> getListMessages(int num) {
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            messages.add(getMessage());
        }
        return messages;
    }
}
