package com.epam.producer.service;

import com.epam.producer.model.Message;

import java.util.List;

public interface MessageProducer {
    Message getMessage();
    List<Message> getListMessages(int num);
}
