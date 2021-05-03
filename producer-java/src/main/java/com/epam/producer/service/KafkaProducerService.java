package com.epam.producer.service;

public interface KafkaProducerService {
    void sendMessage();
    void sendListMessages(int num);
}
