package com.epam.producer.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerServiceImpl implements KafkaProducerService{
    @Value("${tpd.topic-name}")
    private String topic;

    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;
    @Autowired
    private MessageProducer messageProducer;
    @Override
    public void sendMessage() {
        kafkaTemplate.send(topic, messageProducer.getMessage());
    }
    @Override
    public void sendListMessages(int num) {
        kafkaTemplate.send(topic, messageProducer.getListMessages(num));
    }
}
