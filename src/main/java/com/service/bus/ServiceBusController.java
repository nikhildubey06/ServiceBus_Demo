package com.service.bus;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class ServiceBusController {

    @Autowired
    private QueueService queueService;

    @Autowired
    private TopicService topicService;

    @PostMapping("/sendQueueSingleMessage")
    public void sendQueueSingleMessage(@RequestBody String messageText) {
        queueService.sendSingleMessage(messageText);
    }

    @PostMapping("/sendQueueBatchMessages")
    public void sendQueueBatchMessages(@RequestBody List<String> messages) {
        queueService.sendBatchMessages(messages);
    }

    @PostMapping("/sendTopicSingleMessage")
    public void sendTopicSingleMessage(@RequestBody String messageText) {
        topicService.sendSingleMessage(messageText);
    }

    @PostMapping("/sendTopicBatchMessages")
    public void sendTopicBatchMessages(@RequestBody List<String> messages) {
        topicService.sendBatchMessages(messages);
    }

    @GetMapping("/getQueueMessages")
    public List<String> getQueueMessages() {
        return queueService.getMessages();
    }

    @GetMapping("/getTopicMessages")
    public List<String> getTopicMessages() {
        return topicService.getMessages();
    }
}

//subscription-01 (asbtopictest0101/asb-topic-01/subscription-01) | Service Bus Explorer
//test-queue (asb-queue-01/test-queue) | Service Bus Explorer