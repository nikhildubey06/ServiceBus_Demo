package com.service.bus;

import org.springframework.stereotype.Service;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;

import java.util.ArrayList;
import java.util.List;

@Service
public class TopicService {

    private final ServiceBusSenderClient senderClient;
    private final ServiceBusReceiverClient receiverClient;

    public TopicService() {
        String connectionString = "Endpoint=sb://asbtopictest0101.servicebus.windows.net/;SharedAccessKeyName=ConnectionString;SharedAccessKey=WxcOMWzT2Bkk2q5Co5dk5rMa3qVK74hxb+ASbIbdxgM=;EntityPath=asb-topic-01";
        String topicName = "asb-topic-01";
        String subscriptionName = "subscription-01";

        this.senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .topicName(topicName)
                .buildClient();

        this.receiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .topicName(topicName)
                .subscriptionName(subscriptionName)
                .buildClient();
    }
    
    // send single message
    public void sendSingleMessage(String messageText) {
        ServiceBusMessage message = new ServiceBusMessage(messageText);
        senderClient.sendMessage(message);
    }

    // send messages in batch
    public void sendBatchMessages(List<String> messages) {
        List<ServiceBusMessage> serviceBusMessages = new ArrayList<>();
        for (String messageText : messages) {
            serviceBusMessages.add(new ServiceBusMessage(messageText));
        }
        senderClient.sendMessages(serviceBusMessages);
    }

    // get messages from service bus
    public List<String> getMessages() {
        List<String> messages = new ArrayList<>();
        Iterable<ServiceBusReceivedMessage> receivedMessages;

        do {
            receivedMessages = receiverClient.receiveMessages(100); // Specify a large batch size

            for (ServiceBusReceivedMessage message : receivedMessages) {
                messages.add(message.getBody().toString());
            }
        } while (!receivedMessages.iterator().hasNext());

        return messages;
    }
}

