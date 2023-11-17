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
public class QueueService {

    private final ServiceBusSenderClient senderClient;
    private final ServiceBusReceiverClient receiverClient;

    public QueueService() {
        String connectionString = "Endpoint=sb://asb-queue-01.servicebus.windows.net/;SharedAccessKeyName=ConnectionString;SharedAccessKey=7nyOFTJuAnLU39RNj+jelgJTJNj4td7ra+ASbCSK6jE=;EntityPath=test-queue";
        String queueName = "test-queue";

        this.senderClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();

        this.receiverClient = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .receiver()
                .queueName(queueName)
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

    //get message from service bus
    public List<String> getMessages() {
        List<String> messages = new ArrayList<>();
        Iterable<ServiceBusReceivedMessage> receivedMessages;

        do {
            receivedMessages = receiverClient.receiveMessages(100);

            for (ServiceBusReceivedMessage message : receivedMessages) {
                messages.add(message.getBody().toString());
            }
        } while (!receivedMessages.iterator().hasNext());

        return messages;
    }
}

