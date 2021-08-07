package org.qortal.chat;

import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ChatDuplicateMessageFilter extends Thread {

    public static class SimpleChatMessage {
        private long timestamp;
        private String message;

        public SimpleChatMessage(long timestamp, String message) {
            this.timestamp = timestamp;
            this.message = message;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        public String getMessage() {
            return this.message;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this)
                return true;

            if (!(other instanceof SimpleChatMessage))
                return false;

            SimpleChatMessage otherMessage = (SimpleChatMessage) other;

            return Objects.equals(this.getMessage(), otherMessage.getMessage());
        }
    }


    private static ChatDuplicateMessageFilter instance;
    private volatile boolean isStopping = false;

    private static final int numberOfUniqueMessagesToMonitor = 3; // Only hold the last 3 messages in memory
    private static final long maxMessageAge = 60 * 60 * 1000L; // Forget messages after 1 hour

    // Maintain a short list of recent chat messages for each address, to save having to query the database every time
    private Map<String, List<SimpleChatMessage>> recentMessages = new ConcurrentHashMap<>();

    public ChatDuplicateMessageFilter() {

    }

    public static synchronized ChatDuplicateMessageFilter getInstance() {
        if (instance == null) {
            instance = new ChatDuplicateMessageFilter();
            instance.start();
        }

        return instance;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Duplicate Chat Message Filter");

        try {
            while (!isStopping) {
                Thread.sleep(60000);

                this.cleanup();
            }
        } catch (InterruptedException e) {
            // Fall-through to exit thread...
        }
    }

    public void shutdown() {
        isStopping = true;
        this.interrupt();
    }


    public boolean isDuplicateMessage(String address, long timestamp, String message) {
        boolean isDuplicateMessage;
        boolean messagesUpdated = false;

        SimpleChatMessage thisMessage = new SimpleChatMessage(timestamp, message);

        // Add message to array for address
        List<SimpleChatMessage> messages = new ArrayList<>();
        if (this.recentMessages.containsKey(address)) {
            messages = this.recentMessages.get(address);
        }

        // Check for duplicate, and add if unique
        if (!messages.contains(thisMessage)) {
            messages.add(thisMessage);
            this.recentMessages.put(address, messages);
            messagesUpdated = true;
            isDuplicateMessage = false;
        }
        else {
            // Can't add message because it already exists
            isDuplicateMessage = true;
        }

        // Ensure we're not tracking more messages than intended
        while (messages.size() > numberOfUniqueMessagesToMonitor) {
            messages.remove(0);
            messagesUpdated = true;
        }

        // Ensure we're not holding on to messages for longer than a defined time period
        Iterator iterator = messages.iterator();
        long now = NTP.getTime();
        while (iterator.hasNext()) {
            SimpleChatMessage simpleChatMessage = (SimpleChatMessage) iterator.next();
            if (simpleChatMessage.getTimestamp() < now - maxMessageAge) {
                // Older than tracked interval
                iterator.remove();
                messagesUpdated = true;
            }
        }

        if (messagesUpdated) {
            if (messages.size() > 0) {
                this.recentMessages.put(address, messages);
            }
            else {
                this.recentMessages.remove(address);
            }
        }

        return isDuplicateMessage;
    }


    private void cleanup() {

        // Cleanup map of addresses and messages
        this.deleteOldMessagesForAllAddresses();
    }

    private void deleteOldMessagesForAddress(String address, long now) {
        if (address == null) {
            return;
        }

        if (this.recentMessages.containsKey(address)) {
            boolean messagesUpdated = false;

            List<SimpleChatMessage> messages = recentMessages.get(address);

            // Ensure we're not holding on to messages for longer than a defined time period
            Iterator iterator = messages.iterator();
            while (iterator.hasNext()) {
                SimpleChatMessage simpleChatMessage = (SimpleChatMessage) iterator.next();
                if (simpleChatMessage.getTimestamp() < now - maxMessageAge) {
                    // Older than tracked interval
                    iterator.remove();
                    messagesUpdated = true;
                }
            }

            // Update messages for address
            if (messagesUpdated) {
                if (messages.size() > 0) {
                    this.recentMessages.put(address, messages);
                }
                else {
                    this.recentMessages.remove(address);
                }
            }
        }
    }

    private void deleteOldMessagesForAllAddresses() {
        long now = NTP.getTime();
        for (Map.Entry<String, List<SimpleChatMessage>> entry : this.recentMessages.entrySet()) {
            this.deleteOldMessagesForAddress(entry.getKey(), now);
        }
    }

}
