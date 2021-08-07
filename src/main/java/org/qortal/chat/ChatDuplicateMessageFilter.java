package org.qortal.chat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChatDuplicateMessageFilter {

    private static ChatDuplicateMessageFilter instance;
    private volatile boolean isStopping = false;

    private static final int numberOfUniqueMessagesToMonitor = 3;

    // Maintain a short list of recent chat messages for each address, to save having to query the database every time
    private Map<String, List<String>> recentMessages = new ConcurrentHashMap<>();

    public ChatDuplicateMessageFilter() {

    }

    public static synchronized ChatDuplicateMessageFilter getInstance() {
        if (instance == null) {
            instance = new ChatDuplicateMessageFilter();
        }

        return instance;
    }

    public boolean isDuplicateMessage(String address, String message) {
        boolean isDuplicateMessage;
        boolean messagesUpdated = false;

        // Add timestamp to array for address
        List<String> messages = new ArrayList<>();
        if (this.recentMessages.containsKey(address)) {
            messages = this.recentMessages.get(address);
        }

        if (!messages.contains(message)) {
            messages.add(message);
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

}
