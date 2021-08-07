package org.qortal.chat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.qortal.settings.Settings;
import org.qortal.utils.NTP;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ChatRateLimiter extends Thread {

    private static ChatRateLimiter instance;
    private volatile boolean isStopping = false;

    // Maintain a list of recent chat timestamps for each address, to save having to query the database every time
    private Map<String, List<Long>> recentMessages = new ConcurrentHashMap<String, List<Long>>();

    public ChatRateLimiter() {

    }

    public static synchronized ChatRateLimiter getInstance() {
        if (instance == null) {
            instance = new ChatRateLimiter();
            instance.start();
        }

        return instance;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Chat Rate Limiter");

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


    public void addMessage(String address, long timestamp) {
        // Add timestamp to array for address
        List<Long> timestamps = new ArrayList<Long>();
        if (this.recentMessages.containsKey(address)) {
            timestamps = this.recentMessages.get(address);
        }
        if (!timestamps.contains(timestamp)) {
            timestamps.add(timestamp);
        }
        this.recentMessages.put(address, timestamps);
    }

    public boolean isAddressAboveRateLimit(String address) {
        int chatRateLimitCount = Settings.getInstance().getChatRateLimitCount();
        long chatRateLimitMilliseconds = Settings.getInstance().getChatRateLimitSeconds() * 1000L;
        long now = NTP.getTime();

        if (this.recentMessages.containsKey(address)) {
            int messageCount = 0;
            boolean timestampsUpdated = false;

            List<Long> timestamps = this.recentMessages.get(address);
            Iterator iterator = timestamps.iterator();
            while (iterator.hasNext()) {
                Long timestamp = (Long) iterator.next();
                if (timestamp >= now - chatRateLimitMilliseconds) {
                    // Message within tracked range
                    messageCount++;
                }
                else {
                    // Older than tracked range - delete to reduce memory consumption
                    iterator.remove();
                    timestampsUpdated = true;
                }
            }
            // Update timestamps for address
            if (timestampsUpdated) {
                if (timestamps.size() > 0) {
                    this.recentMessages.put(address, timestamps);
                }
                else {
                    this.recentMessages.remove(address);
                }
            }

            if (messageCount >= chatRateLimitCount) {
                // Rate limit has been hit
                return true;
            }
        }

        return false;
    }


    private void cleanup() {

        // Cleanup map of addresses and timestamps
        this.deleteOldTimestampsForAllAddresses();
    }

    private void deleteOldTimestampsForAddress(String address) {
        if (address == null) {
            return;
        }

        long chatRateLimitMilliseconds = Settings.getInstance().getChatRateLimitSeconds() * 1000L;
        long now = NTP.getTime();

        if (this.recentMessages.containsKey(address)) {
            boolean timestampsUpdated = false;

            List<Long> timestamps = recentMessages.get(address);
            Iterator iterator = timestamps.iterator();
            while (iterator.hasNext()) {
                Long timestamp = (Long) iterator.next();
                if (timestamp < now - chatRateLimitMilliseconds) {
                    // Older than tracked interval
                    iterator.remove();
                    timestampsUpdated = true;
                }
            }
            // Update timestamps for address
            if (timestampsUpdated) {
                if (timestamps.size() > 0) {
                    this.recentMessages.put(address, timestamps);
                } else {
                    this.recentMessages.remove(address);
                }
            }
        }
    }

    private void deleteOldTimestampsForAllAddresses() {
        for (Map.Entry<String, List<Long>> entry : this.recentMessages.entrySet()) {
            this.deleteOldTimestampsForAddress(entry.getKey());
        }
    }

}
