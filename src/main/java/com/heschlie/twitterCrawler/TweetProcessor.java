package com.heschlie.twitterCrawler;

import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sschlie on 8/20/2014.
 */
public class TweetProcessor implements Runnable {
    private LinkedBlockingQueue<Status> readQueue;
    private LinkedBlockingQueue<Status> postQueue;
    private ConcurrentHashMap<String, AtomicInteger> userSightings;
    private ConcurrentHashMap<String, AtomicInteger> popularhastags;

    public TweetProcessor(LinkedBlockingQueue<Status> readQueue,
                          LinkedBlockingQueue<Status> postQueue,
                          ConcurrentHashMap<String, AtomicInteger> userSightings,
                          ConcurrentHashMap<String, AtomicInteger> popularHashtags) {
        this.userSightings = userSightings;
        this.readQueue = readQueue;
        this.postQueue = postQueue;
        this.popularhastags = popularHashtags;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Status status = readQueue.take();
                processStatus(status);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void processStatus(Status status) throws InterruptedException {
        //Get interesting info
        String userName = status.getUser().getScreenName();
        String message = status.getText();
        HashtagEntity[] hashtags = status.getHashtagEntities();

        //Add and iterate the users
        userSightings.putIfAbsent(userName, new AtomicInteger(0));
        userSightings.get(userName).incrementAndGet();

        //Add hashtags to the list
        for (HashtagEntity tag : hashtags) {
            String text = tag.getText();
            popularhastags.putIfAbsent(text, new AtomicInteger(0));
            popularhastags.get(text).incrementAndGet();
        }

        //Check for keywords and if so add to retweet queue
        if (checkWords(message)) {
            postQueue.put(status);
        }
    }

    /**
    Checks the given message from the a status for keywords
     */
    private boolean checkWords(String message) {
        boolean containsKeywords = false;
        if (message.contains("iridiumflaregames")) {
            containsKeywords = true;
        }
        return containsKeywords;
    }
}
