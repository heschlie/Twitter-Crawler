package com.heschlie.twitterCrawler;

import twitter4j.Status;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sschlie on 8/20/2014.
 */
public class TwitterCrawler {
    private LinkedBlockingQueue<Status> readTweets;
    private LinkedBlockingQueue<Status> postTweets;

    private ConcurrentHashMap<String, AtomicInteger> noisyUsers;

    private int NUMTHREADS = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) {
        TwitterCrawler twitterCrawler = new TwitterCrawler();
        twitterCrawler.init();
    }

    private void init() {
        readTweets = new LinkedBlockingQueue<Status>();
        postTweets = new LinkedBlockingQueue<Status>();

        noisyUsers = new ConcurrentHashMap<String, AtomicInteger>();

        new Thread(new Crawler(readTweets)).start();
        NUMTHREADS--;

        for (int i = 0; i < NUMTHREADS; i++) {
            new Thread(new TweetProcessor(readTweets, postTweets, noisyUsers)).start();
        }
    }

}
