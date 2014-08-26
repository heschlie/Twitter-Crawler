package com.heschlie.twitterCrawler;

import org.apache.logging.log4j.*;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sschlie on 8/20/2014.
 */
public class Crawler implements Runnable, StatusListener{
    private TwitterStream stream;
    private LinkedBlockingQueue<Status> readQueue;

    private org.apache.logging.log4j.Logger log;

    public Crawler(LinkedBlockingQueue<Status> readQueue) {
        this.readQueue = readQueue;
        ConfigurationBuilder cb = createBuilder();
        TwitterStreamFactory tsf = new TwitterStreamFactory(cb.build());
        stream = tsf.getInstance();
        stream.addListener(this);

        log = LogManager.getLogger();
    }

    private ConfigurationBuilder createBuilder() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("iwikDfljpyBHyjOKcJ2DR3VUO")
                .setOAuthConsumerSecret("qeG9R35Gc2SOndd5Zjzhkd590BweI06bQdmj5Q3osyHHFUoxS9")
                .setOAuthAccessToken("2753139690-aKLgjJSTevUuylpdtUHL0qwkg0ZJOajDZZWGbDH")
                .setOAuthAccessTokenSecret("FnlQyqjFrQRYY41azsuZNUgFEdRc615jBELtQh7jJXEcC");
        return cb;
    }

    @Override
    public void run() {
        stream.sample();
    }

    @Override
    public void onStatus(Status status) {
        if (status.getUser().getName().equals("schlieBot")) {
            return;
        }

        try {
            readQueue.put(status);
        } catch (InterruptedException e) {
            log.error("Failed to append to readQueue");
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }
}
