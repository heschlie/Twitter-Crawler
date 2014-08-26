package com.heschlie.twitterCrawler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.Status;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by sschlie on 8/20/2014.
 * A Twitter crawler being used for fun!
 */
public class TwitterCrawler {
    private LinkedBlockingQueue<Status> readTweets;
    private LinkedBlockingQueue<Status> postTweets;

    private ConcurrentHashMap<String, AtomicInteger> noisyUsers;
    private ConcurrentHashMap<String, AtomicInteger> popularHashtags;

    private int NUMTHREADS = Runtime.getRuntime().availableProcessors();

    private Logger log;

    public static void main(String[] args) {
        TwitterCrawler twitterCrawler = new TwitterCrawler();
        twitterCrawler.init();
    }

    private void init() {
        log = LogManager.getLogger();
        readTweets = new LinkedBlockingQueue<Status>();
        postTweets = new LinkedBlockingQueue<Status>();

        noisyUsers = new ConcurrentHashMap<String, AtomicInteger>();
        popularHashtags = new ConcurrentHashMap<String, AtomicInteger>();

        readFiles();

        new Thread(new Crawler(readTweets)).start();
        NUMTHREADS--;

        for (int i = 0; i < NUMTHREADS; i++) {
            new Thread(new TweetProcessor(readTweets, postTweets, noisyUsers, popularHashtags)).start();
        }

        //Save the Maps to file every 5 minutes in case of crash
        Timer timer = new Timer();
//        timer.scheduleAtFixedRate(new MapWriter(), 300000L, 300000L);
        timer.scheduleAtFixedRate(new MapWriter(), 5000L, 5000L);
    }

    @SuppressWarnings("unchecked")
    private void readFiles() {
        Kryo kryo = new Kryo();
        kryo.register(ConcurrentHashMap.class);
        MapSerializer map = new MapSerializer();
        map.setKeyClass(String.class, new DefaultSerializers.StringSerializer());
        map.setValueClass(AtomicInteger.class, new FieldSerializer(kryo, AtomicInteger.class));

        File users = new File("userMap.bin");
        if (users.exists()) {
            try {
                Input in = new Input(new FileInputStream(users));
                noisyUsers = kryo.readObject(in, ConcurrentHashMap.class);
            } catch (FileNotFoundException e) {
                log.warn(e.getMessage());
            }
        }

        File hashTags = new File("hashTags.bin");
        if (hashTags.exists()) {
            try {
                Input in = new Input(new FileInputStream(hashTags));
                popularHashtags = kryo.readObject(in, ConcurrentHashMap.class);
            } catch (FileNotFoundException e) {
                log.warn(e.getMessage());
            }
        }
    }

    private class MapWriter extends TimerTask {
        private Kryo kryo;

        public MapWriter() {
            kryo = new Kryo();
            kryo.register(ConcurrentHashMap.class, new MapSerializer());
            log.info("MapWriter created");
        }

        @Override
        public void run() {
            try {
                writeUsers();
                writeHashTags();
            } catch (FileNotFoundException e) {
                log.error("Could not write file!");
                log.error(e.getMessage());
            }
        }

        private void writeUsers() throws FileNotFoundException {
            Output file = new Output(new FileOutputStream("userMap.bin"));
            kryo.writeObject(file, noisyUsers);
            file.flush();
            file.close();
            log.debug("File written to userMap.bin");
        }

        private void writeHashTags() throws FileNotFoundException {
            Output file = new Output(new FileOutputStream("hashTags.bin"));
            kryo.writeObject(file, popularHashtags);
            file.flush();
            file.close();
            log.debug("File written to hashTags.bin");
        }
    }
}
