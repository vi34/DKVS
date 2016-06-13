package vi34.com;

import vi34.com.events.Event;

import java.io.*;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by vi34 on 11/06/16.
 */
public class Replica implements Runnable{
    int index;
    int port;
    int n;
    Thread[] commThreads;
    Properties config;
    BlockingQueue<Event> eventQueue;

    Replica(int idx) {
        index = idx;
        config = new Properties();
        try (FileInputStream inputStream = new FileInputStream("dkvs.properties")) {
            config.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        n = Integer.valueOf(config.getProperty("n"));
        port = Integer.valueOf(config.getProperty("node." + (index + 1)).split(":")[1]);
        eventQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        startCommThreads();
        while (!Thread.interrupted()) {
            try {
                Event event = eventQueue.take();
                System.out.println("Replica process: " + event.getType());
            } catch (InterruptedException e) {
                e.printStackTrace();

            }
        }
    }



    private void startCommThreads() {
        commThreads = new Thread[n + 1];
        commThreads[0] = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)){
                while (!Thread.interrupted()) {
                    new CommThread(serverSocket.accept(), this).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        commThreads[0].start();
    }
}
