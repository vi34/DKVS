package com.vi34;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

/**
 * Created by vi34 on 14/06/16.
 */
public class Server {
    static int n = 3;
    static Thread[] replicas = new Thread[n];
    public static void main(String[] args) {
        cleanLogs();
        if (args.length == 0) {
            for (int i = 0; i < n; ++i) {
                startNode(i + 1);
            }
        } else {
            for (String arg : args) {
                int ind = Integer.valueOf(arg);
                startNode(ind);
            }
        }
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("> ");
            switch (scanner.next()) {
                case "start":
                    startNode(scanner.nextInt());
                    break;
                case "stop":
                    stopNode(scanner.nextInt());
                    break;
                case "exit":
                    scanner.close();
                    for (int i = 0; i < n; ++i)
                        stopNode(i + 1);
                    return;

                default:
                    System.out.println("unknown command");
            }
        }
    }

    public static void cleanLogs() {
        for (int i = 0; i < n; ++i) {
            new File("dkvs_"+ (i+1)+".log").delete();
        }
    }

    public static void startNode(int i) {
        if (replicas[i - 1] == null) {
            replicas[i - 1] = new Thread(new Replica(i));
            replicas[i - 1].start();
        }
    }

    public static void stopNode(int i) {
        if (replicas[i - 1] != null) {
            replicas[i - 1].interrupt();
            replicas[i - 1] = null;
        }
    }
}
