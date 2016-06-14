package com.vi34;

import java.util.Scanner;

/**
 * Created by vi34 on 14/06/16.
 */
public class Server {
    static int n = 3;
    static Thread[] replicas;
    public static void main(String[] args) {
        replicas = new Thread[n];
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
                    stopNode(scanner.nextInt() - 1);
                    break;
                case "exit":
                    scanner.close();
                    return;

                default:
                    System.out.println("unknown command");
            }
        }
    }

    private static void startNode(int i) {
        if (replicas[i - 1] == null) {
            replicas[i - 1] = new Thread(new Replica(i));
            replicas[i - 1].start();
        }
    }

    private static void stopNode(int i) {
        if (replicas[i] != null) {
            replicas[i].interrupt();
            replicas[i] = null;
        }
    }
}
