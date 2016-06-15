package com.vi34;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by vi34 on 11/06/16.
 */
public class Client {

    static Proxy proxy;

    public static void main(String[] args) {
        try (Proxy p = new Proxy()){
            proxy = p;
            if (args.length == 1 && args[0].equals("auto")) {
                autoClient();
            } else {
                consoleClient();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void consoleClient() {
        proxy.directConnect("lead");
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String cmd = scanner.nextLine();
            if (cmd.startsWith("get") || cmd.startsWith("set") || cmd.startsWith("ping") || cmd.startsWith("delete") ) {
                System.out.println(proxy.sendRequest(cmd));
            } else if (cmd.startsWith("node")) {
                System.out.println(proxy.directConnect(cmd.split(" ")[1]));
            } else if (cmd.startsWith("exit")) {
                return;
            } else {
                System.out.println("unknown command");
            }
        }
    }

    private static void autoClient() {
        Server.cleanLogs();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        int n = 3;
        for (int i = 0; i < n; ++i) {
            Server.startNode(i + 1);
        }
        try {
           Thread.sleep(100);
            proxy.directConnect("lead");
             /*
            makeReq("get x");
            makeReq("ping");
            makeReq("set x 10");
            makeReq("get x");
            makeReq("set y 20");
            Server.stopNode(1);
            Thread.sleep(2000);
            makeReq("set z 50");
            Thread.sleep(1000);
            Server.startNode(1);
            makeReq("delete x");
            makeReq("get x");
            makeReq("get y");
            makeReq("get z");
            Thread.sleep(7000);*/
            Thread.sleep(3000);
            Server.stopNode(1);
            System.out.println("stop 1");
            Thread.sleep(8000);
            Server.startNode(1);
            System.out.println("start 1");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            for (int i = 0; i < n; ++i) {
                Server.stopNode(i + 1);
            }
        }
    }

    private static void makeReq(String req) {
        System.out.println("\n> " + req);
        System.out.println("\n< " + proxy.sendRequest(req));

    }

}
