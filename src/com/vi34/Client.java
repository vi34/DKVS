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

    static void consoleClient() {
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

    static void autoClient() {
        makeReq("get x");
        makeReq("ping");
        makeReq("set x 10");
        makeReq("get x");
    }

    private static void makeReq(String req) {
        System.out.println("> " + req);
        System.out.println(proxy.sendRequest(req));

    }

}
