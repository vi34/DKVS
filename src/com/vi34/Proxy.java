package com.vi34;

import com.vi34.events.Reply;
import com.vi34.events.Request;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by vi34 on 11/06/16.
 */
public class Proxy implements AutoCloseable {
    int viewNumber;
    int n;
    int requestNumber;
    private static int clientId;

    Socket socket;
    PrintWriter writer;
    BufferedReader reader;

    Properties config;
    Proxy () {
        clientId++;
        config = new Properties();
        try (FileInputStream inputStream = new FileInputStream("dkvs.properties")) {
            config.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        n = Integer.valueOf(config.getProperty("n"));
        socket = new Socket();
        try {
            connect(curLeader());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String directConnect(String to) {
        try {
            if (to.equals("lead")) {
                return connect(curLeader());
            } else {
                return connect(Integer.valueOf(to));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String connect(int to) throws IOException {
        String[] address = config.getProperty("node." + to).split(":");
        if (writer != null) {
            writer.close();
            reader.close();
        }
        if (socket.isClosed()) {
            socket = new Socket();
        }
        socket.connect(new InetSocketAddress(address[0],Integer.valueOf(address[1])));
        writer = new PrintWriter(socket.getOutputStream(), true);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        return new Reply(reader.readLine(), null).response;
    }

    private int curLeader() {
        return viewNumber % n + 1;
    }

    public String sendRequest(String request) {
        try {
            String[] tmp = request.split(" ");
            Operation op = Operation.valueOf(tmp[0].toUpperCase());
            String[] args = new String[tmp.length - 1];
            System.arraycopy(tmp, 1, args, 0, tmp.length - 1);

            writer.println(new Request(op, args, clientId, requestNumber).toString());
            requestNumber++;
            Reply reply = new Reply(reader.readLine(), null);

            if (reply.view > viewNumber) {
                viewNumber = reply.view;
                // TODO: 14/06/16 clarify
            }
            return reply.response.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        reader.close();
        writer.close();
        socket.close();
    }
}
