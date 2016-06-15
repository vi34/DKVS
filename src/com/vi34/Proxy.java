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
    int timeout;
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
        timeout = Integer.valueOf(config.getProperty("timeout"));
        n = Integer.valueOf(config.getProperty("n"));
    }

    public String directConnect(String to) {
        try {
            if (to.equals("lead")) {
                return connect(curLeader()).response;
            } else {
                return connect(Integer.valueOf(to)).response;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Reply connect(int to) throws IOException {
        String[] address = config.getProperty("node." + to).split(":");
        if (writer != null) {
            writer.close();
            reader.close();
        }
        if (socket == null || socket.isClosed()) {
            socket = new Socket();
            socket.setSoTimeout(timeout);
        }

        socket.connect(new InetSocketAddress(address[0],Integer.valueOf(address[1])));
        writer = new PrintWriter(socket.getOutputStream(), true);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        return new Reply(reader.readLine(), null);
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
            String line = reader.readLine();
            if (line == null) {
                throw new IOException();
            }
            Reply reply = new Reply(line, null);

            if (reply.view > viewNumber) {
                viewNumber = reply.view;
            }
            return reply.response.toString();
        } catch (IOException e) {
            System.out.println("lost connection, trying to reconnect");
            int cnt = 0;
            while (cnt < n) {
                cnt++;
                viewNumber++;
                try {
                    Thread.sleep(timeout); // wait for view_change complete
                    Reply reply = connect(curLeader());
                    if (reply.response.equals("ACCEPT")) {
                        if (reply.view != viewNumber) {
                            viewNumber = reply.view;
                            connect(curLeader());
                        }
                        return sendRequest(request);
                    }
                } catch (IOException e1) {
                    System.out.println("reconnect to " + curLeader() + " failed");
                } catch (InterruptedException e1) {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            reader.close();
        }
        socket.close();
    }
}
