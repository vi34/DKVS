package com.vi34;

import com.vi34.events.*;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by vi34 on 12/06/16.
 */
public class Connection {
    volatile Socket socket;
    Replica replica;
    public BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private final ReaderThread readerThread = new ReaderThread();
    private final WriterThread writerThread = new WriterThread();
    InetSocketAddress socketAddress;

    Connection(Socket socket, Replica replica) {
        this.socket = socket;
        String host = socket.getInetAddress().getHostName();
        int port = socket.getPort();
        socketAddress = new InetSocketAddress(host, port);
        this.replica = replica;
        try {
            writerThread.os = socket.getOutputStream();
            readerThread.is = socket.getInputStream();
        } catch (IOException e) {
            try {
                socket.close();
            } catch (IOException e1) {
            }
        }
        writerThread.start();
        readerThread.start();
    }

    Connection(String host, int port, Replica replica) {
        this.replica = replica;
        socketAddress = new InetSocketAddress(host, port);
        socket = new Socket();
        writerThread.start();
        readerThread.start();
    }

    private synchronized void reconnect() throws InterruptedException {
        while (socket.isClosed() || !socket.isConnected()) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            //System.out.println("R"+replica.replicaNumber + "try to recconnect to " + socketAddress.getPort());
            socket = new Socket();
            try {
                Thread.sleep(1000);
                socket.connect(socketAddress);
            } catch (IOException e) {
            }
        }
    }

    private class WriterThread extends Thread {
        OutputStream os;

        @Override
        public void run() {
            try {
                outer: while (!Thread.interrupted()) {
                    try (PrintWriter writer = new PrintWriter(socket.getOutputStream())) {
                        while (true) {
                            String response = messageQueue.take();
                            writer.println(response);
                            if (writer.checkError() || socket.isClosed()) {
                                reconnect();
                                continue outer;
                            }
                            writer.flush();
                        }
                    } catch (IOException e) {
                        reconnect();
                    }
                }
            } catch (InterruptedException e) {
                return;
            }
            //System.out.println("Writer " +replica.replicaNumber + " p: " + socketAddress.getPort() + "stopped" );
        }
    }

    private class ReaderThread extends Thread {
        InputStream is;

        @Override
        public void run() {
            try {
                outer: while (!Thread.interrupted()) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                        String line;
                        while(true) {
                            if ((line = reader.readLine()) == null) {
                                if (Thread.interrupted())
                                    return;
                                reconnect();
                                continue outer;
                            }
                            Event event = buildEvent(line);
                            replica.eventQueue.add(event);
                        }
                    } catch (IOException e) {
                        reconnect();
                    }
                }
            } catch (InterruptedException e) {}
            //System.out.println("REader " +replica.replicaNumber + " p: " + socketAddress.getPort() + "stopped" );
        }

        private Event buildEvent(String s) {
            String[] args = s.split(",");
            String type = args[0];
            switch (type) {
                case Request.TYPE: return new Request(s, Connection.this);
                case Prepare.TYPE: return new Prepare(s, Connection.this);
                case PrepareOK.TYPE: return new PrepareOK(s, Connection.this);
                case Commit.TYPE: return new Commit(s, Connection.this);
                case Reply.TYPE: return new Reply(s, Connection.this);
                case StartViewChange.TYPE: return new StartViewChange(s, Connection.this);
                case DoViewChange.TYPE: return new DoViewChange(s, Connection.this);
                case StartView.TYPE: return new StartView(s, Connection.this);
                case Recovery.TYPE: return new Recovery(s, Connection.this);
                case RecoveryResponse.TYPE: return new RecoveryResponse(s, Connection.this);
                default:
                    System.err.println("Unknown message type: " + s);
            }
            return null;
        }
    }

    public void close() {
        writerThread.interrupt();
        readerThread.interrupt();
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
