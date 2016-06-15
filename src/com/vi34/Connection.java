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
    Socket socket;
    Replica replica;
    public BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    private final ReaderThread readerThread = new ReaderThread();
    private final WriterThread writerThread = new WriterThread();
    InetSocketAddress socketAddress;
    volatile boolean connected = false;

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

    private class WriterThread extends Thread {
        OutputStream os;
        boolean close = false;

        @Override
        public void run() {
            try {
                if (os == null) {
                    reconnect();
                }
            } catch (InterruptedException e) {
                return;
            }
                outer: while (!Thread.interrupted()) {
                    try (PrintWriter writer = new PrintWriter(os)) {
                        while (connected) {
                            try {
                                String response = messageQueue.take();
                                writer.println(response);
                                if (writer.checkError() || !connected) {
                                    reconnect();
                                    continue outer;
                                }
                                writer.flush();
                            } catch (InterruptedException e) {
                                if (close) {
                                    return;
                                } else {
                                    try {
                                        reconnect();
                                    } catch (InterruptedException e1) {
                                        return;
                                    }
                                    continue outer;
                                }
                            }
                        }
                    }
                }
            System.out.println("Writer " +replica.replicaNumber + " p: " + socketAddress.getPort() + "stopped" );
        }

        private void reconnect() throws InterruptedException {
            while (!socket.isConnected()) {
                System.out.println("R"+replica.replicaNumber + "try to recconnect to " + socketAddress.getPort());
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                if (socket.isClosed()) {
                    socket = new Socket();
                }
                try {
                    Thread.sleep(1000);
                    socket.connect(socketAddress);
                    os = socket.getOutputStream();
                    synchronized (readerThread) {
                        readerThread.is = socket.getInputStream();
                        connected = true;
                        readerThread.notify();
                    }
                } catch (IOException e) {

                }
            }
        }
    }

    private class ReaderThread extends Thread {
        InputStream is;

        @Override
        public void run() {
            try {
                if (is == null) {
                    reconnect();
                }
                outer: while (!Thread.interrupted()) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                        String line;
                        while(true) {
                            if ((line = reader.readLine()) == null) {
                                if (Thread.interrupted())
                                    return;
                                connected = false;
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
            } catch (InterruptedException e) {
            }  finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("REader " +replica.replicaNumber + " p: " + socketAddress.getPort() + "stopped" );
            }
        }

        private synchronized void reconnect() throws InterruptedException {
            System.out.println("REader " +replica.replicaNumber + " p: " + socketAddress.getPort() + "waiting" );
            writerThread.interrupt();
            while(!connected) {
                wait();
            }
            System.out.println("REader " +replica.replicaNumber + " p: " + socketAddress.getPort() + "UNwaiting" );
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
        writerThread.close = true;
        writerThread.interrupt();
        readerThread.interrupt();
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
