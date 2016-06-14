package com.vi34;

import com.vi34.events.*;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by vi34 on 12/06/16.
 */
public class Connection {
    Socket socket;
    Replica replica;
    public BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>();
    ReaderThread readerThread;
    WriterThread writerThread;

    Connection(Socket socket, Replica replica) {
        this.socket = socket;
        this.replica = replica;
        try {
            readerThread = new ReaderThread(socket.getInputStream());
            writerThread = new WriterThread(socket.getOutputStream());
            writerThread.start();
            readerThread.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class WriterThread extends Thread {
        OutputStream os;

        public WriterThread(OutputStream os) {
            this.os = os;
        }

        @Override
        public void run() {
            try (PrintWriter writer = new PrintWriter(os)){
                while (!Thread.interrupted()) {
                    String response = messageQueue.take();
                    writer.println(response);
                    if (writer.checkError()) {
                        // TODO: 15/06/16 reconnect
                    }
                    writer.flush();
                }
            } catch (InterruptedException e) {
            }
        }
    }

    private class ReaderThread extends Thread {
        InputStream is;
        ReaderThread (InputStream is) {
            this.is = is;
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = reader.readLine()) != null && !Thread.interrupted()) {
                    Event event = buildEvent(line);
                    replica.eventQueue.add(event);
                }
            } catch (IOException e) {
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
