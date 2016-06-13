package vi34.com;

import vi34.com.events.*;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
        this.socket = socket; // todo close socket
        this.replica = replica;
        try {
            new ReaderThread(socket.getInputStream()).start();
            new WriterThread(socket.getOutputStream()).start();
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
                    writer.flush();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
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
                e.printStackTrace();
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
