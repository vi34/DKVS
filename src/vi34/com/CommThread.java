package vi34.com;

import vi34.com.events.Event;
import vi34.com.events.Request;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by vi34 on 12/06/16.
 */
public class CommThread extends Thread {
    Socket socket;
    Replica replica;

    CommThread() {

    }

    CommThread(Socket socket, Replica replica) {
        this.socket = socket;
        this.replica = replica;
    }

    @Override
    public void run() {
        try ( PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
              BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Event event = buildEvent(line);
                System.out.println("Replica "+ (replica.index+1) + ": get \" " + line+"\"");
                replica.eventQueue.add(event);
                writer.println("Hi");
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
        String[] args = s.split(" ");
        String type = args[0];
        switch (type) {
            case Request.TYPE: return new Request(args[1], s.substring(type.length() + args[1].length()));
            default:
                System.out.println(("Unknown message type"));
        }
        return null;
    }
}
