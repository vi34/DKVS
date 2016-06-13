package vi34.com;

import vi34.com.events.Request;

import java.io.*;
import java.net.Socket;
import java.util.Properties;

/**
 * Created by vi34 on 11/06/16.
 */
public class Proxy {
    int viewNumber;
    int n;
    int requestNumber;
    int clientId;

    Properties config;
    Proxy () {
        config = new Properties();
        try (FileInputStream inputStream = new FileInputStream("dkvs.properties")) {
            config.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        n = Integer.valueOf(config.getProperty("n"));
    }

    public String sendRequest(String request) {
        int primary = viewNumber % n + 1;
        String[] address = config.getProperty("node." + primary).split(":");
        try (
                Socket socket = new Socket(address[0], Integer.valueOf(address[1]));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            writer.println(Request.TYPE + request);
            System.out.println("Proxy sent request to " + primary);
            return reader.readLine();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
