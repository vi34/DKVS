package vi34.com.events;

import vi34.com.Connection;
import vi34.com.Operation;

/**
 * Created by vi34 on 12/06/16.
 */
public class Request implements Event {
    public static final String TYPE = "Request";
    public Operation op;
    public String[] args;
    public int clientId;
    public int requestNumber;
    private String s;
    private Connection connection;

    public Request() {
    }

    public Request(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        String[] tmp = s.split(",");
        String[] req = tmp[1].split(" ");
        op =  Operation.valueOf(req[0].toUpperCase());
        args = new String[req.length - 1];
        System.arraycopy(req, 1, args, 0, req.length - 1);
        clientId = Integer.valueOf(tmp[2]);
        requestNumber = Integer.valueOf(tmp[3]);

    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return s;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
