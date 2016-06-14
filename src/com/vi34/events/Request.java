package com.vi34.events;

import com.vi34.Connection;
import com.vi34.Operation;

/**
 * Created by vi34 on 12/06/16.
 */
public class Request implements Event {
    public static final String TYPE = "Request";
    public Operation op;
    public String[] args;
    public int clientId;
    public int requestNumber;
    private Connection connection;


    public Request(String s, Connection connection) {
        this.connection = connection;
        String[] tmp = s.split(",");
        String[] req = tmp[1].split(" ");
        op =  Operation.valueOf(req[0].toUpperCase());
        args = new String[req.length - 1];
        System.arraycopy(req, 1, args, 0, req.length - 1);
        clientId = Integer.valueOf(tmp[2]);
        requestNumber = Integer.valueOf(tmp[3]);

    }

    public Request(Operation op, String[] args, int clientId, int requestNumber) {
        this.op = op;
        this.args = args;
        this.clientId = clientId;
        this.requestNumber = requestNumber;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return Request.TYPE +","+ op + " " + String.join(" ", args) + "," + clientId + "," + requestNumber;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
