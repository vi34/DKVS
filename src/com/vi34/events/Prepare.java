package com.vi34.events;

import com.vi34.Connection;
import com.vi34.Operation;

/**
 * Created by vi34 on 12/06/16.
 */
public class Prepare implements Event {
    public static final String TYPE = "Prepare";
    public Request req;
    public int view;
    public int opNum;
    public int commitNum;
    private String s;
    private Connection connection;

    public Prepare(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        String[] tmp = s.split(",");
        String[] msg = tmp[2].split(":");
        int clientId = Integer.valueOf(msg[0]);
        int requestNumber = Integer.valueOf(msg[1]);
        String[] reqM = msg[2].split(" ");
        Operation op =  Operation.valueOf(reqM[0].toUpperCase());
        String[] args = new String[reqM.length - 1];
        req = new Request(op, args, clientId, requestNumber);
        System.arraycopy(reqM, 1, req.args, 0, reqM.length - 1);
        view = Integer.valueOf(tmp[1]);
        opNum = Integer.valueOf(tmp[3]);
        commitNum =  Integer.valueOf(tmp[4]);
    }

    public Prepare(Request req, int view, int opNum, int commitNum) {
        this.req = req;
        this.view = view;
        this.opNum = opNum;
        this.commitNum = commitNum;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return Prepare.TYPE + "," + view + ","
                + req.clientId + ":" + req.requestNumber + ":" + req.op + " " + String.join(" ", req.args)
                + "," + opNum + "," + commitNum;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
