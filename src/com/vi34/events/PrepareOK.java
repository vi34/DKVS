package com.vi34.events;

import com.vi34.Connection;

/**
 * Created by vi34 on 12/06/16.
 */
public class PrepareOK implements Event {
    public static final String TYPE = "PrepareOK";
    public int view;
    public int opNum;
    public int replicaNum;
    private String s;
    private Connection connection;

    public PrepareOK(String s, Connection connection) {
        this.connection = connection;
        this.s = s;
        String[] tmp = s.split(",");
        view = Integer.valueOf(tmp[1]);
        opNum = Integer.valueOf(tmp[2]);
        replicaNum = Integer.valueOf(tmp[3]);
    }

    public PrepareOK(int view, int opNum, int replicaNum) {
        this.view = view;
        this.opNum = opNum;
        this.replicaNum = replicaNum;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public String toString() {
        return PrepareOK.TYPE + "," + view + "," + opNum + "," + replicaNum;
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
